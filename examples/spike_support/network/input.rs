//! AP-12: bounded wire prefetch; no admission or queue rights are acquired here.
#![forbid(unsafe_code)]
use super::*;
use std::{
    pin::Pin,
    task::{Context, Poll},
};
use tokio::io::{AsyncRead, ReadBuf};

pub(super) struct Input<S> {
    socket: S,
    scratch: Box<[u8]>,
    start: usize,
    end: usize,
    staged: bool,
}
impl<S: AsyncRead + Unpin> Input<S> {
    pub fn new(socket: S, scratch: Box<[u8]>, staged: bool) -> Self {
        assert!(!scratch.is_empty());
        Self {
            socket,
            scratch,
            start: 0,
            end: 0,
            staged,
        }
    }
    fn refill(&mut self, cx: &mut Context<'_>) -> Poll<std::io::Result<()>> {
        if self.start == self.end {
            let mut buf = ReadBuf::new(&mut self.scratch);
            std::task::ready!(Pin::new(&mut self.socket).poll_read(cx, &mut buf))?;
            self.end = buf.filled().len();
            self.start = 0;
        }
        Poll::Ready(Ok(()))
    }
    async fn available(&mut self) -> std::io::Result<()> {
        std::future::poll_fn(|cx| self.refill(cx)).await?;
        if self.start == self.end {
            return Err(std::io::ErrorKind::UnexpectedEof.into());
        }
        Ok(())
    }
    pub async fn discard(&mut self, mut len: usize) -> std::io::Result<()> {
        if !self.staged {
            return ingress::discard(&mut self.socket, len, &mut self.scratch).await;
        }
        while len != 0 {
            self.available().await?;
            let n = len.min(self.end - self.start);
            self.start += n;
            len -= n;
        }
        Ok(())
    }
    pub async fn fill(
        &mut self,
        lease: &mut super::super::backend::Lease,
        mut len: usize,
        read_buf: bool,
    ) -> std::io::Result<()> {
        if !self.staged {
            return ingress::fill(&mut self.socket, lease, len, read_buf).await;
        }
        // The caller already owns queue permits and a newly admitted lease.
        // Only successfully read bytes become initialized/filled lease bytes.
        while len != 0 {
            self.available().await?;
            let n = len.min(self.end - self.start);
            lease.put(&self.scratch[self.start..self.start + n]);
            self.start += n;
            len -= n;
        }
        Ok(())
    }
}
impl<S: AsyncRead + Unpin> AsyncRead for Input<S> {
    fn poll_read(
        self: Pin<&mut Self>,
        cx: &mut Context<'_>,
        dst: &mut ReadBuf<'_>,
    ) -> Poll<std::io::Result<()>> {
        let this = self.get_mut();
        if !this.staged {
            return Pin::new(&mut this.socket).poll_read(cx, dst);
        }
        if dst.remaining() == 0 {
            return Poll::Ready(Ok(()));
        }
        std::task::ready!(this.refill(cx))?;
        let n = dst.remaining().min(this.end - this.start);
        dst.put_slice(&this.scratch[this.start..this.start + n]);
        this.start += n;
        Poll::Ready(Ok(()))
    }
}

#[cfg(test)]
mod tests;

#[cfg(feature = "mosaic-spike")]
pub(super) fn allocation_probe(
    allocator: &stats_alloc::StatsAlloc<std::alloc::System>,
    iterations: usize,
) -> Result<Value> {
    use std::future::Future;
    let fixture = Fixture::new(
        "mosaic",
        vec![ClassConfig {
            capacity: 512,
            slots: 1,
        }],
        1,
        None,
    )?;
    let origin = fixture.open(512)?;
    let wire = [0xa5; 512];
    let mut input = Input::new(&wire[..], vec![0; 1500].into_boxed_slice(), true);
    let mut cx = Context::from_waker(std::task::Waker::noop());
    let region = stats_alloc::Region::new(allocator);
    for _ in 0..iterations {
        // A new finite source each cycle; the preceding payload was fully consumed.
        assert_eq!(input.start, input.end);
        input.socket = &wire[..];
        let mut lease = origin.acquire(512)?;
        {
            let future = input.fill(&mut lease, 512, false);
            let mut future = std::pin::pin!(future);
            assert!(matches!(future.as_mut().poll(&mut cx), Poll::Ready(Ok(()))));
        }
        let frame = lease.freeze();
        assert_eq!(std::hint::black_box(frame.as_ref()), &wire);
        drop(frame);
    }
    let costs = region.change();
    assert_eq!(
        (costs.allocations, costs.deallocations, costs.reallocations),
        (0, 0, 0)
    );
    drop((input, origin));
    fixture.finish()?;
    Ok(
        json!({"iterations":iterations,"allocations":costs.allocations,"deallocations":costs.deallocations,"reallocations":costs.reallocations,"drained":true,"scope":"ready memory source through actual staged fill and Mosaic reuse; excludes socket/runtime setup"}),
    )
}
