//! Deterministic partial I/O, wakeups and failures for the actual adapters.
use std::{
    cell::RefCell,
    collections::VecDeque,
    io::{self, IoSlice},
    pin::Pin,
    rc::Rc,
    task::{Context, Poll},
};
use tokio::io::{AsyncRead, AsyncWrite, ReadBuf};

#[derive(Clone, Copy)]
pub enum Step {
    Data(usize),
    Pending,
    Stall,
    Error(io::ErrorKind),
    Zero,
    OverReport,
}
pub struct ScriptedIo {
    pub steps: VecDeque<Step>,
    pub input: &'static [u8],
    pub consumed: usize,
    pub output: Rc<RefCell<Vec<u8>>>,
    pub calls: usize,
}
impl ScriptedIo {
    pub fn new(input: &'static [u8], steps: impl IntoIterator<Item = Step>) -> Self {
        Self {
            steps: steps.into_iter().collect(),
            input,
            consumed: 0,
            output: Rc::default(),
            calls: 0,
        }
    }
    fn step(&mut self, cx: &mut Context<'_>) -> Poll<io::Result<Step>> {
        self.calls += 1;
        match self
            .steps
            .pop_front()
            .expect("adapter polled beyond its I/O script")
        {
            Step::Pending => {
                cx.waker().wake_by_ref();
                Poll::Pending
            }
            Step::Stall => {
                self.steps.push_front(Step::Stall);
                Poll::Pending
            }
            Step::Error(error) => Poll::Ready(Err(error.into())),
            step => Poll::Ready(Ok(step)),
        }
    }
}
impl AsyncRead for ScriptedIo {
    fn poll_read(
        mut self: Pin<&mut Self>,
        cx: &mut Context<'_>,
        buf: &mut ReadBuf<'_>,
    ) -> Poll<io::Result<()>> {
        match std::task::ready!(self.step(cx))? {
            Step::Data(n) => {
                let n = n.min(buf.remaining()).min(self.input.len() - self.consumed);
                buf.put_slice(&self.input[self.consumed..self.consumed + n]);
                self.consumed += n;
            }
            Step::Zero => (),
            _ => panic!("invalid scripted read"),
        }
        Poll::Ready(Ok(()))
    }
}
impl AsyncWrite for ScriptedIo {
    fn poll_write(
        self: Pin<&mut Self>,
        cx: &mut Context<'_>,
        buf: &[u8],
    ) -> Poll<io::Result<usize>> {
        self.poll_write_vectored(cx, &[IoSlice::new(buf)])
    }
    fn poll_write_vectored(
        mut self: Pin<&mut Self>,
        cx: &mut Context<'_>,
        bufs: &[IoSlice<'_>],
    ) -> Poll<io::Result<usize>> {
        let mut remaining = match std::task::ready!(self.step(cx))? {
            Step::Data(n) => n,
            Step::Zero => return Poll::Ready(Ok(0)),
            Step::OverReport => {
                return Poll::Ready(Ok(bufs.iter().map(|b| b.len()).sum::<usize>() + 1));
            }
            _ => panic!("invalid scripted write"),
        };
        let mut written = 0;
        for buf in bufs {
            let n = remaining.min(buf.len());
            self.output.borrow_mut().extend_from_slice(&buf[..n]);
            written += n;
            remaining -= n;
        }
        Poll::Ready(Ok(written))
    }
    fn is_write_vectored(&self) -> bool {
        true
    }
    fn poll_flush(self: Pin<&mut Self>, _: &mut Context<'_>) -> Poll<io::Result<()>> {
        Poll::Ready(Ok(()))
    }
    fn poll_shutdown(self: Pin<&mut Self>, _: &mut Context<'_>) -> Poll<io::Result<()>> {
        Poll::Ready(Ok(()))
    }
}
