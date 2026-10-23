//! AP-15: reuse one retained mutable allocation for serial consumers.
//!
//! Frame completion clears filled length; it does **not** return a slot or charge.
//! Initialized bytes persist and may contain earlier frame data. The application
//! must stop processing explicitly on shutdown and release this backing lease.
//! Independent asynchronous fan-out should use Mosaic's ordinary frozen buffers.
#![forbid(unsafe_code)]

use crate::{
    Error,
    mosaic::{BufMut, DomainIdentity},
};

/// One owned allocation, including while idle. `Send`, not `Sync` or `Clone`.
///
/// ```
/// use abps_reference::{mosaic, serial::SerialBuffer};
/// # fn example() -> Result<(), abps_reference::Error> {
/// let config = mosaic::Config::new(
///     vec![mosaic::ClassConfig { capacity: 64, slots: 1 }], 1, usize::MAX);
/// let mut storage = mosaic::StorageOwner::prepare(&config)?;
/// let pool = storage.activate()?;
/// let (owner, domain) = pool.try_open_domain(64)?;
/// let mut serial = SerialBuffer::from_buffer(domain.try_acquire(64)?);
/// for bytes in [b"first".as_slice(), b"next".as_slice()] {
///     let mut write = serial.begin();
///     write.put_slice(bytes)?;
///     let frame = write.finish();
///     assert_eq!(frame.as_ref(), bytes);
/// } // frame borrows end before the next write
/// assert_eq!(domain.metrics().admitted_bytes, 64);
/// drop(serial);
/// assert_eq!(domain.metrics().admitted_bytes, 0);
/// drop((domain, owner, pool));
/// storage.try_destroy()?;
/// # Ok(()) }
/// # example().unwrap();
/// ```
pub struct SerialBuffer {
    buffer: BufMut,
}

impl SerialBuffer {
    /// Transfer a lease unchanged. The first `begin` explicitly discards its
    /// filled bytes; `into_buffer` before begin preserves them.
    pub fn from_buffer(buffer: BufMut) -> Self {
        Self { buffer }
    }

    /// Start an empty frame without resetting initialization or admission.
    ///
    /// ```compile_fail
    /// use abps_reference::serial::SerialBuffer;
    /// fn overlap(serial: &mut SerialBuffer) {
    ///     let frame = serial.begin().finish();
    ///     let write = serial.begin();
    ///     println!("{:?}", frame.as_ref());
    /// }
    /// ```
    pub fn begin(&mut self) -> SerialWrite<'_> {
        clear(&mut self.buffer);
        SerialWrite {
            buffer: Some(&mut self.buffer),
        }
    }

    /// Transfer the retained allocation, with its current lengths, to the caller.
    pub fn into_buffer(self) -> BufMut {
        self.buffer
    }

    pub fn capacity(&self) -> usize {
        self.buffer.capacity()
    }
    pub fn initialized_len(&self) -> usize {
        self.buffer.initialized_len()
    }
    pub fn origin(&self) -> DomainIdentity {
        self.buffer.origin()
    }
}

// Zero is always <= initialized_len. This cannot fail for any safe BufMut state.
fn clear(buffer: &mut BufMut) {
    buffer
        .set_filled_len(0)
        .expect("zero filled length is valid");
}

/// Exclusive frame construction. Drop/cancellation discards filled bytes only.
#[must_use = "dropping the writer discards the frame, retaining its allocation"]
pub struct SerialWrite<'a> {
    buffer: Option<&'a mut BufMut>,
}

impl<'a> SerialWrite<'a> {
    fn buffer(&self) -> &BufMut {
        self.buffer.as_deref().expect("live writer")
    }
    fn buffer_mut(&mut self) -> &mut BufMut {
        self.buffer.as_deref_mut().expect("live writer")
    }

    pub fn capacity(&self) -> usize {
        self.buffer().capacity()
    }
    pub fn filled_len(&self) -> usize {
        self.buffer().filled_len()
    }
    /// Initialization is lease-scoped, not frame-scoped.
    pub fn initialized_len(&self) -> usize {
        self.buffer().initialized_len()
    }
    pub fn origin(&self) -> DomainIdentity {
        self.buffer().origin()
    }

    /// Append initialized bytes; on a range error, lengths and payload are unchanged.
    pub fn put_slice(&mut self, bytes: &[u8]) -> Result<(), Error> {
        self.buffer_mut().put_slice(bytes)
    }

    /// Expose a writable initialized prefix. Only a previously uninitialized tail
    /// is zeroed. Existing bytes may belong to an earlier frame of this lease.
    /// The caller must explicitly set filled length after reading/writing into it.
    pub fn initialize_to(&mut self, len: usize) -> Result<&mut [u8], Error> {
        self.buffer_mut().initialize_to(len)
    }

    pub fn set_filled_len(&mut self, len: usize) -> Result<(), Error> {
        self.buffer_mut().set_filled_len(len)
    }

    /// Consume the writer and lend its filled prefix without freezing or cloning.
    pub fn finish(mut self) -> SerialFrame<'a> {
        SerialFrame {
            buffer: self.buffer.take().expect("live writer"),
        }
    }
}

impl Drop for SerialWrite<'_> {
    fn drop(&mut self) {
        if let Some(buffer) = self.buffer.as_deref_mut() {
            clear(buffer);
        }
    }
}

/// Readable frame borrowing the exclusive lease. Slices cannot outlive this frame.
///
/// ```compile_fail
/// use abps_reference::serial::SerialBuffer;
/// fn escape(serial: &mut SerialBuffer) -> &[u8] {
///     let frame = serial.begin().finish();
///     frame.as_ref()
/// }
/// ```
/// ```compile_fail
/// use abps_reference::serial::SerialBuffer;
/// fn overwrite(serial: &mut SerialBuffer) {
///     let frame = serial.begin().finish();
///     let bytes = frame.as_ref();
///     drop(frame);
///     serial.begin().put_slice(b"new").unwrap();
///     println!("{bytes:?}");
/// }
/// ```
#[must_use = "dropping the frame clears filled length, retaining its allocation"]
pub struct SerialFrame<'a> {
    buffer: &'a mut BufMut,
}

impl SerialFrame<'_> {
    pub fn capacity(&self) -> usize {
        self.buffer.capacity()
    }
    pub fn filled_len(&self) -> usize {
        self.buffer.filled_len()
    }
    pub fn initialized_len(&self) -> usize {
        self.buffer.initialized_len()
    }
    pub fn origin(&self) -> DomainIdentity {
        self.buffer.origin()
    }
}

impl AsRef<[u8]> for SerialFrame<'_> {
    fn as_ref(&self) -> &[u8] {
        self.buffer.as_ref()
    }
}

impl Drop for SerialFrame<'_> {
    fn drop(&mut self) {
        clear(self.buffer);
    }
}
