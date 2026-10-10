//! AP-01/06: type boundaries are part of the ownership proof.
#![forbid(unsafe_code)]
#![cfg(any(feature = "berry-spike", feature = "mosaic-spike"))]
// If the type implements the forbidden trait, inference has two candidates and
// compilation fails. Otherwise only the blanket implementation is available.
macro_rules! not_impl {
    ($ty:ty, $bound:path) => {{
        trait Ambiguous<A> {
            fn witness() {}
        }
        impl<T: ?Sized> Ambiguous<()> for T {}
        impl<T: ?Sized + $bound> Ambiguous<u8> for T {}
        let _ = <$ty as Ambiguous<_>>::witness;
    }};
}
macro_rules! controls {
    ($module:path) => {{
        use $module as b;
        not_impl!(b::StorageOwner, Send);
        not_impl!(b::StorageOwner, Sync);
        not_impl!(b::Pool, Send);
        not_impl!(b::Pool, Sync);
        not_impl!(b::Domain, Send);
        not_impl!(b::Domain, Sync);
        not_impl!(b::DomainOwner, Send);
        not_impl!(b::DomainOwner, Sync);
        not_impl!(b::BufMut, Sync);
        not_impl!(b::BufMut, Clone);
        fn send<T: Send>() {}
        fn shared<T: Send + Sync>() {}
        send::<b::BufMut>();
        shared::<b::Buf>();
    }};
}
#[cfg(feature = "berry-spike")]
#[test]
fn berry_control_and_buffer_boundaries() {
    controls!(abps_reference::berry);
}
#[cfg(feature = "mosaic-spike")]
#[test]
fn mosaic_control_and_buffer_boundaries() {
    controls!(abps_reference::mosaic);
}
#[cfg(feature = "local-branches-spike")]
#[test]
fn unique_transfer_and_local_family_boundaries() {
    use abps_reference::mosaic::{Branch, LocalBuf};
    fn send<T: Send>() {}
    send::<Branch>();
    not_impl!(Branch, Sync);
    not_impl!(Branch, Clone);
    not_impl!(LocalBuf, Send);
    not_impl!(LocalBuf, Sync);
    not_impl!(LocalBuf, Clone);
}
