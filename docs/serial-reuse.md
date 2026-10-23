# Serial reuse of one retained allocation

`serial-adapter` exposes `abps_reference::serial` over Mosaic. This opt-in safe
adapter implements [AP-15](../CONTRIBUTING.md#ap-15-serial-reuse-of-retained-mutable-ownership).
It requires no runtime dependency or new allocation. A single serial consumer
can reuse a mutable lease between frames instead of returning and reacquiring a
slot for each frame. Independent asynchronous fan-out continues to use ordinary
frozen buffers and their owning references.

```rust
use abps_reference::serial::SerialBuffer;
// `domain` is a live Mosaic domain; error handling omitted here.
let mut serial = SerialBuffer::from_buffer(domain.try_acquire(1500)?);
for payload in payloads {
    let mut write = serial.begin();
    write.put_slice(payload)?;
    let frame = write.finish();
    consume(frame.as_ref());
} // end this frame borrow before beginning the next write
drop(serial); // the ordinary Mosaic return now refunds the full allocation
```

The state transitions are `owned BufMut → SerialBuffer → borrowed SerialWrite →
borrowed SerialFrame → SerialBuffer`. `from_buffer` and `into_buffer` transfer
the lease unchanged, including lengths. `begin` clears filled length. Write
cancellation/unwind and frame destruction also clear filled length, without
returning the backing. Forgetting a guard does not compromise a later `begin`.
Slices cannot outlive their frame; the three types are Send, !Sync and !Clone.
There is no underlying mutable-lease escape through a borrowed guard.

Initialization remains lease-scoped. `initialize_to(n)` zeroes only a newly
initialized tail and can expose earlier frame bytes in an existing prefix. The
caller fills the bytes and explicitly sets filled length before publishing a
frame. `put_slice` copies and initializes exactly its input. Empty frames expose
no bytes. A true new Mosaic acquisition still starts with both lengths zero.

The full class capacity remains admitted while the adapter is idle. Neither an
empty frame nor frame completion signals a free slot, admission refund, or
reclaimable arena. Domain close stops new acquisition but cannot revoke this
existing right. The embedding loop must stop on shutdown and release the adapter;
forgotten ownership cannot be forcibly reclaimed. One retained serial buffer per
connection can therefore waste capacity when many connections are idle.

Use `serial_pipeline` for an initialized `Read`/`Write` example. Prepared begin,
finish and guard destruction do constant metadata work with no additional atomics;
copying/initializing is proportional to bytes touched. This is a work bound, not
a wall-clock guarantee. `serial_probe` compares fresh leases, direct parked leases
and the actual adapter; `serial_alloc` measures allocator calls separately with a
positive control. Both report retained charge and confirmed teardown.

The [implementation report](reports/2026-10-23/memory-shortcuts.md) records measured
effects and limits. Serial reuse is a different retention contract from fresh
admission and its primitive results are not TCP delivery improvements.
