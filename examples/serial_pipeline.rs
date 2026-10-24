//! Serial initialized Read/Write; no independent frame ownership or extra arena.
#![forbid(unsafe_code)]
use abps_reference::{mosaic as m, serial::SerialBuffer};
use std::io::{Read, Write};

fn main() -> Result<(), Box<dyn std::error::Error>> {
    let config = m::Config::new(
        vec![m::ClassConfig {
            capacity: 4096,
            slots: 1,
        }],
        1,
        usize::MAX,
    );
    let mut storage = m::StorageOwner::prepare(&config)?;
    let pool = storage.activate()?;
    let (owner, domain) = pool.try_open_domain(4096)?;
    let mut serial = SerialBuffer::from_buffer(domain.try_acquire(4096)?);
    let mut input = std::io::stdin().lock();
    let mut output = std::io::stdout().lock();
    loop {
        let mut write = serial.begin();
        let n = input.read(write.initialize_to(4096)?)?;
        if n == 0 {
            break;
        }
        write.set_filled_len(n)?;
        output.write_all(write.finish().as_ref())?;
    }
    output.flush()?;
    drop((serial, domain, owner, pool));
    storage.try_destroy()?;
    Ok(())
}
