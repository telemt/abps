//! AP-15 prepared serial cycles; allocation checks use the separate serial_alloc.
#![forbid(unsafe_code)]
#[allow(dead_code)]
mod perf_support;
mod serial_support;
fn main() -> perf_support::Result<()> {
    let args = perf_support::Args::parse()?;
    println!("{}", serial_support::run(&args, None)?);
    Ok(())
}
