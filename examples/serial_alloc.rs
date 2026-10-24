//! AP-15 allocation observer. Its results are not throughput evidence.
#![forbid(unsafe_code)]
#[allow(dead_code)]
mod perf_support;
mod serial_support;
#[global_allocator]
static GLOBAL: &stats_alloc::StatsAlloc<std::alloc::System> = &stats_alloc::INSTRUMENTED_SYSTEM;
fn main() -> perf_support::Result<()> {
    let args = perf_support::Args::parse()?;
    println!("{}", serial_support::run(&args, Some(GLOBAL))?);
    Ok(())
}
