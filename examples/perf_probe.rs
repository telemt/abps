//! Reproducible reference-model audit; see docs/performance.md.
#![forbid(unsafe_code)]
mod perf_support;
use perf_support::{Args, Result, core, tcp};
use serde_json::json;

fn main() -> Result<()> {
    let args = Args::parse()?;
    args.validate()?;
    let result = match args.get("case", "micro") {
        "micro" => core::micro(&args)?,
        "snapshot" => core::snapshot(&args)?,
        "retention" => core::retention(&args)?,
        "handoff" => core::handoff(&args)?,
        "tcp" => {
            let runtime = tokio::runtime::Builder::new_multi_thread()
                .worker_threads(4)
                .enable_all()
                .build()?;
            runtime.block_on(tcp::run(&args, |_| {}))?
        }
        _ => return Err("case must be micro, snapshot, retention, handoff, or tcp".into()),
    };
    println!("{}", json!({"args": args.0, "result": result}));
    if result.get("failed").and_then(serde_json::Value::as_bool) == Some(true) {
        return Err("measurement failed; JSON retained above".into());
    }
    Ok(())
}
