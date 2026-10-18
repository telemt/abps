//! Separate-server/process generator and worker-local allocation experiments.
#![forbid(unsafe_code)]
#[allow(dead_code)]
mod perf_support;
#[allow(dead_code)]
mod spike_support;
use perf_support::{Args, Result};
use serde_json::json;
fn main() -> Result<()> {
    let args = Args::parse()?;
    spike_support::validate(&args)?;
    let result = match args.get("case", "micro") {
        "micro" => spike_support::micro(&args)?,
        "recovery" => spike_support::recovery::run()?,
        "returns" => spike_support::diagnostics::returns(&args)?,
        "working-set" => spike_support::diagnostics::working_set(&args)?,
        "churn" => spike_support::diagnostics::churn(&args)?,
        "observability" => spike_support::observability::run(&args)?,
        "public-api" => spike_support::diagnostics::public_api(&args)?,
        "pending-domains" => spike_support::diagnostics::pending_domains(&args)?,
        "soak" => spike_support::soak::run(&args)?,
        "concurrent-views" => spike_support::diagnostics::concurrent_views(&args)?,
        "observe" => spike_support::observe(&args)?,
        "tcp" => match args.get("role", "server") {
            "server" => spike_support::network::server(&args)?,
            "plan" => spike_support::network::resource_plan(&args)?,
            "loadgen" => spike_support::network::loadgen(&args)?,
            _ => return Err("role must be plan, server or loadgen".into()),
        },
        _ => return Err("case must be micro, observe, or tcp".into()),
    };
    println!("{}", json!({"args":args.0,"result":result}));
    if result.get("failed").and_then(serde_json::Value::as_bool) == Some(true) {
        return Err("measurement reported failure; JSON retained above".into());
    }
    Ok(())
}
