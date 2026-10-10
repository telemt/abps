"""Named negative controls with an unmodified baseline and retained diagnostics."""
import os
from pathlib import Path
import signal
import subprocess


def classify(code, output, selector, timed_out=False):
    if timed_out:
        return "timeout"
    if "could not compile" in output or "No space left on device" in output:
        return "build-failure"
    if (code == 101 and f"test {selector} ... FAILED" in output
        and "test result: FAILED. 0 passed; 1 failed;" in output):
        return "killed"
    if code == 0 and "test result: ok. 1 passed;" in output:
        return "surviving-requires-review"
    return "incomplete"


def execute(command, source, env, log):
    timed_out = False
    with log.open("w") as stream:
        process = subprocess.Popen(command, cwd=source, env=env, stdout=stream,
                                   stderr=subprocess.STDOUT, start_new_session=True)
        try:
            code = process.wait(timeout=300)
        except (subprocess.TimeoutExpired, KeyboardInterrupt):
            timed_out = True
            os.killpg(process.pid, signal.SIGKILL)
            code = process.wait()
    return code, log.read_text(errors="replace"), timed_out


def run_case(name, source, file, edits, command, selector, env):
    directory = source.parent
    code, output, timeout = execute(command, source, env, directory / "baseline.log")
    baseline = code == 0 and f"test {selector} ... ok" in output and "test result: ok. 1 passed;" in output
    result = dict(name=name, command=command, selector=selector, baseline_passed=baseline,
                  reference_acquire="acquire-load" if "abps_tsan" in env.get("RUSTFLAGS", "") else "fence")
    if not baseline:
        return result | dict(status="baseline-incomplete", caught=False, exit_code=code)
    path = source / file
    text = path.read_text()
    for old, new in edits:
        if text.count(old) != 1:
            (directory / "anchor.log").write_text("absent or ambiguous mutation anchor: " + old)
            return result | dict(status="anchor-incomplete", caught=False, exit_code=None)
        text = text.replace(old, new)
    path.write_text(text)
    code, output, timeout = execute(command, source, env, directory / "run.log")
    status = classify(code, output, selector, timeout)
    return result | dict(status=status, caught=status == "killed", exit_code=code)
