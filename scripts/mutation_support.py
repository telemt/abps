"""Named negative controls with an unmodified baseline and retained diagnostics."""
import hashlib
from pathlib import Path
import subprocess
from verify import kill_process_tree


def classify(code, output, selector, timed_out=False):
    if timed_out:
        return "timeout"
    if "could not compile" in output or "No space left on device" in output:
        return "build-failure"
    if (code == 101 and f"test {selector} ... FAILED" in output
        and "test result: FAILED. 0 passed; 1 failed;" in output):
        return "killed"
    # Miri aborts the whole test binary on Undefined Behavior inside the one
    # selected test, before libtest can report it as FAILED.
    if (code not in (0, None) and f"test {selector} ... " in output
        and "error: Undefined Behavior" in output):
        return "killed"
    if code == 0 and "test result: ok. 1 passed;" in output:
        return "surviving-requires-review"
    return "incomplete"


def execute(command, source, env, log, timeout=300):
    timed_out = False
    with log.open("w") as stream:
        process = subprocess.Popen(command, cwd=source, env=env, stdout=stream,
                                   stderr=subprocess.STDOUT, start_new_session=True)
        try:
            code = process.wait(timeout=timeout)
        except (subprocess.TimeoutExpired, KeyboardInterrupt):
            timed_out = True
            kill_process_tree(process)
            code = process.wait()
    return code, log.read_text(errors="replace"), timed_out


def create_case_target(output, name):
    """Create one control's own Cargo target, <output>/target/<12 hex digits>.

    MSVC link.exe cannot create an executable beyond MAX_PATH even with Windows
    long paths enabled. This package's build script lies 84 characters below the
    target, so a target over 175 characters leaves every baseline incomplete; the
    hashed name keeps the target only 20 characters longer than --output.
    """
    target = output.resolve() / "target" / hashlib.sha256(name.encode()).hexdigest()[:12]
    target.mkdir(parents=True)  # exclusive: no control shares another's fingerprints
    return target


def run_case(name, source, file, edits, command, selector, env, variants=None):
    """Baseline, then the mutation, under each environment variant in order.

    Variants (for example one Miri seed each) let a probabilistic detector be
    checked without depending on one schedule: every baseline must pass, and
    the first mutated outcome other than a survival decides the case. A kill
    under one variant suffices; a timeout or build failure is never converted.
    """
    directory = source.parent
    variants = variants or [{}]
    log = lambda stem, i: directory / (f"{stem}.log" if len(variants) == 1 else f"{stem}-{i}.log")
    result = dict(name=name, command=command, selector=selector, baseline_passed=False,
                  reference_acquire="acquire-load" if "abps_tsan" in env.get("RUSTFLAGS", "") else "fence")
    if len(variants) > 1:
        result["variants"] = variants
    for i, extra in enumerate(variants):
        code, output, timeout = execute(command, source, env | extra, log("baseline", i))
        if not (code == 0 and f"test {selector} ... ok" in output and "test result: ok. 1 passed;" in output):
            return result | dict(status="baseline-incomplete", caught=False, exit_code=code)
    result["baseline_passed"] = True
    path = source / file
    text = path.read_text()
    for old, new in edits:
        if text.count(old) != 1:
            (directory / "anchor.log").write_text("absent or ambiguous mutation anchor: " + old)
            return result | dict(status="anchor-incomplete", caught=False, exit_code=None)
        text = text.replace(old, new)
    path.write_text(text)
    for i, extra in enumerate(variants):
        code, output, timeout = execute(command, source, env | extra, log("run", i))
        status = classify(code, output, selector, timeout)
        if status != "surviving-requires-review":
            break
    if len(variants) > 1:
        result["decided_by"] = i
    return result | dict(status=status, caught=status == "killed", exit_code=code)
