#!/usr/bin/env python3
"""Compile the Mosaic buffer boundary, then require precise access rejections."""
import argparse
import json
import os
from pathlib import Path
import shutil
import subprocess
from verify import source_identity

ROOT = Path(__file__).resolve().parents[1]
CASES = [
    ("root", "lease.root.root()", "E0599", "`root`"),
    ("domain", "lease.root.domain(0)", "E0599", "`domain`"),
    ("pin", "lease.root.pin(1)", "E0599", "`pin`"),
    ("cursor", "lease.root.cursor(0)", "E0599", "`cursor`"),
    ("destroy", "lease.root.destroy()", "E0599", "`destroy`"),
    ("drained", "lease.root.drained()", "E0599", "`drained`"),
    ("pointer", "lease.root.0", "E0616", "private"),
    ("plan", "lease.root.plan()", "E0624", "private"),
]


def rejected(code, output, expected, marker):
    """An unrelated compilation/setup failure cannot satisfy the boundary."""
    errors = []
    finished = []
    for line in output.splitlines():
        try:
            row = json.loads(line)
        except json.JSONDecodeError:
            continue
        if row.get("reason") == "build-finished":
            finished.append(row.get("success"))
        if row.get("reason") == "compiler-message" and row["message"]["level"] == "error":
            errors.append(row["message"])
    return (code == 101 and finished == [False] and len(errors) == 1
            and (errors[0].get("code") or {}).get("code") == expected
            and marker in errors[0]["message"]
            and any(s.get("is_primary") and s["file_name"] == "src/mosaic/raw.rs"
                    for s in errors[0]["spans"]))


def main():
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--output", type=Path, required=True)
    parser.add_argument("--build-root", type=Path, required=True)
    args = parser.parse_args()
    args.output.mkdir(parents=True, exist_ok=False)
    source = args.output / "source"
    source.mkdir()
    for name in ["src", "examples", "tests", "benches"]:
        shutil.copytree(ROOT / name, source / name)
    for name in ["Cargo.toml", "Cargo.lock", "build.rs"]:
        shutil.copy2(ROOT / name, source / name)
    identity = source_identity()
    env = dict(os.environ, CARGO_TARGET_DIR=str(args.build_root.resolve()),
               RUSTFLAGS="", CARGO_ENCODED_RUSTFLAGS="")
    command = ["cargo", "check", "--locked", "--lib", "--features",
               "local-branches-spike,bytes-interop", "--message-format=json"]
    results = dict(source_sha256=identity, status="incomplete", command=command, checks=[])
    for name, cmd in [("compiler", ["rustc", "-vV"]), ("cargo", ["cargo", "--version"])]:
        results[name] = subprocess.check_output(cmd, text=True)
    file = source / "src/mosaic/raw.rs"
    original = file.read_text()
    def save():
        (args.output / "results.json").write_text(json.dumps(results, indent=2) + "\n")
    for name, expression, expected, marker in [("baseline", None, None, None), *CASES]:
        file.write_text(original + ("" if expression is None else
            f"\n#[allow(dead_code)]\nfn authority_probe(lease: &Lease) {{ let _ = {expression}; }}\n"))
        try:
            run = subprocess.run(command, cwd=source, env=env, text=True,
                                 stdout=subprocess.PIPE, stderr=subprocess.STDOUT, timeout=300)
        except subprocess.TimeoutExpired:
            save()
            raise RuntimeError("authority compiler timed out")
        (args.output / (name + ".log")).write_text(run.stdout)
        passed = (run.returncode == 0 and '"success":true' in run.stdout) if expression is None else rejected(run.returncode, run.stdout, expected, marker)
        results["checks"].append(dict(name=name, expression=expression,
            expected_error=expected, exit_code=run.returncode, passed=passed))
        save()
        if not passed:
            raise RuntimeError("authority check incomplete: " + name)
    file.write_text(original)
    results["final_source_sha256"] = source_identity()
    if identity != results["final_source_sha256"]:
        save()
        raise RuntimeError("source changed during authority checks")
    results["status"] = "passed"
    save()
    print("remote authority: baseline and eight access rejections passed")


if __name__ == "__main__":
    main()
