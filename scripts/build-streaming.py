#!/usr/bin/env python3
"""Freeze build provenance before AP-18/19 runs; reuse an explicitly supplied tmpfs cache."""
import argparse, hashlib, json, os, subprocess
from pathlib import Path
import verify
from performance_support import require

def main():
    p=argparse.ArgumentParser(description=__doc__)
    p.add_argument('--target',type=Path,required=True);p.add_argument('--record',type=Path,required=True)
    a=p.parse_args();require(not a.record.exists(),'build','preserve existing record')
    a.target.mkdir(parents=True,exist_ok=True);a.record.parent.mkdir(parents=True,exist_ok=True)
    for path in [a.target,a.record.parent]:
        require(subprocess.check_output(['findmnt','-n','-o','FSTYPE','--target',str(path)],text=True).strip()=='tmpfs','build','tmpfs required')
    require(len(Path('/proc/swaps').read_text().splitlines())==1,'build','swap must be disabled')
    features='local-branches-spike,bytes-interop,serial-adapter'
    names=['spike_probe','spike_alloc','serial_probe','serial_alloc','serial_tcp','streaming_probe']
    cmd=['cargo','build','--locked','--release','--features',features,'--config','profile.release.debug=1','--config','profile.release.strip="none"']
    for name in names:cmd+=['--example',name]
    r=dict(schema=1,status='incomplete',source_root=str(verify.ROOT),source_sha256=verify.source_identity(),profile='release',features=features,
        rustc=subprocess.check_output(['rustc','-Vv'],text=True),command=cmd,target=str(a.target.resolve()),binaries={})
    try:
        with a.record.with_suffix('.log').open('w') as out:
            result=subprocess.run(cmd,cwd=verify.ROOT,env=dict(os.environ,CARGO_TARGET_DIR=str(a.target.resolve())),stdout=out,stderr=subprocess.STDOUT)
        result.check_returncode();require(verify.source_identity()==r['source_sha256'],'build','source changed during compilation')
        for name in names:
            path=a.target.resolve()/'release/examples'/name
            r['binaries'][name]=dict(path=str(path),sha256=hashlib.sha256(path.read_bytes()).hexdigest())
        r['status']='passed'
    finally:a.record.write_text(json.dumps(r,indent=2)+'\n')
if __name__=='__main__':main()
