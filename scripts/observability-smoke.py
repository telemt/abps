#!/usr/bin/env python3
"""Functional measurements on the local host; never a timing regression gate."""
import argparse
import json
import os
from pathlib import Path
import subprocess
import verify


def main():
    p=argparse.ArgumentParser(description=__doc__)
    p.add_argument('--output',type=Path,required=True)
    p.add_argument('--build-root',type=Path,required=True)
    p.add_argument('--iterations',type=int,default=1000)
    args=p.parse_args()
    if args.iterations<1: p.error('iterations must be positive')
    args.output.mkdir(parents=True,exist_ok=False)
    source=verify.source_identity()
    env=dict(os.environ,CARGO_TARGET_DIR=str(args.build_root))
    with (args.output/'build.log').open('w') as log:
        subprocess.run(['cargo','build','--locked','--release','--features',verify.FULL,'--example','spike_probe'],env=env,stdout=log,stderr=subprocess.STDOUT,check=True)
    binary=args.build_root/'release/examples/spike_probe'
    rows=[]
    for backend,slots,records,active in [('berry',64,256,16),('mosaic',64,256,16),('mosaic',129,4096,64)]:
        for op in ['grants','pages','busy-destroy','mixed']:
            for interval in ([0,1,32] if op=='mixed' else [32]):
                cmd=[str(binary),'--case=observability',f'--backend={backend}',f'--op={op}',f'--class-slots={slots}',f'--records={records}',f'--active-domains={active}',f'--iterations={args.iterations}',f'--diagnostic-interval={interval}','--fragmentation=alternating']
                run=subprocess.run(cmd,text=True,capture_output=True,timeout=120)
                entry=dict(command=cmd,exit_code=run.returncode,stdout=run.stdout,stderr=run.stderr)
                rows.append(entry)
                (args.output/'measurements.json').write_text(json.dumps(rows,indent=2)+'\n')
                if run.returncode: raise RuntimeError(entry)
                result=json.loads(run.stdout)['result']
                if not result['drained']: raise RuntimeError('teardown not confirmed')
                if op=='mixed':
                    admission=result['admission']
                    if admission['attempted']!=args.iterations+1: raise RuntimeError('lost admission attempts')
                    if admission['accepted']<=0: raise RuntimeError('vacuous admission measurement')
                    if admission['useful_bytes']!=0: raise RuntimeError('empty buffers are not useful payload')
                    if result['charged_bytes']!=64*admission['accepted']: raise RuntimeError('lost charged bytes')
    if verify.source_identity()!=source: raise RuntimeError('source changed during measurement')
    (args.output/'summary.json').write_text(json.dumps(dict(source_sha256=source,scope='local harness smoke only',cases=len(rows),
        compiler=subprocess.check_output(['rustc','-Vv'],text=True),affinity=sorted(os.sched_getaffinity(0)),
        iterations=args.iterations,limits='No dedicated host, affinity isolation or performance comparison. Small tail samples.'),indent=2)+'\n')

if __name__=='__main__': main()
