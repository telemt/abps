#!/usr/bin/env python3
"""AP-17 targeted qualification using the existing evidence registry/runner."""
import argparse
from pathlib import Path
from types import SimpleNamespace
import verify as v

GROUPS=[
    (['--lib'],'old_return_after_reuse_refunds_only_its_origin_and_pins_shutdown'),
    (['--lib'],'si11_reissue_before_refund_cannot_credit_new_origin_or_destroy_root'),
    (['--lib'],'si11_teardown_after_refund_without_join_synchronization'),
    (['--lib'],'abps_batch_boundaries_conserve_handles_pending_and_source'),
    (['--lib'],'br01_source_reserved_staged_and_installed_boundaries_conserve_rights'),
    (['--lib'],'br03_final_family_release_races_teardown_across_many_schedules'),
    (['--lib'],'branch_record_incarnations_one_two_one_preserve_lifetime_and_initialization'),
    v.QUEUE_RECEIPTS,
]

def main():
    p=argparse.ArgumentParser(description=__doc__)
    p.add_argument('--tool',required=True,choices=['native','miri','loom','asan','tsan'])
    p.add_argument('--output',required=True,type=Path);p.add_argument('--build-root',required=True,type=Path)
    p.add_argument('--seeds',default='1,17,42');p.add_argument('--timeout',default=1800,type=int)
    a=p.parse_args()
    args=SimpleNamespace(suite=a.tool,profile='pr',output=a.output,build_root=a.build_root,
        timeout=a.timeout,seed=424242,seeds=a.seeds,alias='both',cases=None,operations=None,seconds=None,dedicated_host=False,mutation_scope='all')
    r=v.Runner(args)
    if a.tool in ['native','loom']:
        r.execute();return
    tasks=[('stable',lambda:r.compiler('stable')),('nightly',lambda:r.compiler('nightly'))]
    if a.tool=='miri':
        for i,g in enumerate(GROUPS+[v.SERIAL_ADAPTER]):tasks.append((f'miri-{i}',lambda g=g:r.miri(g)))
    else:
        tasks.append(('instrumentation',lambda:r.sanitizer_setup(a.tool)))
        for i,g in enumerate(GROUPS):tasks.append((f'{a.tool}-{i}',lambda g=g:r.sanitizer_group(a.tool,g)))
    r.meta['scope']='AP-17 targeted transition audit; not the complete verification matrix'
    r.meta['required_steps']={name:'not_run' for name,_ in tasks};r.save()
    for name,action in tasks:
        try:action()
        except BaseException:
            r.meta['required_steps'][name]='incomplete';r.save();raise
        r.meta['required_steps'][name]='passed';r.save()
    r.complete()
if __name__=='__main__':main()
