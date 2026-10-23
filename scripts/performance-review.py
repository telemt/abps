#!/usr/bin/env python3
"""AP-09 staged performance review. Prebuild; run suites separately from tracing.

Physical cores, matched explicit resource plans, and seven interleaved trials.
This runner records exploratory evidence, not a production performance gate.
"""
import argparse
import importlib.util
from pathlib import Path
from performance_support import cpu_selection

spec=importlib.util.spec_from_file_location('berry',Path(__file__).with_name('berry-performance.py'))
berry=importlib.util.module_from_spec(spec);spec.loader.exec_module(berry)


def tcp_profile(backend, queue, mode='scalar'):
    # Explicit fixture values; all arithmetic and validation remain in Rust.
    if backend=='berry':
        if queue!=8 or mode not in ('scalar','batch'):raise ValueError('unsupported Berry profile')
        fanout,slots,budget=1,34,25500
    else:
        fanout=4
        slots,budget={8:(130,97500),32:(322,241500),64:(578,433500)}[queue]
    return dict(backend=backend,threads=1,streams=2,fanout=fanout,queue=queue,
        **{'return':'remote','write-batch':8,'class-slots':f'{slots},{slots}',
           'domain-limit-bytes':budget,'capacity-policy':'reserved','capacity-check':'require-envelope',
           'fanout-api':mode,'max-frame-age-ms':100},overflow='skip')



def fixed_arena_profile(queue=32, *, local=False, queue_backend='tokio', freshness='off', min_age=0):
    """AP-10: queue descriptors never scale physical capacity or byte admission."""
    if queue not in (8,32,64): raise ValueError('fixed-arena queue must be 8,32,64')
    if queue_backend not in ('tokio','local') or freshness not in ('off','oldest','adaptive'):
        raise ValueError('unsupported queue/freshness policy')
    if queue_backend=='local' and not local or freshness in ('oldest','adaptive') and queue_backend!='local':
        raise ValueError('local queue and oldest require local routing')
    if not 0<=min_age<=3600000: raise ValueError('invalid freshness age')
    p=tcp_profile('mosaic',8)
    p.update(queue=queue,threads=2 if local else 1)
    p.update({'return':'local' if local else 'remote','capacity-check':'report',
              'queue-backend':queue_backend,'freshness-priority':freshness})
    if freshness=='oldest':p['freshness-min-age-ms']=min_age
    return p


def main():
    p=argparse.ArgumentParser(description=__doc__)
    p.add_argument('--suite',choices=['diagnostics','fanout','tcp','fixed-arena','adaptive'],required=True)
    p.add_argument('--output',type=Path,required=True)
    p.add_argument('--target',type=Path,default=Path('target'))
    p.add_argument('--build-record',type=Path)
    p.add_argument('--repeats',type=int,default=7)
    p.add_argument('--seconds',type=float,default=2)
    p.add_argument('--iterations',type=int,default=200000)
    p.add_argument('--smt',action='store_true')
    p.add_argument('--micro-cpus',type=berry.cpus)
    p.add_argument('--server-cpus',type=berry.cpus)
    p.add_argument('--generator-cpus',type=berry.cpus)
    p.add_argument('--reader-cpus',type=berry.cpus)
    args=p.parse_args()
    if args.repeats<1 or args.seconds<=0 or args.iterations<1:p.error('positive repeats/duration/iterations required')
    cores=cpu_selection(args.smt)
    args.micro_cpus=args.micro_cpus or cores
    args.server_cpus=args.server_cpus or cores[:min(2,max(0,len(cores)-2))]
    args.reader_cpus=args.reader_cpus or cores[2:-1]
    args.generator_cpus=args.generator_cpus or cores[-1:]
    args.server_prefix='';args.target=args.target.resolve()
    runner=berry.Runner(args)
    for trial in range(args.repeats):
        jobs=[]
        if args.suite=='diagnostics':
            for words in [1,16,256]:
                for kind in ['pages','grants']:
                    for interval in [0,1,32]:
                        jobs.append((f'diagnostics-w{words}-{kind}-every{interval}',dict(
                            case='observability',backend='mosaic',op='mixed',threads=2,classes=8,records=256,
                            iterations=args.iterations,**{'class-slots':64*words,'diagnostic-kind':kind,'diagnostic-interval':interval})))
        elif args.suite=='fanout':
            for backend in ['berry','mosaic']:
                for workers in [1,2,4]:
                    for backing in ['same','distinct']:
                        jobs.append((f'views-{backend}-{workers}-{backing}',dict(case='concurrent-views',backend=backend,
                            threads=workers,backing=backing,seconds=args.seconds,warmup=.15)))
                for size in [64,1500,65536]:
                    for fanout in [1,8,20]: # public micro fanout limit; local family 64 is in branches runner
                        for api in ['fanout','fanout_batch','borrowed']:
                            jobs.append((f'fanout-{backend}-{api}-{size}-{fanout}',dict(backend=backend,op=api,size=size,
                                fanout=fanout,seconds=args.seconds,warmup=.15)))
        elif args.suite=='adaptive':
            profiles=[(name,fixed_arena_profile(local=True,queue_backend='local',freshness=name)) for name in ['off','oldest','adaptive']]
            profiles.append(('keyframe',dict(fixed_arena_profile(local=True,queue_backend='local',freshness='adaptive'),**{'frame-dependencies':'keyframe'})))
            for name,profile in profiles:
                for pattern in ['healthy','pause','constant','ramp','stop-resume']:
                    options=dict(profile,rate=20000,seconds=args.seconds)
                    if pattern!='healthy':options.update(slow='yes',**{'slow-profile':pattern,'send-buffer-bytes':4096})
                    jobs.append((f'adaptive-{name}-{pattern}',options))
                jobs.append((f'adaptive-{name}-mixed',dict(profile,rate=200,seconds=args.seconds,
                    distribution='mixed-frames',**{'domain-limit-bytes':136314880,'keyframe-burst':4})))
        elif args.suite=='fixed-arena':
            profiles=[(f'remote-q{q}',fixed_arena_profile(q)) for q in [8,32,64]]
            profiles += [('local-tokio',fixed_arena_profile(local=True)),
                         ('local-fifo',fixed_arena_profile(local=True,queue_backend='local'))]
            profiles += [(f'local-oldest-{age}ms',fixed_arena_profile(local=True,queue_backend='local',freshness='oldest',min_age=age)) for age in [0,2]]
            for name,profile in profiles:
                for rate,slow in [(1000,False),(20000,False),(20000,True)]:
                    options=dict(profile,rate=rate,seconds=args.seconds)
                    if slow:options.update(slow='yes',**{'send-buffer-bytes':4096})
                    jobs.append((f'fixed-{name}-r{rate}-slow{int(slow)}',options))
        else:
            for queue in [8,32,64]:
                for mode in ['scalar','batch','worker-branches']:
                    for rate in [1000,20000]:
                        jobs.append((f'tcp-mosaic-q{queue}-{mode}-r{rate}',dict(tcp_profile('mosaic',queue,mode),rate=rate,seconds=args.seconds)))
            for backend in ['berry','mosaic','bytes']:
                for mode in ['scalar','batch']:
                    # Same small Berry-compatible envelope for all three baselines.
                    profile=tcp_profile('berry',8,mode);profile['backend']=backend
                    jobs.append((f'tcp-common-{backend}-{mode}',dict(profile,rate=1000,seconds=args.seconds)))
        if trial%2:jobs.reverse()
        for key,options in jobs:
            (runner.tcp if args.suite in ('tcp','fixed-arena','adaptive') else runner.run)(key,options,trial)

if __name__=='__main__':main()
