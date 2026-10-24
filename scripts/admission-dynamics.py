#!/usr/bin/env python3
"""AP-16 source-qualified research. Prebuild first; output must be tmpfs.

Replay/native traces are instrumented mechanism evidence. TCP uses the same
binary/generator across policies, fixed arena and open-loop offered load.
"""
import argparse
import importlib.util
import json
import os
from pathlib import Path
import subprocess
from types import SimpleNamespace
from performance_support import cpu_selection, require

ROOT = Path(__file__).resolve().parents[1]


def module(name, path):
    spec = importlib.util.spec_from_file_location(name, path)
    result = importlib.util.module_from_spec(spec)
    spec.loader.exec_module(result)
    return result


def validate_trace(r):
    require(r.get('drained') is True, 'trace', 'unconfirmed teardown')
    require(r['offered'] > 0, 'trace', 'empty workload')
    require(r['offered'] == sum(r[k] for k in ['accepted', 'core_rejected', 'expired', 'source_pending_full']), 'trace', 'offered conservation')
    require(r['accepted'] == len(r['admissions']), 'trace', 'missing grant events')
    require(r['core_rejected'] == sum(r['errors'].values()), 'trace', 'lost error cause')
    require(r['final']['admitted'] == 0 and r['final']['available'] == r['final']['total_slots'], 'trace', 'rights retained')
    if r['timing_evidence']:
        ids = [e[0] for e in r['final_drop_intervals']]
        require(len(set(ids)) == len(ids) == r['accepted'], 'returns', 'missing/duplicate final drop')
        require(set(ids) == {e[0] for e in r['admissions']}, 'returns', 'wrong allocation identity')
        require(all(a <= b for _, a, b in r['final_drop_intervals']), 'returns', 'backward drop interval')
    else:
        require(r['errors']['Contended'] == 0, 'replay', 'sequential contention')
    if r['options']['mode'] == 'Bucket':
        # Independent arrival-curve oracle. O(events), maximum excess over a
        # preceding prefix; compares successful admission timestamps, not hints.
        rate, burst = r['options']['rate_bytes_per_second'], r['options']['burst_bytes']
        prefix, minimum = 0, None
        for _, now, charge in r['admissions']:
            before = prefix*1_000_000_000 - rate*now
            minimum = before if minimum is None else min(minimum, before)
            prefix += charge
            require(prefix*1_000_000_000-rate*now-minimum <= burst*1_000_000_000, 'bucket', 'rate bound violated')
    return r


def profile(pattern='steady', slow=False, seconds=15, rate=1000, fanout=100):
    return dict(case='admission-tcp', backend='mosaic', threads=1, streams=1,
        fanout=fanout, queue=8, seconds=seconds, rate=rate, overflow='skip',
        slow='yes' if slow else 'no', seed=42, **{'return':'remote','return-workers':1,
        'class-capacities':'1500','class-slots':'32','domain-limit-bytes':48000,
        'fanout-api':'batch','write-batch':8,'max-frame-age-ms':100,
        'send-buffer-bytes':4096,'arrival-pattern':pattern,'admission-pacing':'off'})


def pacing(mode, rate, burst):
    return {'admission-pacing':mode, **({} if mode=='off' else {'admission-rate-bytes':rate,'admission-burst-bytes':burst})}


class Study:
    def __init__(self, args):
        self.args=args
        helpers=module('shortcuts',ROOT/'scripts/memory-shortcuts.py')
        self.build=helpers.Build(args.build_record)
        self.binary=self.build.binary('spike_probe')
        args.output.mkdir(parents=True,exist_ok=True)
        require(subprocess.check_output(['findmnt','-n','-o','FSTYPE','--target',str(args.output)],text=True).strip()=='tmpfs','output','tmpfs required')
        require(len(Path('/proc/swaps').read_text().splitlines())==1,'output','swap must be disabled')
        self.cores=cpu_selection(False)
        require(len(self.cores)>=4,'affinity','four physical cores required for separated TCP')
        self.records=[]
        self.raw=args.output/'records.jsonl'
        require(not self.raw.exists(),'output','use a fresh output directory; preserve prior evidence')
        (args.output/'build.json').write_text(json.dumps(self.build.record,indent=2)+'\n')
        runner=module('berry_runner',ROOT/'scripts/berry-performance.py')
        opts=SimpleNamespace(target=self.binary.parent.parent.parent,output=args.output/'tcp',
            server_cpus=self.cores[:2],generator_cpus=self.cores[-1:],reader_cpus=self.cores[2:-1],
            micro_cpus=self.cores,server_prefix='',smt=False,build_record=args.build_record)
        self.tcp=runner.Runner(opts)

    def save(self, r):
        self.records.append(r)
        with self.raw.open('a') as f:f.write(json.dumps(r)+'\n')

    def trace(self, case, values, tag):
        options=dict(case=case,backend='mosaic',streams=8,fanout=1,iterations=self.args.iterations,
            rate=self.args.trace_rate,**{'class-capacities':'1500','class-slots':'32','domain-limit-bytes':48000})
        options.update(values)
        workers=options.get('return-workers',1)
        if case=='admission-returns':
            if workers+1>len(self.cores):
                self.save(dict(id=tag,status='unsupported',reason='insufficient distinct physical cores',returners=workers));return None
            options['cpus']=','.join(map(str,self.cores[:workers+1]))
        cmd=[str(self.binary),*[f'--{k}={v}' for k,v in options.items()]]
        rec=dict(id=tag,status='incomplete',command=cmd)
        out=self.args.output/(tag+'.json');err=self.args.output/(tag+'.stderr')
        try:
            with out.open('w') as f,err.open('w') as e:p=subprocess.run(cmd,stdout=f,stderr=e,timeout=180)
            rec['exit_code']=p.returncode;p.check_returncode()
            r=validate_trace(json.loads(out.read_text())['result'])
            rec.update(status='passed',result={k:v for k,v in r.items() if k not in ['admissions','observed_occupancy','final_drop_intervals']},raw_sha256=__import__('hashlib').sha256(out.read_bytes()).hexdigest())
            return r
        except Exception as e:rec.update(status='failed',error=str(e));raise
        finally:self.save(rec)

    def mechanisms(self):
        modes=['off','bucket','tick-cap']
        for native in [False,True]:
            case='admission-returns' if native else 'admission-replay'
            for workers in ([1,4,8] if native else [1]):
                for mode in modes:
                    hashes=[]
                    for pattern in ['synchronized','spread','impulse']:
                        r=self.trace(case,dict(pacing(mode,self.args.pacing_rate,self.args.burst),**{'release-pattern':pattern,'return-workers':workers}),f'{case}-{workers}-{mode}-{pattern}')
                        if r and pattern!='impulse':hashes.append((r['offered_hash'],r['service_multiset_hash']))
                    require(len(set(hashes))<=1,'trace','phase comparison changed offered work or service multiset')

    def network(self, calibration=False):
        rates=[500,1000,2000,4000] if calibration else [self.args.rate]
        for offered in rates:
            for trial in range(1 if calibration else self.args.repeats):
                variants=['bytes','off','bucket']
                variants=variants[trial%3:]+variants[:trial%3]
                for pattern in self.args.patterns:
                    for slow in self.args.clients:
                        plans=[]
                        for variant in variants:
                            p=profile(pattern,slow=='slow',self.args.seconds,offered,self.args.fanout)
                            p['seed']=42+7919*trial
                            if variant=='bytes':p['backend']='bytes'
                            if variant=='bucket':p.update(pacing('bucket',self.args.pacing_rate,self.args.burst))
                            r=self.tcp.tcp(f'{offered}-{pattern}-{slow}-{variant}',p,trial)
                            require(r['status']=='passed','tcp','run not verified')
                            plan=r['pipeline_plan'];plans.append((variant,plan))
                            self.save(dict(id=r['id'],status='passed',flow=r['flow'],variant=variant,pattern=pattern,clients=slow,trial=trial,rate=offered,
                                server_rss_peak_kib=r['server_rss_peak_kib']))
                        off=next(p for v,p in plans if v=='off');on=next(p for v,p in plans if v=='bucket')
                        require(off['roots'][0]['reservation_bytes']==on['roots'][0]['reservation_bytes'],'plan','arena changed')
                        require(on['external_storage_model_bytes']*100<=off['external_storage_model_bytes']*105,'plan','external growth exceeds 5%')

    def finish(self):
        require(self.records,'suite','empty execution')
        self.build.verify_source()
        (self.args.output/'complete.json').write_text(json.dumps(dict(status='completed',executions=len(self.records),unsupported=sum(r['status']=='unsupported' for r in self.records),promotion='not established on shared host'))+'\n')


def main():
    p=argparse.ArgumentParser(description=__doc__)
    p.add_argument('--build-record',type=Path,required=True);p.add_argument('--output',type=Path,required=True)
    p.add_argument('--suite',choices=['mechanisms','tcp','calibrate','all'],required=True)
    p.add_argument('--seconds',type=float,default=60);p.add_argument('--repeats',type=int,default=7)
    p.add_argument('--iterations',type=int,default=10000);p.add_argument('--trace-rate',type=int,default=3100)
    p.add_argument('--rate',type=int,default=1000);p.add_argument('--pacing-rate',type=int,default=1500000)
    p.add_argument('--burst',type=int,default=6000);p.add_argument('--fanout',type=int,default=100)
    p.add_argument('--patterns',nargs='+',choices=['steady','synchronized'],default=['steady','synchronized'])
    p.add_argument('--clients',nargs='+',choices=['healthy','slow'],default=['healthy','slow'])
    a=p.parse_args();require(a.seconds>0 and a.repeats>0 and a.iterations>0,'arguments','nonempty campaign required')
    s=Study(a)
    if a.suite in ['mechanisms','all']:s.mechanisms()
    if a.suite in ['tcp','calibrate','all']:s.network(a.suite=='calibrate')
    s.finish()

if __name__=='__main__':main()
