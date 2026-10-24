#!/usr/bin/env python3
"""AP-09/14/15: prebuilt, source-qualified memory shortcuts; all output in tmpfs.

Build records use schema 1, status passed, source_root, source_sha256 (verify.py),
profile, features, rustc, and binaries: {name: {path, sha256}}. No compilation or
profiling is performed here. TCP always uses the same baseline load generator.
"""
import argparse
import hashlib
import importlib.util
import json
import math
import os
from pathlib import Path
import random
import subprocess
from types import SimpleNamespace

from performance_support import cpu_selection, require

ROOT = Path(__file__).resolve().parents[1]


def module(name, path):
    spec = importlib.util.spec_from_file_location(name, path)
    loaded = importlib.util.module_from_spec(spec)
    spec.loader.exec_module(loaded)
    return loaded


def digest(path):
    return hashlib.sha256(Path(path).read_bytes()).hexdigest()


class Build:
    def __init__(self, path):
        self.path = Path(path).resolve()
        self.record = json.loads(self.path.read_text())
        r = self.record
        require(r.get('schema') == 1 and r.get('status') == 'passed', 'build', 'unqualified build')
        for key in ['source_root', 'source_sha256', 'profile', 'features', 'rustc', 'binaries']:
            require(bool(r.get(key)), 'build', 'missing '+key)
        self.verify_source()
        for name in r['binaries']:
            self.binary(name)

    def verify_source(self):
        verifier = module('snapshot_verify', Path(self.record['source_root'])/'scripts/verify.py')
        require(verifier.source_identity() == self.record['source_sha256'], 'build', 'source drift')

    def binary(self, name):
        item = self.record['binaries'][name]
        path = Path(item['path'])
        require(path.is_file() and digest(path) == item['sha256'], 'build', 'binary drift: '+name)
        return path


def validate_serial(r, allocation=False):
    require(r.get('drained') is True and r.get('iterations', 0) > 0, 'serial', 'empty or undrained')
    require(r['checksum'] == r['iterations']*330, 'serial', 'payload observation mismatch')
    require(r['retained_idle_bytes'] == r['size']*r['working_set'], 'serial', 'lost retained charge')
    if allocation:
        require(r['positive_control'] is True, 'serial', 'allocation control missing')
        require(all(r[k] == 0 for k in ['allocations', 'deallocations', 'reallocations']), 'serial', 'allocator activity')
        require(r['ns_per_frame'] is None, 'serial', 'instrumented throughput claim')
    else:
        require(math.isfinite(r['ns_per_frame']) and r['ns_per_frame'] > 0, 'serial', 'invalid timing')


def validate_cycle(r):
    require(r.get('drained') is True and r.get('iterations', 0) > 0, 'cycle', 'empty or undrained')
    require(r['checksum'] == r['iterations']*7, 'cycle', 'payload observation mismatch')
    require(math.isfinite(r['ns_per_frame']) and r['ns_per_frame'] > 0, 'cycle', 'invalid timing')
    require(r['bank']['reserved_bytes'] == 0, 'cycle', 'unconfirmed teardown')
    require(all(s['admitted'] == 0 for s in r['bank']['streams']), 'cycle', 'retained charge')


def cpu_ticks(samples):
    """A vanished process may yield a final all-zero observation."""
    require(bool(samples), 'cpu', 'missing samples')
    return max(samples, key=lambda s: s[2]+s[3])[2:4]


def tcp_options(baseline, server, output, cores):
    # Runner uses release/examples for the common generator. The server's actual
    # profile/path is an override, never a replacement for the generator binary.
    gen = baseline.binary('spike_probe')
    require(gen.parent.name == 'examples' and gen.parent.parent.name == 'release', 'tcp', 'baseline release generator required')
    return SimpleNamespace(target=gen.parent.parent.parent,
        output=output, build_record=baseline.path, server_binary=server.binary('spike_probe'),
        server_build_record=server.path, server_cpus=cores[:1], generator_cpus=cores[-1:],
        reader_cpus=cores[1:-1], micro_cpus=cores[:1], server_prefix='', smt=False)


class Experiment:
    def __init__(self, args):
        self.args = args
        self.cores = cpu_selection(False)
        require(len(self.cores) >= 4, 'affinity', 'four physical cores required')
        self.builds = {'baseline': Build(args.baseline_record), 'candidate': Build(args.candidate_record)}
        if args.lto_record:
            self.builds['lto'] = Build(args.lto_record)
        parent = args.output.resolve()
        while not parent.exists():
            parent = parent.parent
        fs = subprocess.check_output(['findmnt', '-n', '-o', 'FSTYPE', '--target', str(parent)], text=True).strip()
        require(fs == 'tmpfs', 'output', 'experiments require tmpfs; no disk fallback')
        require(not args.output.exists(), 'output', 'use a fresh evidence directory')
        args.output.mkdir(parents=True)
        self.raw = args.output/'primitive.jsonl'
        self.count = 0
        (args.output/'environment.json').write_text(json.dumps(dict(
            builds={k:b.record for k,b in self.builds.items()}, driver_sha256=digest(__file__),
            cpus=self.cores, parameters={k:str(v) if isinstance(v,Path) else v for k,v in vars(args).items()},
            loadavg=Path('/proc/loadavg').read_text(), swaps=Path('/proc/swaps').read_text(),
            scope='exploratory VM results; no dedicated-host promotion'),indent=2)+'\n')

    def run(self, label, name, options, trial, allocation=False):
        build = self.builds[label]
        binary = build.binary(name)
        command = ['taskset','-c',str(self.cores[0]),str(binary),*[f'--{k}={v}' for k,v in options.items()]]
        record = dict(variant=label, trial=trial, command=command, binary_sha256=digest(binary),
                      source_sha256=build.record['source_sha256'], profile=build.record['profile'],
                      status='failed', allocation_observer=allocation)
        try:
            p = subprocess.run(command,text=True,capture_output=True,timeout=180)
            record.update(exit_code=p.returncode,stdout=p.stdout,stderr=p.stderr)
            require(p.returncode == 0, 'process', p.stderr)
            document = json.loads(p.stdout)
            data = document.get('result',document)
            (validate_serial(data,allocation) if name.startswith('serial') else validate_cycle(data))
            record.update(status='passed',data=data)
        except Exception as e:
            record['error'] = str(e)
            raise
        finally:
            with self.raw.open('a') as out:
                out.write(json.dumps(record)+'\n')
            self.count += 1

    def serial(self):
        candidates = [v for v in self.builds if v != 'baseline']
        modes = ['fresh-copy','park-copy','serial-copy','fresh-init','park-init','serial-init']
        for trial in range(self.args.repeats):
            cases = [(size,w) for size in [64,1500,65536,1048576] for w in [1,64]]
            random.Random(self.args.seed+trial).shuffle(cases)
            for size,working in cases:
                for v in candidates[::(-1 if trial%2 else 1)]:
                    for mode in modes[::(-1 if trial%2 else 1)]:
                        self.run(v,'serial_probe',dict(mode=mode,size=size,**{'working-set':working,
                            'iterations':max(3000,min(2000000,400000000//size))}),trial)
            print('serial trial',trial,flush=True)
        for v in candidates:
            for mode in ['serial-copy','serial-init']:
                self.run(v,'serial_alloc',dict(mode=mode,size=1500,**{'working-set':64,'iterations':100000}),0,True)
                self.run(v,'serial_probe',dict(mode=mode,size=1500,**{'working-set':64,'iterations':1000000,'sample-stride':64}),0)

    def policy(self):
        for trial in range(self.args.repeats):
            variants = list(self.builds)
            variants = variants[trial%len(variants):]+variants[:trial%len(variants)]
            for classes in [2,8,32,63,64]:
                for streams in [1,8,32]:
                    for accounting in ['full','cached']:
                        for v in variants:
                            self.run(v,'spike_probe',dict(case='memory-cycle',backend='mosaic',
                                classes=classes,streams=streams,iterations=self.args.iterations,
                                **{'memory-policy':'adaptive-shared','memory-accounting':accounting}),trial)
            print('policy trial',trial,flush=True)

    def tcp(self):
        base = self.builds['baseline']
        runner_module = module('baseline_runner',Path(base.record['source_root'])/'scripts/berry-performance.py')
        memory = module('memory_review',ROOT/'scripts/memory-review.py')
        class MemoryChecked(runner_module.Runner):
            def save(self, key, record):
                if record.get('status') == 'passed':
                    try:
                        memory.validate_memory(record)
                    except Exception as error:
                        record['status'] = 'failed'
                        record.setdefault('errors', []).append(str(error))
                        super().save(key, record)
                        raise
                return super().save(key, record)
        runners = {v:MemoryChecked(tcp_options(base,b,self.args.output/v,self.cores)) for v,b in self.builds.items()}
        for trial in range(self.args.repeats):
            variants = list(self.builds)
            variants = variants[trial%len(variants):]+variants[:trial%len(variants)]
            for pattern in ['steady','synchronized']:
                for slow in [False,True]:
                    key=f'{pattern}-'+('slow' if slow else 'healthy')
                    p=memory.profile('adaptive-shared',headroom=0,pattern=pattern)
                    p.update(seconds=self.args.seconds,rate=16000,seed=self.args.seed+trial*7919,
                             **{'class-slots':'32,32','memory-accounting':'cached'})
                    if slow:p.update(slow='yes',**{'slow-profile':'constant'})
                    if trial==0:
                        plans=[]
                        for b in self.builds.values():
                            cmd=[str(b.binary('spike_probe')),'--case=tcp','--role=plan',*[f'--{k}={v}' for k,v in p.items()]]
                            plans.append(json.loads(subprocess.check_output(cmd,text=True))['result']['pipeline_plan'])
                        require(all(x==plans[0] for x in plans),'tcp','resource plans differ')
                        (self.args.output/(key+'-plan.json')).write_text(json.dumps(plans[0],indent=2)+'\n')
                    for v in variants:
                        r=runners[v].tcp(key,p,trial)
                        require(r['status']=='passed','tcp','flow failure')
                        memory.validate_memory(r)
                        self.count+=1

    def finish(self):
        require(self.count>0,'suite','empty selection')
        for b in self.builds.values():b.verify_source()
        (self.args.output/'complete.json').write_text(json.dumps(dict(status='passed',records=self.count))+'\n')


def main():
    p=argparse.ArgumentParser(description=__doc__)
    p.add_argument('--suite',choices=['serial','policy','tcp','all'],required=True)
    p.add_argument('--baseline-record',type=Path,required=True)
    p.add_argument('--candidate-record',type=Path,required=True)
    p.add_argument('--lto-record',type=Path)
    p.add_argument('--output',type=Path,required=True)
    p.add_argument('--repeats',type=int,default=7)
    p.add_argument('--seconds',type=float,default=15)
    p.add_argument('--iterations',type=int,default=500000)
    p.add_argument('--seed',type=int,default=1033)
    a=p.parse_args()
    if a.repeats<1 or not math.isfinite(a.seconds) or not 0<a.seconds<=3600 or not 1<=a.iterations<=200000000:
        p.error('positive bounded repetitions/durations/iterations required')
    e=Experiment(a)
    for suite in ['serial','policy','tcp'] if a.suite=='all' else [a.suite]:getattr(e,suite)()
    e.finish()


if __name__=='__main__':main()
