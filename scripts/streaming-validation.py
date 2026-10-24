#!/usr/bin/env python3
"""AP-18/19: frozen manifests, existing TCP oracle, separate serial/retention evidence."""
import argparse
import importlib.util
import json
import os
from pathlib import Path
import selectors
import shlex
import subprocess
import sys
import time
from types import SimpleNamespace
from performance_support import cpu_selection, require

ROOT=Path(__file__).resolve().parents[1]
def module(name,path):
    spec=importlib.util.spec_from_file_location(name,path)
    m=importlib.util.module_from_spec(spec);spec.loader.exec_module(m);return m

helpers=module('streaming_build',ROOT/'scripts/memory-shortcuts.py')
berry=module('streaming_runner',ROOT/'scripts/berry-performance.py')
admission=module('streaming_profiles',ROOT/'scripts/admission-dynamics.py')


def scenarios(seconds):
    """Fixed resources within a comparison; queue depth never enlarges backing."""
    result=[]
    for topology in ['fanout','domains']:
        for slow in [False,True]:
            for backend,policy in [('bytes','unreserved'),('mosaic','unreserved'),('mosaic','reserved')]:
                p=admission.profile(slow=slow,seconds=seconds,rate=1000 if topology=='fanout' else 4000,
                                    fanout=100 if topology=='fanout' else 1)
                p.update(streams=1 if topology=='fanout' else 8)
                p.update(backend=backend,**{'capacity-policy':policy,'class-slots':'32' if topology=='fanout' else '16',
                    'domain-limit-bytes':48000 if topology=='fanout' else 24000,'capacity-check':'report','memory-policy':'off',
                    'slow-bytes-per-second':1024})
                result.append(dict(id=f'{topology}-{"slow" if slow else "healthy"}-{backend}-{policy}',options=p))
    return result


def surface_cases():
    result=[]
    # One-factor sweeps around a declared anchor, plus complete exhaustion.
    anchor=dict(size=1500,slots=64,occupancy=75,**{'remote-percent':50,'hold-us':0})
    variations=[anchor]
    for key, values in [('size',[64,65536]),('occupancy',[25,95,100]),
                        ('remote-percent',[0,100]),('hold-us',[1000,50000])]:
        variations.extend(dict(anchor,**{key:v}) for v in values)
    for backend,policy in [('abps','unreserved'),('bytes','unreserved'),('berry','unreserved'),('mosaic','unreserved'),('mosaic','reserved')]:
        for i,p in enumerate(variations):
            result.append(dict(id=f'surface-{backend}-{policy}-{i}',options=dict(p,backend=backend,**{'capacity-policy':policy})))
    return result


def validate_surface(r):
    require(r['iterations']==r['accepted']+sum(r['errors'].values()),'surface','attempt conservation')
    require(r['remote_dispatched']==r['remote_returned'],'surface','remote rights lost')
    require(r['held_slots']<=r['slots'],'surface','invalid preheld occupancy')
    if r['held_slots']==r['slots']:
        require(r['accepted']==0,'surface','exhausted backing admitted a frame')
    # finish() is a teardown proof in the fixture; failures cannot produce this result.
    require(isinstance(r['drained'],dict),'surface','missing teardown report')
    require(r['drained']['admitted']==0 and r['drained']['available']==r['drained']['total_slots'],'surface','unconfirmed drain')


class Observer(berry.Runner):
    def __init__(self,opts):
        super().__init__(opts);self.os_samples=[];self.cgroup=None;self.cgroup_samples=[]
    def process_sample(self,pid,elapsed):
        sample=super().process_sample(pid,elapsed)
        if self.cgroup and self.cgroup.path:self.cgroup_samples.append(dict(elapsed_s=elapsed,**self.cgroup.sample()))
        try:
            stat=Path(f'/proc/{pid}/stat').read_text().rsplit(')',1)[1].split()
            self.os_samples.append(dict(pid=pid,elapsed_s=elapsed,rss_kib=sample[1],
                                        minor_faults=int(stat[7]),major_faults=int(stat[9])))
        except (OSError,ValueError,IndexError):pass
        return sample


class Cgroup:
    """Optional delegated child only. Never enables controllers or edits limits."""
    def __init__(self,parent):
        self.path=None;self.state=dict(status='unsupported',reason='no delegated cgroup parent supplied')
        if parent:
            path=parent.resolve()/f'abps-streaming-{os.getpid()}-{time.monotonic_ns()}'
            try:
                path.mkdir()
                if not (path/'memory.current').is_file():raise RuntimeError('memory controller not delegated')
                self.path=path;self.state=dict(status='available',path=str(path),scope='server processes only; no memory limit set')
            except (OSError,RuntimeError) as e:
                self.state=dict(status='unsupported',reason=str(e))
                if path.exists():
                    try:path.rmdir()
                    except OSError:pass
    def sample(self):
        if not self.path:return self.state
        r=dict(self.state)
        try:
            for name in ['memory.current','memory.peak','memory.stat','memory.events']:
                r[name]=(self.path/name).read_text().strip()
        except OSError as e:r.update(status='incomplete',error=str(e))
        return r
    def close(self):
        if self.path:self.path.rmdir()


def command(binary,options):return [str(binary),*[f'--{k}={v}' for k,v in options.items()]]


class Study:
    def __init__(self,a):
        self.a=a;self.build=helpers.Build(a.build_record);self.cores=cpu_selection(False)
        require(len(self.cores)>=4,'host','four distinct physical cores required')
        require(not a.output.exists(),'output','preserve previous campaign; use a new directory')
        parent=a.output.parent.resolve()
        require(subprocess.check_output(['findmnt','-n','-o','FSTYPE','--target',str(parent)],text=True).strip()=='tmpfs','output','tmpfs required')
        require(len(Path('/proc/swaps').read_text().splitlines())==1,'output','swap must be disabled')
        a.output.mkdir();self.records=[]
        self.manifest=dict(schema=1,profile=a.profile,repeats=a.repeats,seconds=a.seconds,seed=42,
            cpu_sets=dict(server=self.cores[:2],loadgen=self.cores[-1:],readers=self.cores[2:-1]),
            tcp=scenarios(a.seconds),surface=surface_cases(),serial_modes=['fresh','serial','bytes-retained'],
            serial_sizes=[64,1500,65536],serial_iterations=a.iterations,
            expected_records=expected_records(a.suite,a.repeats),warmup='not separated for TCP',
            source=self.build.record,scope='shared-host screening' if a.profile=='screen' else 'dedicated-host campaign; qualification requires review')
        (a.output/'manifest.json').write_text(json.dumps(self.manifest,indent=2)+'\n')
        opts=SimpleNamespace(target=self.build.binary('spike_probe').parent.parent.parent,output=a.output/'tcp',
            server_cpus=self.cores[:2],generator_cpus=self.cores[-1:],reader_cpus=self.cores[2:-1],
            micro_cpus=self.cores,server_prefix='',smt=False,build_record=a.build_record)
        self.runner=Observer(opts)
    def save(self,r):
        self.records.append(r)
        with (self.a.output/'records.jsonl').open('a') as f:f.write(json.dumps(r)+'\n')
    def process(self,name,binary,opts,validate):
        path=self.a.output/name
        cmd=command(self.build.binary(binary),opts)
        r=dict(id=name,status='incomplete',command=cmd)
        try:
            with path.with_suffix('.json').open('w') as out,path.with_suffix('.stderr').open('w') as err:
                p=subprocess.run(cmd,stdout=out,stderr=err,timeout=180)
            r['exit_code']=p.returncode;p.check_returncode()
            value=json.loads(path.with_suffix('.json').read_text());validate(value)
            r.update(status='passed',result=value)
        except Exception as e:r.update(status='failed',error=str(e));raise
        finally:self.save(r)
    def surface(self):
        for trial in range(self.a.repeats):
            cases=self.manifest['surface'];offset=trial%len(cases)
            for c in cases[offset:]+cases[:offset]:
                self.process(c['id']+f'-{trial}','streaming_probe',dict(c['options'],iterations=self.a.iterations,cpus=','.join(map(str,self.cores[:2]))),validate_surface)
    def tcp(self,soak=False):
        for trial in range(1 if soak else self.a.repeats):
            cases=self.manifest['tcp'];offset=trial%3
            for j in range(0,len(cases),3):
                group=cases[j:j+3]
                for c in group[offset:]+group[:offset]:
                    if soak and 'slow' not in c['id']:continue
                    options=dict(c['options'],seed=42+7919*trial)
                    if soak:options['seconds']=1800
                    cg=Cgroup(self.a.cgroup_parent);self.runner.os_samples=[];self.runner.cgroup=cg;self.runner.cgroup_samples=[]
                    if cg.path:self.runner.options.server_prefix=shlex.join([sys.executable,str(Path(__file__).resolve()),'--join-cgroup',str(cg.path)])
                    record=dict(id=c['id']+('-soak' if soak else '')+f'-{trial}',status='incomplete')
                    try:
                        r=self.runner.tcp(c['id']+('-soak' if soak else ''),options,trial)
                        require(r['status']=='passed','tcp','unqualified result')
                        require(r['flow']['unclassified_uncompleted_enqueued']==0,'tcp','unclassified queue ownership')
                        record.update(id=r['id'],status='passed',flow=r['flow'],os_memory=cg.sample(),cgroup_samples=self.runner.cgroup_samples,proc_samples=self.runner.os_samples)
                    except Exception as e:
                        record.update(status='failed',error=str(e));raise
                    finally:
                        self.save(record);self.runner.options.server_prefix='';cg.close()
    def serial(self):
        binary=self.build.binary('serial_tcp')
        for trial in range(self.a.repeats):
            modes=self.manifest['serial_modes'];modes=modes[trial%3:]+modes[:trial%3]
            for size in self.manifest['serial_sizes']:
                for mode in modes:
                    name=f'serial-{size}-{mode}-{trial}';path=self.a.output/name
                    opts=dict(size=size,iterations=self.a.iterations,mode=mode)
                    cmd=['taskset','-c',str(self.cores[0]),*command(binary,dict(opts,role='server'))]
                    r=dict(id=name,status='incomplete',commands=[cmd]);server=None
                    try:
                        with path.with_suffix('.stderr').open('w') as err:
                            server=subprocess.Popen(cmd,stdout=subprocess.PIPE,stderr=err,text=True,start_new_session=True)
                            with selectors.DefaultSelector() as sel:
                                sel.register(server.stdout,selectors.EVENT_READ)
                                require(sel.select(15),'serial','readiness timeout')
                            first=server.stdout.readline();path.with_suffix('.server.jsonl').write_text(first)
                            address=json.loads(first)['ready']
                            client=['taskset','-c',str(self.cores[-1]),*command(binary,dict(opts,role='client',address=address))];r['commands'].append(client)
                            with path.with_suffix('.client.json').open('w') as out:
                                p=subprocess.run(client,stdout=out,stderr=err,timeout=180)
                            p.check_returncode();rest,_=server.communicate(timeout=15)
                            with path.with_suffix('.server.jsonl').open('a') as f:f.write(rest)
                            require(server.returncode==0,'serial','server failure')
                        sr=json.loads(rest);cr=json.loads(path.with_suffix('.client.json').read_text())
                        require(sr['drained'] and sr['frames']==cr['frames']==self.a.iterations,'serial','incomplete frame trace')
                        require(cr['validated_bytes']==size*self.a.iterations,'serial','payload validation gap')
                        r.update(status='passed',server=sr,client=cr)
                    except Exception as e:r.update(status='failed',error=str(e));raise
                    finally:
                        if server and server.poll() is None:berry.Runner.kill(server);server.wait()
                        self.save(r)
    def run(self):
        error=None
        try:
            for suite in (['surface','serial','tcp'] if self.a.suite=='all' else [self.a.suite]):getattr(self,suite if suite!='soak' else 'tcp')(**({'soak':True} if suite=='soak' else {}))
        except BaseException as e:
            error=str(e);raise
        finally:
            try:self.build.verify_source();unchanged=True
            except Exception as e:unchanged=False;error=error or str(e)
            result=campaign_summary(self.records,self.manifest['expected_records'],unchanged,error)
            (self.a.output/'summary.json').write_text(json.dumps(result,indent=2)+'\n')
            if error is None:require(result['status']=='passed','campaign','incomplete planned execution')
            elif sys.exc_info()[0] is None:raise ValueError(error)


def expected_records(suite,repeats):
    counts=dict(surface=50*repeats,serial=9*repeats,tcp=12*repeats,soak=6)
    return sum(counts[k] for k in ['surface','serial','tcp']) if suite=='all' else counts[suite]


def campaign_summary(records,expected,unchanged,error):
    statuses={s:sum(r['status']==s for r in records) for s in ['passed','failed','incomplete']}
    passed=unchanged and error is None and len(records)==expected and statuses['passed']==expected
    return dict(status='passed' if passed else 'incomplete',records=len(records),expected_records=expected,
                statuses=statuses,source_unchanged=unchanged,error=error,release_qualified=False)


def main():
    # Used only as an exec prefix for a freshly created delegated child.
    if len(sys.argv)>3 and sys.argv[1]=='--join-cgroup':
        path=Path(sys.argv[2]);require(path.name.startswith('abps-streaming-'),'cgroup','unexpected child')
        (path/'cgroup.procs').write_text(str(os.getpid()));os.execvp(sys.argv[3],sys.argv[3:])
    p=argparse.ArgumentParser(description=__doc__)
    p.add_argument('--build-record',type=Path,required=True);p.add_argument('--output',type=Path,required=True)
    p.add_argument('--suite',choices=['all','surface','serial','tcp','soak'],default='all')
    p.add_argument('--profile',choices=['screen','qualify'],default='screen')
    p.add_argument('--dedicated-host',action='store_true');p.add_argument('--cgroup-parent',type=Path)
    p.add_argument('--iterations',type=int,default=10000)
    a=p.parse_args();a.seconds=15 if a.profile=='screen' else 60;a.repeats=3 if a.profile=='screen' else 7
    if a.profile=='qualify' and not a.dedicated_host:p.error('qualification requires a dedicated host')
    if not 1<=a.iterations<=20000000:p.error('invalid iteration bound')
    Study(a).run()
if __name__=='__main__':main()
