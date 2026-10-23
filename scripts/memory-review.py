#!/usr/bin/env python3
"""AP-13/14 isolated memory-policy, topology and arrival experiments."""
import argparse
import importlib.util
import json
import os
from pathlib import Path
from performance_support import cpu_selection, require, count
spec=importlib.util.spec_from_file_location('berry',Path(__file__).with_name('berry-performance.py'))
berry=importlib.util.module_from_spec(spec);spec.loader.exec_module(berry)

def profile(policy='off', elasticity='off', *, priority='weighted', headroom=25, pattern='steady'):
    p={'backend':'mosaic','threads':1,'streams':8,'fanout':8,'queue':64,
       'queue-backend':'local','return':'local','overflow':'skip','write-batch':8,
       'freshness-priority':'adaptive','class-slots':'128,128','domain-limit-bytes':128768,
       'capacity-policy':'unreserved','capacity-check':'report','max-frame-age-ms':100,
       'send-buffer-bytes':4096,'arrival-pattern':pattern}
    if policy!='off':p.update({'memory-policy':policy,'memory-priority':priority,'elasticity':elasticity,
                             'root-headroom-percent':headroom})
    return p

def shared_variants():
    # Each adjacent component comparison changes one policy factor. Off is the
    # delivery baseline; pooled is a control, not proof that prediction helps.
    result=[('off',{}),('legacy',{'memory-policy':'adaptive'})]
    for pressure in [0,10,50,100]:
        result.append((f'shared-{pressure}-offered-full',{
            'memory-policy':'adaptive-shared','memory-pressure-ms':pressure,
            'memory-demand':'offered','memory-accounting':'full'}))
    for accounting in ['full','cached']:
        result.append((f'shared-10-eligible-{accounting}',{
            'memory-policy':'adaptive-shared','memory-pressure-ms':10,
            'memory-demand':'eligible','memory-accounting':accounting}))
    return result

def validate_bank(m):
    p=m['plan']
    require(count(m,'reserved_bytes','memory')==0,'memory','unconfirmed teardown')
    peak=count(m,'peak_reserved_bytes','memory')
    ceiling=count(p,'ceiling_bytes','memory.plan')
    original=count(p,'original_reservation_bytes','memory.plan')
    require(0<peak<=ceiling,'memory','empty reservation or ceiling exceeded')
    require(0<original and ceiling*100<=original*125,'memory','headroom exceeds 25%')
    require(count(m,'attempts','memory')<=64 and isinstance(m['records'],list)
            and 0<len(m['records'])<=32,'memory','work/record bound exceeded')
    require(m['trace_complete'] is True and count(m,'trace_overflow','memory')==0,'memory','incomplete trace')
    require(isinstance(m['streams'],list) and 0<len(m['streams'])<=64,'memory','missing streams')
    for stream in m['streams']:
        require(count(stream,'admitted','memory.stream')==0
                and count(stream,'soft','memory.stream')<=count(stream,'hard','memory.stream'),
                'memory.stream','debt/drain invariant')
    if p.get('mode')=='Shared':
        require(peak==original and p['initial_roots']==1 and p['standby_roots']==0
                and m['attempts']==0 and m['grown']==0 and m['shrunk']==0,
                'shared','changed arena/topology')
        s=m.get('shared')
        require(isinstance(s,dict),'shared','missing observations')
        require(isinstance(s.get('streams'),list) and len(s['streams'])==len(m['streams']),
                'shared','missing stream observations')
        for parent,stream in zip(m['streams'],s['streams']):
            offered=count(stream,'offered_bytes','shared.stream')
            eligible=count(stream,'eligible_bytes','shared.stream')
            accepted=count(stream,'accepted_bytes','shared.stream')
            require(accepted<=eligible<=offered,'shared.stream','offer/admission order')
            require(count(stream,'observed_refunded_bytes','shared.stream')==accepted,
                    'shared.stream','backing refunds not reconciled after confirmed drain')
            require(count(stream,'observed_debt','shared.stream')==0 and
                    count(stream,'observed_peak_admitted','shared.stream')<=parent['hard'],
                    'shared.stream','debt/hard observation')
        for sample in m.get('trace',[]):
            require(0<=sample['observed_debt']<=sample['admitted']<=p['soft_total'],
                    'shared.trace','shared budget/debt bound')

def validate_memory(report):
    if report.get('result',{}).get('kind','').startswith('deterministic retention'):
        validate_bank(report['result']['bank']);return
    server=report.get('server',{})
    needed=server.get('pipeline_plan',{}).get('memory') is not None
    seen=0
    for worker in server.get('workers',[]):
        m=(worker.get('drained') or {}).get('memory')
        if m is None:continue
        seen+=1;validate_bank(m)
    require(not needed or seen==1,'memory','missing single issuer ledger')

class MemoryRunner(berry.Runner):
    @staticmethod
    def pressure():
        return {name:(Path('/proc/pressure')/name).read_text() for name in ('cpu','memory') if (Path('/proc/pressure')/name).exists()}
    def process_sample(self,pid,elapsed):
        sample=super().process_sample(pid,elapsed)
        try:
            fields=Path(f'/proc/{pid}/stat').read_text().rsplit(')',1)[1].split()
            faults=[int(fields[7]),int(fields[9])]
        except (OSError,IndexError,ValueError):faults=[0,0]
        return sample+faults
    def save(self,key,record):
        record['sample_columns']=['elapsed_s','rss_kib','user_ticks','system_ticks','minor_faults','major_faults']
        record['pressure_after']=self.pressure()
        if record.get('status')=='passed':
            try:validate_memory(record)
            except Exception as error:
                record.update(status='failed',errors=record.get('errors',[])+[str(error)])
                super().save(key,record)
                raise
        return super().save(key,record)

def main():
    p=argparse.ArgumentParser(description=__doc__)
    p.add_argument('--suite',choices=['smoke','replay','screen','final','soak','trace',
                                    'shared-screen','shared-replay','shared-final'],required=True)
    p.add_argument('--output',type=Path,required=True);p.add_argument('--target',type=Path,required=True)
    p.add_argument('--repeats',type=int,default=7);p.add_argument('--seconds',type=float,default=15)
    p.add_argument('--build-record',type=Path);p.add_argument('--smt',action='store_true')
    p.add_argument('--slots',type=int,choices=[16,32,64],default=32)
    p.add_argument('--pattern',choices=['steady','synchronized','shifted','phase-change'],default='synchronized')
    p.add_argument('--slow-profile',choices=['none','constant','pause','ramp','stop-resume'],default='constant')
    p.add_argument('--shared-variant',choices=[name for name,_ in shared_variants()])
    p.add_argument('--rate',type=int)
    a=p.parse_args()
    if a.repeats<1 or a.seconds<=0:p.error('positive repetitions and duration required')
    if a.rate is not None and not 1<=a.rate<=1_000_000:p.error('rate must be 1..1000000')
    cores=cpu_selection(a.smt)
    if len(cores)<4:p.error('at least four independent physical cores required')
    a.server_cpus=cores[:1];a.generator_cpus=cores[-1:];a.reader_cpus=cores[1:-1];a.micro_cpus=cores[:1]
    a.server_prefix='';a.target=a.target.resolve();runner=MemoryRunner(a)
    if a.suite.startswith('shared-'):
        for trial in range(a.repeats):
            variants=shared_variants()
            if trial%2:variants.reverse()
            for name,changes in variants:
                if a.shared_variant and name!=a.shared_variant:continue
                if a.suite=='shared-replay' and name=='off':continue
                options=profile(pattern=a.pattern)
                options.update(changes)
                options.update(seconds=a.seconds,rate=a.rate or (2000 if a.suite=='shared-replay' else 16000),
                               seed=(1_000_003 if a.suite=='shared-final' else 1009)+trial*7919,
                               **{'class-slots':f'{a.slots},{a.slots}'})
                if a.slow_profile!='none':
                    options.update(slow='yes',**{'slow-profile':a.slow_profile})
                if a.suite=='shared-replay':
                    options['case']='memory-replay'
                    options['memory-trace-capacity']=int(a.seconds*11+32)
                record=(runner.run if a.suite=='shared-replay' else runner.tcp)(name,options,trial)
                if record.get('status')!='passed':raise RuntimeError(f'failed {name}')
                validate_memory(record)
        if not runner.done:raise RuntimeError('empty shared experiment selection')
        print(json.dumps({'suite':a.suite,'output':str(a.output),'records':len(runner.done)}))
        return
    variants=[('off','off','weighted'),('fixed','off','weighted'),('adaptive','off','weighted'),
              ('adaptive','cold','weighted'),('adaptive','warm','weighted'),('adaptive','off','strict')]
    if a.suite=='final':variants=[('off','off','weighted'),('adaptive','off','weighted')]
    if a.suite=='soak':variants=[('adaptive','cold','weighted')]
    if a.suite=='trace':variants=[('adaptive','cold','weighted'),('adaptive','warm','weighted')]
    for trial in range(a.repeats):
        jobs=[]
        patterns=['steady','synchronized'] if a.suite=='screen' else ['phase-change']
        if a.suite=='replay':patterns=['steady','synchronized','shifted','phase-change']
        for policy,elasticity,priority in variants:
            if a.suite=='replay' and policy=='off':continue
            for pattern in patterns:
                options=profile(policy,elasticity,priority=priority,pattern=pattern)
                options.update(seconds=a.seconds,rate=2000 if a.suite=='replay' else 8000,seed=1009+trial*7919)
                if pattern!='steady':options.update(slow='yes',**{'slow-profile':'stop-resume'})
                if a.suite=='replay':options['case']='memory-replay'
                if a.suite=='trace':options['memory-trace-capacity']=int(a.seconds*11+32)
                jobs.append((f'{policy}-{elasticity}-{priority}-{pattern}',options))
                if a.suite=='replay' and policy=='adaptive' and priority=='weighted':
                    if elasticity=='off':
                        jobs.append((f'{policy}-{elasticity}-group-{pattern}',dict(options,**{'memory-correlation':'group'})))
                    elif elasticity=='cold':
                        for room in [0,10]:
                            jobs.append((f'{policy}-{elasticity}-room{room}-{pattern}',dict(options,**{'root-headroom-percent':room})))
                        jobs.append((f'{policy}-{elasticity}-chunk5-{pattern}',dict(options,**{'root-chunk-percent':5})))
        if trial%2:jobs.reverse()
        for name,options in jobs:
            record=(runner.run if a.suite=='replay' else runner.tcp)(name,options,trial)
            if record.get('status')!='passed':raise RuntimeError(f"failed {name}: {record}")
            # Runner already reconciles end-to-end flow. This layer additionally
            # checks the independent arena/root policy ledger (never RSS).
            validate_memory(record)
    print(json.dumps({'suite':a.suite,'output':str(a.output),'records':len(runner.done)}))
if __name__=='__main__':main()
