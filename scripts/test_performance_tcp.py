#!/usr/bin/env python3
"""AP-09 real-process relay/affinity negative controls (prebuilt spike_probe).

Run with ABPS_SPIKE_PROBE=/absolute/path/to/spike_probe. No compiler runs here.
"""
import json
import os
from pathlib import Path
import socket
import struct
import subprocess
import threading
import tempfile
import sys
import unittest
from performance_support import physical_cpus

BINARY=os.environ.get('ABPS_SPIKE_PROBE')


def exact(sock,n):
    data=b''
    while len(data)<n:
        block=sock.recv(n-len(data))
        if not block:raise RuntimeError(f'premature source EOF: {len(data)}/{n}')
        data+=block
    return data


@unittest.skipUnless(BINARY,'requires prebuilt ABPS_SPIKE_PROBE')
class PerformanceProcessContracts(unittest.TestCase):
    def relay(self,mutation):
        with socket.socket() as listener:
            listener.bind(('127.0.0.1',0));listener.listen();listener.settimeout(5)
            port=listener.getsockname()[1];errors=[]
            def forward():
                peers=[]
                try:
                    source=consumer=None
                    for _ in range(2):
                        peer,_=listener.accept();peer.settimeout(5);peers.append(peer)
                        stream,subscriber=struct.unpack('!II',exact(peer,8))
                        if stream!=0:raise RuntimeError('wrong stream')
                        if subscriber==1:source=peer
                        elif subscriber==0:consumer=peer
                        else:raise RuntimeError('wrong subscriber')
                    source.sendall(b'\x01')
                    header=exact(source,20);size=struct.unpack('!I',header[:4])[0]
                    payload=bytearray(exact(source,size))
                    wire=bytearray(header)+payload
                    mutation(wire)
                    consumer.sendall(wire)
                    consumer.shutdown(socket.SHUT_WR)
                except Exception as error:errors.append(error)
                finally:
                    for peer in peers:peer.close()
            thread=threading.Thread(target=forward);thread.start()
            try:
                result=subprocess.run([BINARY,'--case=tcp','--role=loadgen','--backend=mosaic',
                    '--threads=1','--streams=1','--fanout=1','--seconds=.1','--rate=10',f'--ports={port}'],
                    capture_output=True,text=True,timeout=10)
            finally:
                thread.join(timeout=6)
            self.assertFalse(thread.is_alive());self.assertEqual(errors,[])
            return result,json.loads(result.stdout)['result']

    def test_whole_payload_corruption_has_json_and_nonzero_exit(self):
        for position in ['first','middle','last']:
            def corrupt(wire):
                i={'first':20,'middle':20+(len(wire)-20)//2,'last':len(wire)-1}[position]
                wire[i]^=1
            process,report=self.relay(corrupt)
            self.assertNotEqual(process.returncode,0)
            self.assertTrue(report['failed'])
            self.assertEqual(report['total']['invalid'],1)
            self.assertEqual(report['total']['delivered'],0)
            self.assertEqual(report['consumers'][0]['terminal'],'invalid_payload')

    def test_truncation_is_observed_without_inventing_corruption(self):
        def truncate(wire):del wire[-3:]
        process,report=self.relay(truncate)
        self.assertEqual(process.returncode,0,process.stderr)
        self.assertFalse(report['failed'])
        self.assertEqual(report['consumers'][0]['terminal'],'truncated_payload')
        self.assertEqual(report['total']['delivered'],0)
        self.assertEqual(report['total']['invalid'],0)

    def test_valid_frame_is_delivered(self):
        process,report=self.relay(lambda wire:None)
        self.assertEqual(process.returncode,0,process.stderr)
        self.assertEqual(report['total']['delivered'],1)
        self.assertEqual(report['total']['invalid'],0)

    def test_mixed_probe_has_distinct_actual_affinity_and_windows(self):
        cpus=physical_cpus()
        if len(cpus)<2:self.skipTest('requires two physical cores')
        for kind in ['pages','grants']:
            process=subprocess.run([BINARY,'--case=observability','--backend=mosaic','--op=mixed',
                '--iterations=10000',f'--cpus={cpus[0]},{cpus[1]}',f'--diagnostic-kind={kind}'],capture_output=True,text=True,timeout=15)
            self.assertEqual(process.returncode,0,process.stderr)
            result=json.loads(process.stdout)['result'];affinity=result['affinity']
            for role,cpu in [('issuer',cpus[0]),('returner',cpus[1])]:
                for phase in ['begin','end']:self.assertEqual(affinity[f'{role}_{phase}']['allowed_cpus'],str(cpu))
            self.assertNotEqual(affinity['issuer_begin']['tid'],affinity['returner_begin']['tid'])
            self.assertEqual(result['admission']['attempted'],10000)
            self.assertEqual(result['remote_final_drops']+result['queue_rejected'],result['admission']['accepted']+result['startup_final_drops'])
            self.assertGreater(result['admission_loop_seconds'],0)
            self.assertGreaterEqual(result['completed_cycle_seconds'],result['admission_loop_seconds'])
        for selection in [str(cpus[0]),f'{cpus[0]},{cpus[0]}']:
            process=subprocess.run([BINARY,'--case=observability','--op=mixed',f'--cpus={selection}'],capture_output=True,text=True,timeout=5)
            self.assertNotEqual(process.returncode,0)

    def test_plan_role_never_prints_server_readiness_and_rejects_deficits(self):
        base=[BINARY,'--case=tcp','--role=plan','--backend=mosaic','--streams=2','--fanout=4',
            '--queue=8','--write-batch=8','--capacity-policy=reserved','--capacity-check=require-envelope']
        for slots,budget,good in [('130,130',97500,True),('129,130',97500,False),('130,130',97499,False)]:
            p=subprocess.run(base+[f'--class-slots={slots}',f'--domain-limit-bytes={budget}'],capture_output=True,text=True,timeout=5)
            self.assertEqual(p.returncode==0,good,p.stderr)
            if good:
                self.assertEqual(len(p.stdout.splitlines()),1)
                report=json.loads(p.stdout)['result']
                self.assertEqual(report['role'],'plan');self.assertNotIn('ports',report)
                self.assertEqual(report['pipeline_plan']['roots'][0]['domains'][0]['required_byte_limit'],97500)

    def test_preparation_resume_keeps_successful_records_immutable(self):
        target=Path(BINARY).resolve().parents[2]
        with tempfile.TemporaryDirectory() as directory:
            output=Path(directory)/'results'
            command=[sys.executable,*(['-O'] if sys.flags.optimize else []),str(Path(__file__).with_name('mosaic-performance.py')),
                     '--suite=preparation','--repeats=1','--target='+str(target),'--output='+str(output)]
            first=subprocess.run(command,capture_output=True,text=True,timeout=30)
            self.assertEqual(first.returncode,0,first.stderr)
            original=(output/'results.jsonl').read_bytes()
            second=subprocess.run(command,capture_output=True,text=True,timeout=30)
            self.assertEqual(second.returncode,0,second.stderr)
            self.assertEqual((output/'results.jsonl').read_bytes(),original)
            self.assertEqual(len(original.splitlines()),4)

if __name__=='__main__':
    if not BINARY:raise SystemExit('ABPS_SPIKE_PROBE is required; process checks not executed')
    program=unittest.main(exit=False)
    if program.result.skipped:raise SystemExit('unvalidated: required process checks were skipped')
    raise SystemExit(0 if program.result.wasSuccessful() else 1)
