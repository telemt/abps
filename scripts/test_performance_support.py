#!/usr/bin/env python3
"""AP-09 negative controls for measurement validation and durable run status."""
import copy
import importlib.util
import json
from pathlib import Path
import tempfile
import unittest
from types import SimpleNamespace
from unittest.mock import patch
import performance_support as v


def fixture():
    hist = dict(samples=1, out_of_range=0)
    consumer = dict(delivered=1, bytes=64, timely_delivered=1, expired_delivered=0,
                    timely_bytes=64, invalid=0, scheduled_arrival_latency=hist)
    client = dict(failed=False, measurement_seconds=1, source=dict(planned=1,attempted=1,
        missed_arrivals=0,enqueued=1,queue_full=0,queue_closed=0,sent=1,failed_writes=0),
        consumers=[consumer],total=copy.deepcopy(consumer))
    stream = dict(received=1,accepted=1,discarded=0,enqueued=1,cancelled_before_forward=0,
        queue_credit_attempts=1,dispatched_recipient_rights=1,locally_enqueued=1,
        skipped=0,queue_rejected=0,no_subscriber_deliveries=0,ingress_rejected_deliveries=0,
        contended=0,contention_by_operation=dict(admission=0),discard_reasons=dict(NoReceivers=0))
    worker = dict(destroyed=True,flow_complete=True,drained=dict(admitted=0,available=2,total_slots=2),
        streams=[stream],writer_completed=1,write_api_calls=1)
    server = dict(failed=False,unconfirmed_workers=0,fanout=1,workers=[worker],shared_drained=None,
        supervisor=dict(charged_bytes=0,abandoned_bytes=0,abandoned_roots=0,unconfirmed_bytes=0))
    return server,client


class PerformanceContracts(unittest.TestCase):
    def test_tokio_receipts_reject_lost_and_duplicated_ownership(self):
        server, client = fixture()
        server['pipeline_plan'] = {'queue_backend':'tokio', 'queue_accounting':'receipts-v1'}
        w = server['workers'][0]
        w['writer_aborted'] = dict.fromkeys(('deadline','policy','io','cancel','unwind'),0)
        w['tokio_queues'] = [dict(stream=0, subscriber=0, metrics=dict(enqueued=1, transferred=1, cancelled=0))]
        self.assertEqual(v.validate_issuer_flow(server,client)['unclassified_uncompleted_enqueued'], 0)
        for name in ['enqueued','transferred','cancelled']:
            bad=copy.deepcopy(server)
            bad['workers'][0]['tokio_queues'][0]['metrics'][name]+=1
            with self.assertRaises(v.MeasurementError): v.validate_issuer_flow(bad,client)
        for name in ['tokio_queues','writer_aborted']:
            bad=copy.deepcopy(server);del bad['workers'][0][name]
            with self.assertRaises(v.MeasurementError): v.validate_issuer_flow(bad,client)
        # A cancelled queue entry remains an offered loss, not a writer abort.
        for k in ['received','accepted','enqueued','dispatched_recipient_rights','locally_enqueued']:
            w['streams'][0][k]+=1
        for k in ['planned','attempted','enqueued','sent']: client['source'][k]+=1
        w['tokio_queues'][0]['metrics'].update(enqueued=2, cancelled=1)
        result=v.validate_issuer_flow(server,client)
        self.assertEqual(result['cancelled_queued'],1)
        self.assertEqual(result['delivery_fraction'],.5)
        self.assertEqual(result['unclassified_uncompleted_enqueued'],0)

    def test_writer_abort_schema_cannot_be_partially_missing(self):
        server, client = fixture()
        # A writer-only worker need not own a domain, but it must report the
        # same loss schema. One missing report must not disable all accounting.
        other = copy.deepcopy(server['workers'][0])
        other.update(streams=[], drained=None, writer_completed=0, write_api_calls=0)
        server['workers'].append(other)
        self.assertFalse(v.validate_issuer_flow(server,client)['writer_abort_classification'])
        reasons = dict.fromkeys(('deadline','policy','io','cancel','unwind'),0)
        for first in [0, 1]:
            server['workers'][first]['writer_aborted'] = reasons.copy()
            with self.assertRaisesRegex(v.MeasurementError, 'mixed writer abort schemas'):
                v.validate_issuer_flow(server,client)
            del server['workers'][first]['writer_aborted']
        for worker in server['workers']:
            worker['writer_aborted'] = reasons.copy()
        self.assertTrue(v.validate_issuer_flow(server,client)['writer_abort_classification'])

    def test_writer_abort_counts_cannot_hide_or_duplicate_local_losses(self):
        server, client = fixture()
        worker = server['workers'][0]
        worker['writer_aborted'] = dict.fromkeys(('deadline','policy','io','cancel','unwind'),0)
        self.assertTrue(v.validate_issuer_flow(server,client)['writer_abort_classification'])
        for reason in worker['writer_aborted']:
            worker['writer_aborted'][reason] = 1
            with self.assertRaises(v.MeasurementError): v.validate_issuer_flow(server,client)
            worker['writer_aborted'][reason] = 0
        del worker['writer_aborted']['cancel']
        with self.assertRaises(v.MeasurementError): v.validate_issuer_flow(server,client)

    def test_fixed_arena_profiles_never_scale_payload_budget_with_queues(self):
        spec=importlib.util.spec_from_file_location('review',Path(__file__).with_name('performance-review.py'))
        m=importlib.util.module_from_spec(spec);spec.loader.exec_module(m)
        for q in [8,32,64]:
            p=m.fixed_arena_profile(q)
            self.assertEqual((p['class-slots'],p['domain-limit-bytes'],p['capacity-check']),('130,130',97500,'report'))
            self.assertEqual(p['queue'],q)
        with self.assertRaises(ValueError):m.fixed_arena_profile(freshness='oldest')

    def test_local_queue_losses_are_not_hidden_and_credits_must_drain(self):
        server,client=fixture()
        server['pipeline_plan']={'queue_backend':'local','freshness_priority':'oldest'}
        metrics=dict(capacity=2,prepared_capacity=2,storage_model_bytes=100,free=2,reserved=0,queued=0,transferring=0,releasing=0,
            enqueued=1,transferred=1,evicted=0,failed_replacements=0,cancelled=0,rejected_full=0,occupancy_high_water=1,queue_age_samples=1,queue_age_max_ns=10)
        server['workers'][0]['local_queues']=[dict(stream=0,subscriber=0,metrics=metrics)]
        self.assertEqual(v.validate_issuer_flow(server,client)['freshness_evicted'],0)
        for name in ['free','reserved','queued','transferring','releasing','enqueued','transferred','evicted','failed_replacements']:
            bad=copy.deepcopy(server);bad['workers'][0]['local_queues'][0]['metrics'][name]+=1
            with self.assertRaises(v.MeasurementError):v.validate_issuer_flow(bad,client)
        # One previously enqueued frame was evicted; it remains in planned losses.
        stream=server['workers'][0]['streams'][0]
        for name in ['received','accepted','enqueued','dispatched_recipient_rights','locally_enqueued']:stream[name]+=1
        for name in ['planned','attempted','enqueued','sent']:client['source'][name]+=1
        metrics.update(enqueued=2,evicted=1,failed_replacements=1)
        result=v.validate_issuer_flow(server,client)
        self.assertEqual(result['delivery_fraction'],.5)
        self.assertEqual(result['uncompleted_enqueued'],1)
        self.assertEqual(result['unclassified_uncompleted_enqueued'],0)

    def test_adaptive_report_rejects_invented_loss_phases(self):
        server,client=fixture()
        server['pipeline_plan']={'queue_backend':'local','freshness_priority':'adaptive'}
        a=dict(window=2,pressure_q16=0,debt_ms=0,state='streaming',disconnect_reason=None,
            age_evictions=0,window_evictions=0,dependency_evictions=0,dependency_skips=0,
            policy_rejections=0,stale_commits=0,recovery_generation=0)
        m=dict(capacity=2,prepared_capacity=2,storage_model_bytes=100,free=2,reserved=0,
            queued=0,transferring=0,releasing=0,enqueued=1,transferred=1,evicted=0,
            failed_replacements=0,cancelled=0,rejected_full=0,occupancy_high_water=1,
            queue_age_samples=1,queue_age_max_ns=10,adaptive=a)
        server['workers'][0]['local_queues']=[dict(stream=0,subscriber=0,metrics=m)]
        self.assertEqual(v.validate_issuer_flow(server,client)['delivery_fraction'],1)
        for key,value in [('window',3),('pressure_q16',65537),('age_evictions',1),
            ('disconnect_reason','unknown'),('state','reclaimed')]:
            old=a[key];a[key]=value
            with self.assertRaises(v.MeasurementError):v.validate_issuer_flow(server,client)
            a[key]=old

    def test_probe_requires_matching_arguments_and_nonempty_result(self):
        args={'iterations':10}
        good={'args':{'iterations':'10'},'result':{'drained':True}}
        self.assertEqual(v.validate_probe_envelope(good,args),good['result'])
        for bad in [{}, {'args':{}},dict(good,args={'iterations':'11'}),
                    dict(good,result={}),dict(good,result=[]),dict(good,result={'failed':True})]:
            with self.assertRaises(v.MeasurementError):v.validate_probe_envelope(bad,args)

    def test_positive_and_residual_stages(self):
        server,client=fixture()
        result=v.validate_issuer_flow(server,client)
        self.assertEqual(result['delivery_fraction'],1)
        self.assertEqual(result['network_bytes_per_planned_s'],64)
        self.assertEqual(result['uncompleted_enqueued'],0)
        self.assertEqual(result['unobserved_completed'],0)

    def test_each_conservation_equation_has_negative_control(self):
        changes=[('source','planned'),('source','attempted'),('source','enqueued'),
                 ('source','sent'),('total','delivered'),('total','bytes'),('total','timely_delivered')]
        for section,name in changes:
            with self.subTest(section=section,name=name):
                server,client=fixture();client[section][name] += 1 if name!='sent' else -1
                with self.assertRaises(v.MeasurementError):v.validate_issuer_flow(server,client)
        for name in ['received','accepted','discarded','enqueued','cancelled_before_forward','dispatched_recipient_rights','contended']:
            with self.subTest(name=name):
                server,client=fixture();server['workers'][0]['streams'][0][name]+=1
                with self.assertRaises(v.MeasurementError):v.validate_issuer_flow(server,client)

    def test_errors_are_not_success_even_when_flow_balances(self):
        for field in ['failed','unconfirmed_workers']:
            server,client=fixture();server[field]=True if field=='failed' else 1
            with self.assertRaises(v.MeasurementError):v.validate_issuer_flow(server,client)
        for field in ['destroyed','flow_complete']:
            server,client=fixture();server['workers'][0][field]=False
            with self.assertRaises(v.MeasurementError):v.validate_issuer_flow(server,client)
        server,client=fixture();server['workers'][0]['error']='failed worker'
        with self.assertRaises(v.MeasurementError):v.validate_issuer_flow(server,client)
        for field in ['admitted','available']:
            server,client=fixture();server['workers'][0]['drained'][field]+=1
            with self.assertRaises(v.MeasurementError):v.validate_issuer_flow(server,client)
        for field in ['charged_bytes','abandoned_bytes','abandoned_roots','unconfirmed_bytes']:
            server,client=fixture();server['supervisor'][field]+=1
            with self.assertRaises(v.MeasurementError):v.validate_issuer_flow(server,client)
        for total in [True,False]:
            server,client=fixture();(client['total'] if total else client['consumers'][0])['invalid']=1
            with self.assertRaises(v.MeasurementError):v.validate_issuer_flow(server,client)

    def test_invalid_counter_types_missing_fields_and_duration(self):
        for value in [True,False,-1,1.0,float('nan'),float('inf'),'1',None]:
            server,client=fixture();client['source']['planned']=value
            with self.assertRaises(v.MeasurementError):v.validate_issuer_flow(server,client)
        for value in [True,0,-1,float('nan'),float('inf'),None]:
            server,client=fixture();client['measurement_seconds']=value
            with self.assertRaises(v.MeasurementError):v.validate_issuer_flow(server,client)
        server,client=fixture();del client['source']['planned']
        with self.assertRaisesRegex(v.MeasurementError,'source.planned'):v.validate_issuer_flow(server,client)

    def test_histogram_overflow_retains_delivery(self):
        server,client=fixture()
        for item in [client['total'],client['consumers'][0]]:
            item['scheduled_arrival_latency']=dict(samples=0,out_of_range=1)
        self.assertEqual(v.validate_issuer_flow(server,client)['delivered'],1)

    def test_writer_only_null_is_legal_but_issuer_null_is_not(self):
        server,client=fixture();writer=copy.deepcopy(server['workers'][0]);writer.update(streams=[],drained=None,writer_completed=0,write_api_calls=0)
        server['workers'].append(writer);v.validate_issuer_flow(server,client)
        server['workers'][0]['drained']=None
        with self.assertRaises(v.MeasurementError):v.validate_issuer_flow(server,client)

    def test_reference_and_branch_validation(self):
        server,client=fixture()
        result=dict(client,accepted=1,subscribers=1,enqueued=1,skipped=0,queue_rejected=0,
                    no_subscriber_deliveries=0,undelivered_enqueued=0,drained=server['workers'][0]['drained'])
        result['source']['not_attempted_after_stop']=0
        v.validate_reference_flow(result)
        result['source']['planned']=2
        with self.assertRaises(v.MeasurementError):v.validate_reference_flow(result)
        branch=dict(drained=True,offered_views=2,accepted_views=1,contended_views=1)
        v.validate_branch_result(branch,'scalar')
        with self.assertRaises(v.MeasurementError):v.validate_branch_result(branch,'local')
        branch['offered_views']=3
        with self.assertRaises(v.MeasurementError):v.validate_branch_result(branch,'scalar')

    def test_resume_requires_qualified_success_and_preserves_failures(self):
        with tempfile.TemporaryDirectory() as directory:
            path=Path(directory)/'results.jsonl'
            good=dict(schema_version=2,id='x',status='passed')
            for bad in [dict(good,status=s) for s in ['failed','incomplete','unvalidated']] + [dict(good,schema_version=1),{'id':'x'}]:
                path.write_text(json.dumps(bad)+'\n'+json.dumps(good)+'\n')
                with self.assertRaises(v.MeasurementError):v.read_records(path)
            path.write_text(json.dumps(good)+'\n');self.assertEqual(list(v.read_records(path)),['x'])

    def test_topology_respects_packages_siblings_and_cpuset(self):
        with tempfile.TemporaryDirectory() as directory:
            root=Path(directory)
            for cpu,package,core in [(0,0,0),(1,0,0),(2,0,1),(3,1,0)]:
                p=root/f'cpu{cpu}'/'topology';p.mkdir(parents=True)
                (p/'physical_package_id').write_text(str(package));(p/'core_id').write_text(str(core))
            self.assertEqual(v.physical_cpus([0,1,2,3],root),[0,2,3])
            self.assertEqual(v.physical_cpus([1,2,3],root),[1,2,3])
            with self.assertRaises(v.MeasurementError):v.physical_cpus([5],root)

    def test_probe_failure_and_timeout_are_persisted(self):
        spec=importlib.util.spec_from_file_location('berry',Path(__file__).with_name('berry-performance.py'))
        module=importlib.util.module_from_spec(spec);spec.loader.exec_module(module)
        for error,status in [(RuntimeError('nonzero exit'),'failed'),(TimeoutError('deadline'),'incomplete')]:
            with tempfile.TemporaryDirectory() as directory:
                runner=object.__new__(module.Runner)
                runner.options=SimpleNamespace(micro_cpus=[0]);runner.done={};runner.bin=Path('/unused')
                runner.output=Path(directory);runner.raw=runner.output/'results.jsonl';runner.identity={'source':'fixture'}
                process=SimpleNamespace(returncode=1)
                with patch.object(module.subprocess,'Popen',return_value=process),patch.object(runner,'wait',side_effect=error):
                    with self.assertRaises(type(error)):runner.run('failure',{})
                record=json.loads(runner.raw.read_text())
                self.assertEqual(record['status'],status)
                self.assertEqual(record['exit_codes'],[1])
                self.assertTrue(record['errors'])
                self.assertEqual(runner.done,{})
                self.assertTrue((runner.output/'failure-0.stderr').exists())

    def test_unvalidated_save_and_plan_timeout_never_become_success(self):
        spec=importlib.util.spec_from_file_location('berry',Path(__file__).with_name('berry-performance.py'))
        module=importlib.util.module_from_spec(spec);spec.loader.exec_module(module)
        with tempfile.TemporaryDirectory() as directory:
            runner=object.__new__(module.Runner)
            runner.options=SimpleNamespace(server_cpus=[0],generator_cpus=[2],reader_cpus=[4],server_binary=None,server_prefix='')
            runner.done={};runner.bin=Path('/unused');runner.output=Path(directory)
            runner.raw=runner.output/'results.jsonl';runner.identity={}
            runner.save('raw',{'result':{}})
            self.assertEqual(json.loads(runner.raw.read_text())['status'],'incomplete')
            self.assertEqual(runner.done,{})
            def timed_out(command,**kwargs):
                kwargs['stdout'].write('partial preflight output')
                kwargs['stderr'].write('partial diagnostic')
                raise module.subprocess.TimeoutExpired(command,30)
            with patch.object(module,'physical_cpus',side_effect=lambda cpus:cpus),patch.object(module.subprocess,'run',side_effect=timed_out):
                with self.assertRaises(module.subprocess.TimeoutExpired):runner.tcp('timeout',{'seconds':1})
            record=json.loads(runner.raw.read_text().splitlines()[-1])
            self.assertEqual(record['status'],'incomplete')
            self.assertEqual((runner.output/'timeout-0.plan.stdout').read_text(),'partial preflight output')
            self.assertEqual((runner.output/'timeout-0.plan.stderr').read_text(),'partial diagnostic')
            self.assertEqual(record['exit_codes'],[None,None])

if __name__=='__main__':unittest.main()
