"""AP-13 experiment ledger negative controls; valid under python -O."""
import copy
import importlib.util
from pathlib import Path
import unittest
spec=importlib.util.spec_from_file_location('memory_review',Path(__file__).with_name('memory-review.py'))
m=importlib.util.module_from_spec(spec);spec.loader.exec_module(m)
class MemoryEvidence(unittest.TestCase):
    def fixture(self):
        return {'server':{'workers':[{'drained':{'memory':{'reserved_bytes':0,'peak_reserved_bytes':125,
            'attempts':1,'records':[{}],'trace_complete':True,'trace_overflow':0,
            'plan':{'ceiling_bytes':125,'original_reservation_bytes':100},
            'streams':[{'admitted':0,'soft':50,'hard':100}]}}}]}}
    def test_accepts_confirmed_bounds(self):m.validate_memory(self.fixture())
    def test_retains_violations(self):
        for key,value in [('reserved_bytes',1),('reserved_bytes',False),('peak_reserved_bytes',126),('peak_reserved_bytes',-1),('peak_reserved_bytes',0),('attempts',65),('attempts',-1),('trace_complete',False),('trace_complete',1),('trace_overflow',1),('records',[{}]*33),('records',[]),('streams',[])]:
            r=self.fixture();r['server']['workers'][0]['drained']['memory'][key]=value
            with self.assertRaises(ValueError):m.validate_memory(r)
    def test_rejects_invalid_stream_counters(self):
        for key,value in [('admitted',1),('admitted',False),('soft',-1),('soft',101),('hard',False)]:
            r=self.fixture();r['server']['workers'][0]['drained']['memory']['streams'][0][key]=value
            with self.assertRaises(ValueError):m.validate_memory(r)
    def test_profiles_do_not_silently_scale_storage(self):
        base=m.profile()
        for policy in ['fixed','adaptive']:
            for elasticity in ['off','cold','warm']:
                p=m.profile(policy,elasticity)
                for key in ['class-slots','domain-limit-bytes','fanout','queue']:self.assertEqual(p[key],base[key])
    def test_shared_ablation_keeps_capacity_and_has_distinct_controls(self):
        variants=dict(m.shared_variants())
        self.assertEqual(len(variants),8)
        self.assertEqual(variants['off'],{})
        for name,changes in variants.items():
            self.assertFalse(set(changes)&{'class-slots','queue','fanout','domain-limit-bytes','elasticity'})
        a=variants['shared-10-eligible-full'];b=variants['shared-10-eligible-cached']
        self.assertEqual({k:v for k,v in a.items() if k!='memory-accounting'},
                         {k:v for k,v in b.items() if k!='memory-accounting'})
    def shared_fixture(self):
        r=self.fixture();bank=r['server']['workers'][0]['drained']['memory']
        bank.update(peak_reserved_bytes=100,attempts=0,grown=0,shrunk=0)
        bank['plan'].update(mode='Shared',initial_roots=1,standby_roots=0,soft_total=100)
        bank['shared']={'streams':[{'offered_bytes':100,'eligible_bytes':80,'accepted_bytes':60,
                                   'observed_refunded_bytes':60,'observed_debt':0,'observed_peak_admitted':60}]}
        return r
    def test_shared_reconciles_backing_not_views(self):
        m.validate_memory(self.shared_fixture())
        for key,value in [('eligible_bytes',101),('accepted_bytes',81),('observed_refunded_bytes',59),
                          ('observed_refunded_bytes',61),('observed_debt',1),('observed_peak_admitted',101)]:
            r=self.shared_fixture()
            r['server']['workers'][0]['drained']['memory']['shared']['streams'][0][key]=value
            with self.assertRaises(ValueError):m.validate_memory(r)
    def test_shared_rejects_growth_missing_observations_and_trace_overdraft(self):
        for key,value in [('peak_reserved_bytes',101),('grown',1),('shrunk',1),('attempts',1),('shared',None),
                          ('trace',[{'observed_debt':1,'admitted':101}])]:
            r=self.shared_fixture()
            r['server']['workers'][0]['drained']['memory'][key]=value
            with self.assertRaises(ValueError):m.validate_memory(r)
