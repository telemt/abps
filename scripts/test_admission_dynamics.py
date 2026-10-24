import copy
import importlib.util
from pathlib import Path
import unittest

spec=importlib.util.spec_from_file_location('admission_dynamics',Path(__file__).with_name('admission-dynamics.py'))
m=importlib.util.module_from_spec(spec);spec.loader.exec_module(m)

class Evidence(unittest.TestCase):
    def record(self):
        return dict(drained=True,offered=3,accepted=2,core_rejected=1,expired=0,source_pending_full=0,
            admissions=[[0,0,10],[2,1_000_000_000,10]],errors={'Contended':0,'BudgetExceeded':1},
            final=dict(admitted=0,available=1,total_slots=1),timing_evidence=True,
            final_drop_intervals=[[0,1,2],[2,1_000_000_001,1_000_000_002]],
            options=dict(mode='Bucket',rate_bytes_per_second=10,burst_bytes=10))
    def test_valid_conservation(self):m.validate_trace(self.record())
    def test_rate_violation_is_detected(self):
        r=self.record();r['admissions'][1][1]=1
        with self.assertRaises(ValueError):m.validate_trace(r)
    def test_missing_final_drop_is_detected(self):
        r=self.record();r['final_drop_intervals'].pop()
        with self.assertRaises(ValueError):m.validate_trace(r)
    def test_duplicate_identity_is_detected(self):
        r=self.record();r['final_drop_intervals'][1][0]=0
        with self.assertRaises(ValueError):m.validate_trace(r)
    def test_lost_offered_work_is_detected(self):
        r=self.record();r['expired']=1
        with self.assertRaises(ValueError):m.validate_trace(r)
    def test_sequential_contention_is_not_normalized(self):
        r=self.record();r['timing_evidence']=False;r['errors']={'Contended':1}
        with self.assertRaises(ValueError):m.validate_trace(r)
    def test_profile_preserves_fixed_physical_budget(self):
        a=m.profile(fanout=1);b=m.profile(fanout=100,slow=True)
        for k in ['class-capacities','class-slots','domain-limit-bytes']:
            self.assertEqual(a[k],b[k])
        self.assertEqual(b['return-workers'],1)

if __name__=='__main__':unittest.main()
