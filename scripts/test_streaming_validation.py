#!/usr/bin/env python3
"""AP-18/19 independent manifest and observation negative controls."""
import copy
import importlib.util
from pathlib import Path
import unittest
from unittest import mock
from types import SimpleNamespace
import tempfile
import json
spec=importlib.util.spec_from_file_location('streaming',Path(__file__).with_name('streaming-validation.py'))
v=importlib.util.module_from_spec(spec);spec.loader.exec_module(v)

class Contracts(unittest.TestCase):
    def test_matched_resources_and_disabled_policies(self):
        cases=v.scenarios(15)
        self.assertEqual(len(cases),12)
        for offset in range(0,len(cases),3):
            group=cases[offset:offset+3]
            for k in ['queue','streams','fanout','rate','class-slots','class-capacities','domain-limit-bytes','seconds','overflow','seed','write-batch','max-frame-age-ms']:
                self.assertEqual(len({c['options'][k] for c in group}),1,k)
            for c in group:
                self.assertEqual(c['options']['admission-pacing'],'off')
                self.assertEqual(c['options']['memory-policy'],'off')
                self.assertNotIn('warmup',c['options'])
    def test_full_occupancy_cannot_pass_with_success(self):
        r=dict(iterations=10,accepted=0,errors={'BudgetExceeded':10},remote_dispatched=0,remote_returned=0,held_slots=2,slots=2,drained=dict(admitted=0,available=2,total_slots=2))
        v.validate_surface(r)
        for change in [dict(accepted=1),dict(remote_returned=1),dict(drained={}),dict(drained=dict(admitted=1,available=2,total_slots=2))]:
            bad=copy.deepcopy(r);bad.update(change)
            with self.assertRaises((ValueError,KeyError)):v.validate_surface(bad)
    def test_no_cgroup_delegation_is_not_zero_memory(self):
        cg=v.Cgroup(None);self.assertEqual(cg.sample()['status'],'unsupported')
        self.assertNotIn('memory.current',cg.sample());cg.close()
    def test_partial_campaign_and_source_drift_never_pass(self):
        self.assertEqual(v.expected_records('all',3),213)
        self.assertEqual(v.expected_records('soak',7),6)
        passed=[dict(status='passed')]*3
        self.assertEqual(v.campaign_summary(passed,3,True,None)['status'],'passed')
        for records,expected,unchanged,error in [(passed,4,True,None),
                (passed,3,False,None),(passed,3,True,'cleanup failed'),
                (passed+[dict(status='failed')],4,True,None),([],3,True,None)]:
            self.assertEqual(v.campaign_summary(records,expected,unchanged,error)['status'],'incomplete')

    def test_failed_tcp_invocation_is_retained(self):
        study=object.__new__(v.Study)
        study.a=SimpleNamespace(repeats=1,cgroup_parent=None)
        study.manifest=dict(tcp=[dict(id='test',options={})])
        study.runner=SimpleNamespace(options=SimpleNamespace(server_prefix=''),tcp=mock.Mock(side_effect=ValueError('bad payload')))
        records=[];study.save=records.append
        with self.assertRaisesRegex(ValueError,'bad payload'):study.tcp()
        self.assertEqual(len(records),1)
        self.assertEqual(records[0]['status'],'failed')
        self.assertIn('bad payload',records[0]['error'])

    def test_source_verification_failure_writes_incomplete_summary(self):
        with tempfile.TemporaryDirectory() as out:
            study=object.__new__(v.Study)
            study.a=SimpleNamespace(suite='surface',output=Path(out))
            study.records=[dict(status='passed')]
            study.manifest=dict(expected_records=1)
            study.surface=lambda:None
            study.build=SimpleNamespace(verify_source=mock.Mock(side_effect=ValueError('source drift')))
            with self.assertRaisesRegex(ValueError,'source drift'):study.run()
            result=json.loads((Path(out)/'summary.json').read_text())
            self.assertEqual(result['status'],'incomplete')
            self.assertFalse(result['source_unchanged'])
            self.assertEqual(result['error'],'source drift')

    def test_surface_one_factor_changes(self):
        cases=v.surface_cases()
        self.assertEqual(len(cases),50)
        for c in cases:
            self.assertEqual(c['options']['slots'],64)
            self.assertIn(c['options']['remote-percent'],[0,50,100])

if __name__=='__main__':unittest.main()
