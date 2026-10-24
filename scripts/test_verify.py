#!/usr/bin/env python3
import importlib.util
from pathlib import Path
import tempfile
import unittest
from unittest.mock import patch
spec=importlib.util.spec_from_file_location('verify',Path(__file__).with_name('verify.py'))
v=importlib.util.module_from_spec(spec);spec.loader.exec_module(v)

class VerificationTests(unittest.TestCase):
    def test_serial_feature_does_not_redefine_existing_full_campaign(self):
        self.assertEqual(v.FULL, 'local-branches-spike,bytes-interop')
        self.assertIn('serial-adapter', v.feature_set(v.SERIAL))
        self.assertIn(v.SERIAL_ADAPTER, v.MIRI_GROUPS)
        self.assertIn('serial-adapter', v.NATIVE)
    def test_remote_authority_rejects_only_the_expected_compiler_error(self):
        import json
        import check_remote_authority as a
        row={"reason":"compiler-message","message":{"level":"error",
             "code":{"code":"E0599"},"message":"no method named `pin`",
             "spans":[{"file_name":"src/mosaic/raw.rs","is_primary":True}]}}
        end={"reason":"build-finished","success":False}
        log=json.dumps(row)+'\n'+json.dumps(end)
        self.assertTrue(a.rejected(101,log,'E0599','`pin`'))
        self.assertFalse(a.rejected(0,log,'E0599','`pin`'))
        self.assertFalse(a.rejected(101,log,'E0616','private'))
        self.assertFalse(a.rejected(101,log+'\n'+json.dumps(row),'E0599','`pin`'))
        self.assertFalse(a.rejected(101,log.replace('raw.rs','unrelated.rs'),'E0599','`pin`'))
        self.assertFalse(a.rejected(101,'could not compile','E0599','`pin`'))
        self.assertFalse(a.rejected(101,json.dumps(row),'E0599','`pin`'))
    def test_remote_authority_accepts_windows_separators_only_for_the_probe_file(self):
        import json
        import check_remote_authority as a
        end=json.dumps({"reason":"build-finished","success":False})
        def log(*spans):
            message={"level":"error","code":{"code":"E0599"},"message":"no method named `pin`",
                     "spans":[{"file_name":name,"is_primary":primary} for name,primary in spans]}
            return json.dumps({"reason":"compiler-message","message":message})+'\n'+end
        # Spans as rustc reports them on x86_64-pc-windows-msvc.
        windows=log(('src\\mosaic\\raw.rs',True),('src\\mosaic\\raw\\remote.rs',False))
        self.assertTrue(a.rejected(101,windows,'E0599','`pin`'))
        self.assertTrue(a.rejected(101,log(('src/mosaic\\raw.rs',True)),'E0599','`pin`'))
        for spans in [(('src\\mosaic\\raw.rs',False),('src\\mosaic\\raw\\remote.rs',True)),
                      (('src\\mosaic\\raw\\remote.rs',True),),(('other\\src\\mosaic\\raw.rs',True),),
                      (('src\\mosaic\\raw.rs.orig',True),)]:
            self.assertFalse(a.rejected(101,log(*spans),'E0599','`pin`'),spans)
        self.assertFalse(a.rejected(0,windows,'E0599','`pin`'))
        self.assertFalse(a.rejected(101,windows,'E0616','private'))
    def test_loom_keeps_bounds_without_interleaving_result_lines(self):
        import argparse
        r=object.__new__(v.Runner);r.args=argparse.Namespace(profile='smoke')
        r.build=Path('/tmp/unused-build');r.records=[{}]
        with patch.object(r,'reference_config'),patch.object(r,'save'),patch.object(r,'execute_tests') as run:
            r.loom()
        self.assertEqual(run.call_count,2)
        for call in run.call_args_list:
            self.assertEqual(call.kwargs['harness'],['--show-output'])
    def test_deep_properties_keep_depth_in_an_explicit_release_build(self):
        import argparse
        for profile,cases,release in [('pr','128',False),('nightly','10000',True)]:
            r=object.__new__(v.Runner);r.args=argparse.Namespace(profile=profile,seed=42)
            with patch.object(r,'execute_tests') as run:
                r.properties()
            self.assertEqual(run.call_count,5)
            for call in run.call_args_list:
                self.assertEqual('--release' in call.kwargs['extra'],release)
                self.assertEqual(call.kwargs['env']['ABPS_PROPTEST_CASES'],cases)
            self.assertIn(['--test','adversarial'],[c.args[2] for c in run.call_args_list])
            self.assertIn(['--test','configuration'],[c.args[2] for c in run.call_args_list])
            self.assertIn(['--lib'],[c.args[2] for c in run.call_args_list])
    def test_explicit_property_bounds_include_raw_grant_histories(self):
        import argparse
        r=object.__new__(v.Runner)
        r.args=argparse.Namespace(profile='pr',seed=17,cases=1000,operations=256)
        with patch.object(r,'execute_tests') as run:
            r.properties()
        self.assertEqual(run.call_count,5)
        for call in run.call_args_list:
            self.assertEqual(call.kwargs['env']['ABPS_PROPTEST_CASES'],'1000')
            self.assertEqual(call.kwargs['env']['ABPS_TRACE_OPERATIONS'],'256')
        self.assertEqual(run.call_args_list[-1].args[3],'grant_histories_match_logical_slot_sets')
    def test_source_drift_prevents_campaign_completion(self):
        import argparse,json
        with tempfile.TemporaryDirectory() as directory:
            args=argparse.Namespace(output=Path(directory)/'out',build_root=None,suite='native',profile='smoke',timeout=5)
            with patch.object(v,'source_identity',side_effect=['initial','changed']):
                r=v.Runner(args)
                with self.assertRaises(v.VerificationError):r.complete()
            result=json.loads((r.out/'results.json').read_text())
            self.assertEqual(result['status'],'incomplete')
            self.assertEqual(result['final_source_sha256'],'changed')
    def test_registry_discovery_does_not_count_as_review_execution(self):
        data={'checks':[{'id':'sample','selector':'the_test'}],
              'review_cases':[{'id':'ABPS-001','checks':['sample']}]}
        records=[{'status':'passed','selected_tests':['the_test']}]
        self.assertEqual(v.review_evidence(data,records)[0]['observed_checks'],[])
        records.append({'status':'failed','executed_tests':['the_test']})
        self.assertEqual(v.review_evidence(data,records)[0]['observed_checks'],[])
        records.append({'status':'passed','executed_tests':['the_test']})
        self.assertEqual(v.review_evidence(data,records)[0]['observed_checks'],[]) # legacy has no build identity
    def test_directed_mutants_require_the_named_assertion_failure(self):
        spec=importlib.util.spec_from_file_location('rights_mutations',Path(__file__).with_name('check-rights-mutations.py'))
        m=importlib.util.module_from_spec(spec);spec.loader.exec_module(m)
        failed='test target ... FAILED\ntest result: FAILED. 0 passed; 1 failed;'
        self.assertEqual(m.classify(101,failed,'target'),'killed')
        self.assertEqual(m.classify(101,failed,'different'),'incomplete')
        self.assertEqual(m.classify(101,'could not compile\n'+failed,'target'),'build-failure')
        self.assertEqual(m.classify(101,failed,'target',True),'timeout')
        self.assertEqual(m.classify(0,'test result: ok. 1 passed;','target'),'surviving-requires-review')
        self.assertEqual(m.classify(-6,'SIGABRT','target'),'incomplete')
    def test_miri_undefined_behavior_kills_only_inside_the_named_test(self):
        spec=importlib.util.spec_from_file_location('mutation_support',Path(__file__).with_name('mutation_support.py'))
        m=importlib.util.module_from_spec(spec);spec.loader.exec_module(m)
        ub='running 1 test\ntest target ... error: Undefined Behavior: deallocating while item is strongly protected'
        self.assertEqual(m.classify(101,ub,'target'),'killed')
        self.assertEqual(m.classify(101,ub,'different'),'incomplete')
        self.assertEqual(m.classify(0,ub,'target'),'incomplete')
        self.assertEqual(m.classify(101,'could not compile\n'+ub,'target'),'build-failure')
        self.assertEqual(m.classify(101,ub,'target',True),'timeout')
        self.assertEqual(m.classify(101,'test target ... error: unsupported operation','target'),'incomplete')
    def test_adapter_and_adversarial_targets_cannot_be_omitted_from_sanitizers(self):
        self.assertIn((["--example","spike_probe"],"spike_support::network::worker_fanout::tests::"),v.SANITIZER_GROUPS)
        self.assertIn((["--test","adversarial"],""),v.SANITIZER_GROUPS)

    def test_adapter_sources_are_part_of_build_identity(self):
        with tempfile.TemporaryDirectory() as directory, patch.object(v,'ROOT',Path(directory)):
            p=Path(directory)/'examples/adapter.rs';p.parent.mkdir();p.write_text('before')
            before=v.source_identity();p.write_text('after');self.assertNotEqual(before,v.source_identity())

    def test_review_matrix_cannot_drop_scenarios_or_detectors(self):
        import json,copy
        data=json.loads(v.MATRIX.read_text());v.validate_registry(data)
        for mutation in [lambda d:d['review_cases'].pop(),
                         lambda d:d['review_cases'][0].update(checks=['missing-selector']),
                         lambda d:d['review_cases'][0].update(negative_controls=[]),
                         lambda d:d['review_cases'][0].update(backends={'berry':'applicable'})]:
            damaged=copy.deepcopy(data);mutation(damaged)
            with self.assertRaises(v.VerificationError):v.validate_registry(damaged)
    def test_tsan_report_cannot_pass_with_zero_exit_code(self):
        for report in ['WARNING: ThreadSanitizer: data race',
                       'SUMMARY: ThreadSanitizer: data race in free',
                       'ThreadSanitizer: reported 2 warnings']:
            self.assertEqual(v.classify(0, report+'\ntest result: ok. 3 passed;', tests=True), 'failed')

    def test_tsan_limitation_text_is_not_a_race_report(self):
        self.assertEqual(v.classify(0, 'TSan does not support fences\ntest result: ok. 1 passed;', tests=True), 'passed')

    def test_reference_configuration_requires_one_actual_sample(self):
        sample='ABPS_REFERENCE_ACQUIRE=acquire-load ABPS_TARGET_SANITIZERS=thread\n'
        self.assertEqual(v.reference_configuration(sample), ('acquire-load','thread'))
        self.assertIsNone(v.reference_configuration('test result: ok. 1 passed;'))
        self.assertIsNone(v.reference_configuration(sample+sample))

    def test_build_script_is_part_of_source_identity(self):
        with tempfile.TemporaryDirectory() as directory, patch.object(v, 'ROOT', Path(directory)):
            build=Path(directory)/'build.rs'
            build.write_text('fn main() {}')
            before=v.source_identity()
            build.write_text('fn main() { println!("cargo::rustc-cfg=abps_tsan"); }')
            self.assertNotEqual(before,v.source_identity())

    def test_loom_registry_requires_both_reference_variants(self):
        row={'id':'x','selector':'x','features':'loom-model','tools':['loom'],'invariants':['OWN-03']}
        data={'checks':[row],'intentional_retention':[]}
        with self.assertRaises(v.VerificationError):v.validate_registry(data)
        row['reference_acquire']=['fence','acquire-load']
        v.validate_registry(data)

    def test_empty_selection_is_not_success(self):
        self.assertEqual(v.classify(0,'test result: ok. 0 passed;',tests=True),'incomplete')
        self.assertEqual(v.classify(0,'',tests=True),'incomplete')
    def test_passing_test_and_expected_failure_are_distinct(self):
        self.assertEqual(v.classify(0,'test result: ok. 2 passed;',tests=True),'passed')
        self.assertEqual(v.classify(101,'test result: FAILED. 0 passed; 1 failed;',tests=True),'failed')
    def test_timeout_and_branch_limit_are_incomplete(self):
        self.assertEqual(v.classify(-9,'',timeout=True),'incomplete')
        self.assertEqual(v.classify(101,'Model exceeded maximum number of branches'),'incomplete')
    def test_compile_or_disk_error_cannot_kill_a_mutant(self):
        for s in ['could not compile abps','No space left on device']:
            self.assertEqual(v.classify(101,s),'incomplete')
    def test_selector_parser_does_not_count_summary(self):
        self.assertEqual(v.test_names('a::b: test\n\n1 test, 0 benchmarks\n'),['a::b'])
    def test_multiple_binary_counts(self):
        self.assertEqual(v.classify(0,'test result: ok. 0 passed;\ntest result: ok. 1 passed;',tests=True),'passed')
    def test_mutations_require_completed_positive_detection(self):
        self.assertFalse(v.mutation_verified({}))
        ok={'end_time':'recorded','total_mutants':2,'caught':2}
        self.assertTrue(v.mutation_verified(ok))
        self.assertFalse(v.mutation_verified(ok|{'end_time':None}))
        for kind in ['missed','timeout','unviable','success']:
            self.assertFalse(v.mutation_verified(ok|{kind:1,'caught':1}))
    def test_registry_rejects_mixed_native_loom_evidence(self):
        data={'checks':[{'id':'x','selector':'x','features':'loom-model','tools':['native'],'invariants':['OWN-01']}],'intentional_retention':[]}
        with self.assertRaises(v.VerificationError):v.validate_registry(data)
    def test_registry_rejects_unknown_invariant(self):
        data={'checks':[{'id':'x','selector':'x','features':'','tools':['native'],'invariants':['MADE-UP-99']}],'intentional_retention':[]}
        with self.assertRaises(v.VerificationError):v.validate_registry(data)
    def test_registry_rejects_duplicate_id(self):
        row={'id':'x','selector':'x','features':'','tools':['native'],'invariants':['OWN-01']}
        with self.assertRaises(v.VerificationError):v.validate_registry({'checks':[row,row],'intentional_retention':[]})
    def test_registry_cannot_be_emptied_to_pass(self):
        with self.assertRaises(v.VerificationError):v.validate_registry({'checks':[],'intentional_retention':[]})
    def test_one_successful_command_does_not_complete_a_campaign(self):
        import argparse,json,sys
        with tempfile.TemporaryDirectory() as directory:
            args=argparse.Namespace(output=Path(directory)/'out',build_root=None,suite='native',profile='smoke',timeout=5)
            r=v.Runner(args)
            r.run('one',[sys.executable,'-c','print("test result: ok. 1 passed;")'],tests=True)
            result=json.loads((r.out/'results.json').read_text())
            self.assertEqual(result['commands'][0]['status'],'passed')
            self.assertEqual(result['status'],'not_run')
    def test_timeout_stops_child_and_records_incomplete(self):
        import argparse,json,sys
        with tempfile.TemporaryDirectory() as directory:
            args=argparse.Namespace(output=Path(directory)/'out',build_root=None,suite='native',profile='smoke',timeout=1)
            r=v.Runner(args)
            with self.assertRaises(v.VerificationError):
                r.run('timeout',[sys.executable,'-c','import time;time.sleep(60)'])
            self.assertEqual(json.loads((r.out/'results.json').read_text())['status'],'incomplete')
    def test_timeout_kill_reaches_descendants(self):
        import subprocess,sys,time
        # The grandchild inherits the pipe, so EOF requires every holder to have exited.
        code=("import subprocess,sys,time;"
              "subprocess.Popen([sys.executable,'-c','import time;time.sleep(60)'],stdout=sys.stdout);"
              "print('ready',flush=True);time.sleep(60)")
        p=subprocess.Popen([sys.executable,'-c',code],stdout=subprocess.PIPE,start_new_session=True)
        with p.stdout:
            self.assertEqual(p.stdout.readline().strip(),b'ready')
            v.kill_process_tree(p);p.wait()
            started=time.monotonic();p.stdout.read()
            self.assertLess(time.monotonic()-started,30,'a descendant outlived the timeout kill')
    def test_directed_timeout_stops_the_run_and_is_never_a_kill(self):
        import sys
        import mutation_support as m
        child='import time;print("test target ... FAILED\\ntest result: FAILED. 0 passed; 1 failed;",flush=True);time.sleep(60)'
        with tempfile.TemporaryDirectory() as directory:
            code,output,timed_out=m.execute([sys.executable,'-c',child],directory,None,Path(directory)/'run.log',timeout=1)
            self.assertTrue(timed_out)
            self.assertIn('test target ... FAILED',output)
            self.assertEqual(m.classify(code,output,'target',timed_out),'timeout')
    def test_directed_variants_need_every_baseline_and_one_detecting_schedule(self):
        import os,sys
        import mutation_support as m
        # A schedule-dependent detector: after mutation only FAILING seeds fail;
        # BROKEN seeds fail even unmodified.
        child=("import os,pathlib,sys;s=os.environ['SEED'];"
               "bad=s in os.environ['BROKEN'] or ('new' in pathlib.Path('f.txt').read_text() "
               "and s in os.environ['FAILING']);"
               "print('test target ... '+('FAILED' if bad else 'ok'));"
               "print('test result: '+('FAILED. 0 passed; 1 failed;' if bad else 'ok. 1 passed;'));"
               "sys.exit(101 if bad else 0)")
        def case(failing,broken='',variants=[{'SEED':s} for s in '123']):
            directory=tempfile.mkdtemp();self.addCleanup(__import__('shutil').rmtree,directory)
            source=Path(directory)/'source';source.mkdir();(source/'f.txt').write_text('old')
            env=dict(os.environ,FAILING=failing,BROKEN=broken,SEED='')
            r=m.run_case('case',source,'f.txt',[('old','new')],[sys.executable,'-c',child],'target',env,variants)
            return r,sorted(p.name for p in Path(directory).glob('*.log')),(source/'f.txt').read_text()
        r,logs,text=case('2')
        self.assertEqual((r['status'],r['caught'],r['baseline_passed'],r['decided_by']),('killed',True,True,1))
        self.assertEqual(logs,['baseline-0.log','baseline-1.log','baseline-2.log','run-0.log','run-1.log'])
        r,logs,text=case('')
        self.assertEqual((r['status'],r['caught'],r['decided_by']),('surviving-requires-review',False,2))
        # A failing baseline under any variant is incomplete and mutates nothing.
        r,logs,text=case('123',broken='3')
        self.assertEqual((r['status'],r['caught'],r['baseline_passed'],text),('baseline-incomplete',False,False,'old'))
        self.assertEqual(logs,['baseline-0.log','baseline-1.log','baseline-2.log'])
        r,logs,text=case('1',variants=[{'SEED':'1'}])
        self.assertEqual((r['status'],logs),('killed',['baseline.log','run.log']))
        self.assertNotIn('variants',r)
    def test_directed_targets_are_short_distinct_and_exclusive(self):
        import mutation_support as m
        spec=importlib.util.spec_from_file_location('rights_mutations',Path(__file__).with_name('check-rights-mutations.py'))
        rights=importlib.util.module_from_spec(spec);spec.loader.exec_module(rights)
        names=[case[0] for case in rights.cases()]
        with tempfile.TemporaryDirectory() as directory:
            output=Path(directory).resolve()
            targets=[m.create_case_target(output,name) for name in names]
            self.assertEqual(len(set(targets)),len(names))
            for target in targets:
                self.assertEqual((target.parent,len(str(target))),(output/'target',len(str(output))+20))
            with self.assertRaises(FileExistsError):m.create_case_target(output,names[0])
    def test_missing_executable_records_unsupported(self):
        import argparse,json
        with tempfile.TemporaryDirectory() as directory:
            args=argparse.Namespace(output=Path(directory)/'out',build_root=None,suite='native',profile='smoke',timeout=1)
            r=v.Runner(args)
            with self.assertRaises(v.VerificationError):r.run('missing',['/nonexistent/abps-tool'])
            self.assertEqual(json.loads((r.out/'results.json').read_text())['status'],'unsupported')
class EvidenceRegressions(unittest.TestCase):
    def test_inventory_mismatch_cannot_be_skipped(self):
        import sys
        spec=importlib.util.spec_from_file_location('inventory',Path(__file__).with_name('verification_inventory.py'))
        inventory=importlib.util.module_from_spec(spec);spec.loader.exec_module(inventory)
        with patch.object(sys,'argv',['inventory']),patch.object(inventory,'sites',return_value=[]):
            with self.assertRaises(RuntimeError):inventory.main()
    def test_execution_must_match_discovery_and_explicit_empty_summary(self):
        import argparse
        variants=[('real: test\n','test imposter ... ok\ntest result: ok. 1 passed;',False,False),
                  ('real: test\n','test real ... ok\ntest result: ok. 2 passed;',False,False),
                  ('real: test\n','test real ... ok\ntest result: ok. 1 passed;',False,True),
                  ('','',True,False),
                  ('','test result: ok. 0 passed;',False,False),
                  ('','test result: ok. 0 passed;',True,True),
                  ('source.rs - example (line 10): test\n',
                   'test source.rs - example (line 10) (compile fail) ... ok\ntest result: ok. 1 passed;',False,True),
                  ('source.rs - example (line 10): test\n',
                   'test source.rs - example (line 10) - compile fail ... ok\ntest result: ok. 1 passed;',False,True)]
        for listing,output,allow,success in variants:
            r=object.__new__(v.Runner);r.records=[];r.env={};r.args=argparse.Namespace()
            r.meta={'source_sha256':'s','toolchains':{'stable':'host: test-host\ncommit-hash: hash'}}
            def run(name,command,**kw):
                text=output if kw.get('tests') else listing
                record={'status':'passed','log':'log'}
                if kw.get('tests'):
                    import re
                    record.update(passed_executions=sum(map(int,re.findall(r'test result: ok\. (\d+) passed;',text))),
                                  executed_tests=re.findall(r'^test (.+?) \.\.\. ok$',text,re.M))
                r.records.append(record);return text
            with self.subTest(output=output,allow=allow),patch.object(r,'run',side_effect=run),patch.object(r,'save'):
                if success:
                    r.execute_tests('case',v.FULL,['--doc'],allow_empty=allow)
                    self.assertEqual(r.records[-1]['evidence_schema'],1)
                else:
                    with self.assertRaises(v.VerificationError):r.execute_tests('case',v.FULL,['--doc'],allow_empty=allow)
    def evidence(self, loom=False):
        features = 'local-branches-spike,loom-model' if loom else v.FULL
        row = {'id':'sample','selector':'target','features':features,'target':['--lib'],
               'tools':['loom' if loom else 'native']}
        if loom: row['reference_acquire']=['fence','acquire-load']
        data={'checks':[row],'review_cases':[{'id':'probe','checks':['sample']}]}
        record={'evidence_schema':1,'status':'passed','selected_tests':['module::target'],
                'executed_tests':['module::target'],'target_tests':['module::target'],'identity':{
                'source_sha256':'source','target':['--lib'],'features':v.feature_set(features),
                'tool':'loom' if loom else 'native','toolchain':'compiler','platform':'host',
                'reference_acquire':'fence','build_configuration':'test'}}
        return data,record

    def test_partial_group_cannot_satisfy_the_whole_selector(self):
        import copy
        data,first=self.evidence()
        first['target_tests']=['module::target','module::target_second']
        observed=lambda records:v.review_evidence(data,records,'source')[0]['observed_checks']
        self.assertEqual(observed([first]),[])
        second=copy.deepcopy(first)
        second['selected_tests']=second['executed_tests']=['module::target_second']
        self.assertEqual(observed([first,second]),['sample'])

    def test_evidence_requires_full_discovered_set_and_identity(self):
        import copy
        data,record=self.evidence()
        observed=lambda records:v.review_evidence(data,records,'source')[0]['observed_checks']
        self.assertEqual(observed([record]),['sample'])
        mutations=[lambda r:r.update(evidence_schema=0),lambda r:r.update(status='incomplete'),
                   lambda r:r.update(selected_tests=[]),lambda r:r.update(executed_tests=['module::target_extra']),
                   lambda r:r.update(selected_tests=['module::target','module::target_second'])]
        for key,value in [('source_sha256','other'),('target',['--test','issuer_traits']),
                          ('tool','miri'),('features',['default']),('reference_acquire','acquire-load'),
                          ('toolchain',''),('platform',''),('build_configuration','')]:
            mutations.append(lambda r,k=key,v=value:r['identity'].update({k:v}))
        for mutate in mutations:
            damaged=copy.deepcopy(record);mutate(damaged)
            self.assertEqual(observed([damaged]),[],damaged)

    def test_both_loom_variants_require_one_source_configuration(self):
        import copy
        data,fence=self.evidence(loom=True);load=copy.deepcopy(fence)
        load['identity']['reference_acquire']='acquire-load'
        observed=lambda records:v.review_evidence(data,records)[0]['observed_checks']
        self.assertEqual(observed([fence]),[])
        self.assertEqual(observed([fence,load]),['sample'])
        for key,value in [('source_sha256','different'),('toolchain','different'),
                          ('platform','different'),('build_configuration','different'),('features',['default','loom-model','berry-spike'])]:
            damaged=copy.deepcopy(load);damaged['identity'][key]=value
            self.assertEqual(observed([fence,damaged]),[])

    def test_failed_command_remains_failed_after_later_success(self):
        import argparse,sys
        with tempfile.TemporaryDirectory() as directory:
            r=v.Runner(argparse.Namespace(output=Path(directory)/'out',build_root=None,suite='native',profile='smoke',timeout=5))
            with self.assertRaises(v.VerificationError):r.run('bad',[sys.executable,'-c','raise SystemExit(1)'])
            r.run('good',[sys.executable,'-c','print("done")'])
            self.assertEqual(r.meta['status'],'failed')
            r.meta['required_steps']={'good':'passed'}
            with self.assertRaises(v.VerificationError):r.complete()

    def test_missing_required_step_prevents_completion_with_successful_commands(self):
        r=object.__new__(v.Runner);r.records=[{'status':'passed'}]
        r.meta={'status':'not_run','source_sha256':'same','required_steps':{'one':'passed','two':'not_run'}}
        with patch.object(v,'source_identity',return_value='same'),patch.object(r,'save'):
            with self.assertRaises(v.VerificationError):r.complete()
            r.meta['required_steps']['two']='passed';r.meta['status']='not_run'
            r.complete();self.assertEqual(r.meta['status'],'passed')

    def test_directed_failure_preserves_control_logs(self):
        with tempfile.TemporaryDirectory() as directory:
            r=object.__new__(v.Runner);r.out=Path(directory)/'out';r.out.mkdir()
            scratch=Path(directory)/'scratch';scratch.mkdir()
            (scratch/'results.json').write_text('[{"status":"timeout"}]')
            (scratch/'run.log').write_text('interrupted detector')
            with patch.object(r,'run',side_effect=v.VerificationError('timeout')):
                with self.assertRaises(v.VerificationError):r.directed_mutation('control',scratch)
            self.assertEqual((r.out/'control/run.log').read_text(),'interrupted detector')

    def test_directed_evidence_keeps_case_logs_without_case_targets(self):
        import mutation_support as m
        with tempfile.TemporaryDirectory() as directory:
            r=object.__new__(v.Runner);r.out=Path(directory)/'out';r.out.mkdir()
            scratch=Path(directory)/'scratch';(scratch/'case').mkdir(parents=True)
            (scratch/'case/run.log').write_text('detector')
            (m.create_case_target(scratch,'case')/'artifact').write_text('build')
            with patch.object(r,'run',side_effect=v.VerificationError('timeout')):
                with self.assertRaises(v.VerificationError):r.directed_mutation('control',scratch)
            self.assertEqual((r.out/'control/case/run.log').read_text(),'detector')
            copied=[p.relative_to(r.out).as_posix() for p in r.out.rglob('*') if p.is_file()]
            self.assertEqual(copied,['control/case/run.log'])

    def test_report_and_failed_summary_override_success(self):
        for report in ['SUMMARY: AddressSanitizer: heap-use-after-free',
                       'ERROR: LeakSanitizer: detected memory leaks',
                       'WARNING: MemorySanitizer: use-of-uninitialized-value',
                       'test result: FAILED. 0 passed; 1 failed;']:
            with self.subTest(report=report):
                self.assertEqual(v.classify(0, report+'\ntest result: ok. 1 passed;', tests=True), 'failed')

    def test_complete_cannot_erase_empty_failed_or_incomplete_campaign(self):
        for state,records in [('not_run', []), ('failed', [{'status':'failed'}]),
                              ('incomplete', [{'status':'incomplete'}])]:
            r=object.__new__(v.Runner)
            r.records=records
            r.meta={'status':state,'source_sha256':'same','required_steps':{'required':'not_run'}}
            with self.subTest(state=state),patch.object(v,'source_identity',return_value='same'),patch.object(r,'save'):
                with self.assertRaises(v.VerificationError):r.complete()
                self.assertNotEqual(r.meta['status'],'passed')

    def test_wrong_binary_cannot_supply_an_execution(self):
        data={'checks':[{'id':'traits','selector':'boundaries','target':['--test','issuer_traits'],
                         'features':v.FULL,'tools':['native']}],
              'review_cases':[{'id':'ABPS-030','checks':['traits']}]}
        records=[{'status':'passed','executed_tests':['mosaic::raw::tests::word_boundaries'],
                  'command':['cargo','test','--lib','--features',v.FULL]}]
        self.assertEqual(v.review_evidence(data,records)[0]['observed_checks'],[])

    def test_one_loom_variant_is_not_two(self):
        data={'checks':[{'id':'race','selector':'race','features':'loom-model','target':['--lib'],
                         'tools':['loom'],'reference_acquire':['fence','acquire-load']}],
              'review_cases':[{'id':'ABPS-025','checks':['race']}]}
        records=[{'status':'passed','executed_tests':['race'],'reference_acquire':'fence'}]
        self.assertEqual(v.review_evidence(data,records)[0]['observed_checks'],[])

    def test_optimized_python_still_rejects_empty_registry(self):
        import subprocess,sys
        code="import verify; verify.validate_registry({'checks':[], 'intentional_retention':[]})"
        for flags in [[],['-O']]:
            result=subprocess.run([sys.executable,'-B',*flags,'-c',code],cwd=Path(__file__).parent,
                                  capture_output=True,text=True)
            self.assertNotEqual(result.returncode,0,result.stdout)

if __name__=='__main__':unittest.main()
