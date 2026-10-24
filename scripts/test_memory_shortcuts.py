import importlib.util
from pathlib import Path
from types import SimpleNamespace
import unittest
import tempfile
from unittest.mock import patch

spec=importlib.util.spec_from_file_location('shortcuts',Path(__file__).with_name('memory-shortcuts.py'))
m=importlib.util.module_from_spec(spec);spec.loader.exec_module(m)

class Shortcuts(unittest.TestCase):
    def test_build_rejects_source_and_binary_drift(self):
        with tempfile.TemporaryDirectory() as tmp:
            path = Path(tmp)/'probe'
            path.write_bytes(b'original')
            build = object.__new__(m.Build)
            build.record = dict(source_root=tmp, source_sha256='expected',
                binaries=dict(probe=dict(path=str(path), sha256=m.digest(path))))
            self.assertEqual(build.binary('probe'), path)
            path.write_bytes(b'changed')
            with self.assertRaises(Exception): build.binary('probe')
            with patch.object(m, 'module', return_value=SimpleNamespace(source_identity=lambda:'different')):
                with self.assertRaises(Exception): build.verify_source()

    def test_terminal_zero_is_not_zero_cpu(self):
        self.assertEqual(m.cpu_ticks([[1,100,12,31],[2,0,0,0]]),[12,31])
        with self.assertRaises(Exception):m.cpu_ticks([])

    def test_tcp_generator_is_always_baseline(self):
        def build(name,profile):
            return SimpleNamespace(path=Path('/records')/name,
                binary=lambda _:Path('/ram')/name/profile/'examples/spike_probe')
        baseline=build('base','release');server=build('optimized','perf-lto')
        opts=m.tcp_options(baseline,server,Path('/ram/out'),[0,2,4,6])
        self.assertEqual(opts.target,Path('/ram/base'))
        self.assertEqual(opts.server_binary,Path('/ram/optimized/perf-lto/examples/spike_probe'))
        self.assertEqual(opts.build_record,baseline.path)
        self.assertEqual(opts.server_build_record,server.path)

    def test_serial_rejects_lost_charge_and_unobserved_allocations(self):
        good=dict(drained=True,iterations=2,checksum=660,size=64,working_set=1,
            retained_idle_bytes=64,positive_control=True,allocations=0,deallocations=0,reallocations=0,ns_per_frame=None)
        m.validate_serial(good,True)
        for key,value in [('retained_idle_bytes',0),('checksum',0),('positive_control',False),('allocations',1),('ns_per_frame',1)]:
            with self.assertRaises(Exception):m.validate_serial(dict(good,**{key:value}),True)

    def test_empty_or_nonfinite_timings_are_rejected(self):
        data=dict(drained=True,iterations=1,checksum=330,size=64,working_set=1,retained_idle_bytes=64,ns_per_frame=1)
        m.validate_serial(data)
        for value in [0,float('nan'),float('inf')]:
            with self.assertRaises(Exception):m.validate_serial(dict(data,ns_per_frame=value))

    def test_policy_requires_real_drain(self):
        good=dict(drained=True,iterations=1,checksum=7,ns_per_frame=1,bank=dict(reserved_bytes=0,streams=[dict(admitted=0)]))
        m.validate_cycle(good)
        for bank in [dict(reserved_bytes=64,streams=[dict(admitted=0)]),dict(reserved_bytes=0,streams=[dict(admitted=64)])]:
            with self.assertRaises(Exception):m.validate_cycle(dict(good,bank=bank))

if __name__=='__main__':unittest.main()
