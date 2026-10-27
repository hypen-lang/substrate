"""Freeze-contract tests using tiny source fixtures, without Cargo or benchmarks."""
import hashlib
import json
from pathlib import Path
import shutil
import subprocess
import sys
import tempfile
import unittest


class PrepareTests(unittest.TestCase):
    def test_generated_directories_are_neither_frozen_nor_hashed(self):
        with tempfile.TemporaryDirectory() as temporary:
            root = Path(temporary) / 'source'
            script = root / 'experiments/collections/prepare.py'
            script.parent.mkdir(parents=True)
            shutil.copyfile(Path(__file__).with_name('prepare.py'), script)
            for folder in ['src', 'tests', 'examples', 'benches', 'docs', 'sdk']:
                (root / folder).mkdir()
            (root / 'Cargo.toml').write_text('[dependencies]\nim = "=15.1.0"\n')
            (root / 'Cargo.lock').write_text('fixture lock')
            (root / 'README.md').write_text('fixture readme')
            generated = ['vendor/redb/target/debug/large-artifact',
                         'vendor/redb/tools/__pycache__/cached.pyc',
                         'src/nested/target/generated.rs',
                         'sdk/__pycache__/cached.pyc']
            retained = ['vendor/redb/src/lib.rs', 'src/kept/target',
                        'sdk/kept/__pycache__']
            for name in generated + retained:
                path = root / name
                path.parent.mkdir(parents=True, exist_ok=True)
                path.write_text(name)
            destination = Path(temporary) / 'prepared'
            subprocess.run([sys.executable, str(script), str(destination)], check=True,
                           stdout=subprocess.DEVNULL)
            hashes = json.loads((destination / 'source-sha256.json').read_text())
            for name in generated:
                self.assertNotIn(name, hashes)
                for backend in ['im', 'imbl']:
                    self.assertFalse((destination / backend / name).exists())
            for name in retained:
                self.assertEqual(hashes[name], hashlib.sha256(name.encode()).hexdigest())
                for backend in ['im', 'imbl']:
                    self.assertEqual((destination / backend / name).read_text(), name)

    def test_vendor_dependency_is_copied_and_hashed_for_both_variants(self):
        for vendored in [False, True]:
            with self.subTest(vendored=vendored), tempfile.TemporaryDirectory() as temporary:
                root = Path(temporary) / 'source'
                script = root / 'experiments/collections/prepare.py'
                script.parent.mkdir(parents=True)
                shutil.copyfile(Path(__file__).with_name('prepare.py'), script)
                for folder in ['src', 'tests', 'examples', 'benches', 'docs', 'sdk']:
                    (root / folder).mkdir()
                    (root / folder / 'fixture.txt').write_text(folder)
                (root / 'Cargo.toml').write_text('[dependencies]\nim = "=15.1.0"\n')
                (root / 'Cargo.lock').write_text('fixture lock')
                (root / 'README.md').write_text('fixture readme')
                if vendored:
                    path = root / 'vendor/redb/src/lib.rs'
                    path.parent.mkdir(parents=True)
                    path.write_text('vendored source')
                destination = Path(temporary) / 'prepared'
                subprocess.run([sys.executable, str(script), str(destination)], check=True,
                               stdout=subprocess.DEVNULL)
                hashes = json.loads((destination / 'source-sha256.json').read_text())
                for backend in ['im', 'imbl']:
                    copied = destination / backend / 'vendor/redb/src/lib.rs'
                    self.assertEqual(copied.exists(), vendored)
                    if vendored:
                        self.assertEqual(copied.read_text(), 'vendored source')
                        self.assertEqual(hashes['vendor/redb/src/lib.rs'],
                                         hashlib.sha256(copied.read_bytes()).hexdigest())
                self.assertEqual(any(key.startswith('vendor/') for key in hashes), vendored)


if __name__ == '__main__':
    unittest.main()
