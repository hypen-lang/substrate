"""Exercise the real hook in disposable repositories, including partial staging."""
import os
from pathlib import Path
import shutil
import subprocess
import tempfile
import unittest


ROOT = Path(__file__).resolve().parents[1]
SCRIPT = "tools/sanitize-benchmark-paths.py"
REPORT = "docs/benchmarks/report with spaces.json"
RAW = b'{"binary":"/Users/developer/Workspace/Substrate/target/run","time":12.5}\n'
CLEAN = b'{"binary":"<repo>/target/run","time":12.5}\n'


class HookTests(unittest.TestCase):
    def setUp(self):
        self.temp = tempfile.TemporaryDirectory()
        self.addCleanup(self.temp.cleanup)
        self.root = Path(self.temp.name)
        self.env = {k: v for k, v in os.environ.items() if not k.startswith("GIT_")}
        self.git("init", "-q")
        self.git("config", "user.name", "Hook Test")
        self.git("config", "user.email", "hook@example.invalid")
        self.git("config", "commit.gpgsign", "false")
        for name in (SCRIPT, ".githooks/pre-commit"):
            target = self.root / name
            target.parent.mkdir(parents=True, exist_ok=True)
            shutil.copy2(ROOT / name, target)
        self.git("config", "core.hooksPath", ".githooks")
        self.write(REPORT, RAW)

    def git(self, *args, env=None):
        return subprocess.check_output(["git", *args], cwd=self.root,
                                       env=env or self.env, stderr=subprocess.PIPE)

    def write(self, name, data):
        path = self.root / name
        path.parent.mkdir(parents=True, exist_ok=True)
        path.write_bytes(data)

    def run_script(self, *args, env=None):
        return subprocess.run(["python3", SCRIPT, *args], cwd=self.root,
                              env=env or self.env, capture_output=True)

    def test_real_commit_sanitizes_index_and_preserves_unstaged_edits(self):
        (self.root / REPORT).chmod(0o755)
        self.git("add", REPORT)
        unstaged = RAW.replace(b"12.5", b"99.5")
        self.write(REPORT, unstaged)
        self.git("commit", "-qm", "Sanitized report")
        self.assertEqual(self.git("show", "HEAD:" + REPORT), CLEAN)
        self.assertEqual((self.root / REPORT).read_bytes(), unstaged)
        self.assertTrue(self.git("ls-files", "--stage", REPORT).startswith(b"100755"))
        self.assertEqual(self.run_script("--staged", "--check").returncode, 0)

    def test_check_does_not_modify_index(self):
        self.git("add", REPORT)
        before = (self.root / ".git/index").read_bytes()
        self.assertEqual(self.run_script("--staged", "--check").returncode, 1)
        self.assertEqual((self.root / ".git/index").read_bytes(), before)
        self.assertEqual((self.root / REPORT).read_bytes(), RAW)

    def test_binary_symlink_deleted_and_outside_files_are_untouched(self):
        self.write("docs/benchmarks/deleted.json", RAW)
        self.git("add", "docs/benchmarks/deleted.json")
        self.git("commit", "-qm", "Initial report")
        (self.root / "docs/benchmarks/deleted.json").unlink()
        self.git("add", "-u", "docs/benchmarks/deleted.json")
        self.write("docs/benchmarks/binary.bin", b"\0" + RAW)
        self.write("docs/benchmarks/invalid.bin", b"\xff" + RAW)
        self.write("outside.json", RAW)
        (self.root / "docs/benchmarks/link").symlink_to("/home/developer/report")
        self.git("add", ".")
        self.assertEqual(self.run_script("--staged").returncode, 0)
        self.assertEqual(self.git("show", ":docs/benchmarks/binary.bin"), b"\0" + RAW)
        self.assertEqual(self.git("show", ":docs/benchmarks/invalid.bin"), b"\xff" + RAW)
        self.assertEqual(self.git("show", ":outside.json"), RAW)
        self.assertEqual(self.git("show", ":docs/benchmarks/link"), b"/home/developer/report")
        self.assertEqual(self.git("ls-files", "docs/benchmarks/deleted.json"), b"")

    def test_alternate_index_is_honored(self):
        self.git("add", REPORT)
        main_index = (self.root / ".git/index").read_bytes()
        alternate = self.root / "alternate-index"
        alternate.write_bytes(main_index)
        env = self.env | {"GIT_INDEX_FILE": str(alternate)}
        self.assertEqual(self.run_script("--staged", env=env).returncode, 0)
        self.assertEqual(self.git("show", ":" + REPORT, env=env), CLEAN)
        self.assertEqual((self.root / ".git/index").read_bytes(), main_index)
        self.assertEqual((self.root / REPORT).read_bytes(), RAW)


if __name__ == "__main__":
    unittest.main()
