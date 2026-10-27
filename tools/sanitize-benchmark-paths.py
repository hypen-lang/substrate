#!/usr/bin/env python3
"""Remove personal paths from UTF-8 benchmark artifacts before publication."""
import argparse
from pathlib import Path
import re
import subprocess


ROOT = Path(__file__).resolve().parents[1]
HOME_PATH = re.compile(r"/(?:Users|home)/[^/\s\"'<>]+")
# Match previous checkout locations as well as the current machine's checkout.
CHECKOUT_PATH = re.compile(
    r"/(?:Users|home)/[^/\s\"'<>]+/(?:[^/\s\"'<>]+/)*Substrate(?=/|[\s\"'<>]|$)"
)


def sanitize(text):
    text = text.replace(str(ROOT) + "/", "<repo>/")
    text = CHECKOUT_PATH.sub("<repo>", text)
    return HOME_PATH.sub("<home>", text)


def sanitize_bytes(data):
    if b"\0" in data:
        return data
    try:
        return sanitize(data.decode("utf-8")).encode("utf-8")
    except UnicodeDecodeError:
        return data


def git(*args, **kwargs):
    return subprocess.check_output(["git", "-C", str(ROOT), *args], **kwargs)


def sanitize_staged(check=False):
    """Edit index blobs only, including when Git supplies an alternate index."""
    listing = git("ls-files", "--stage", "-z", "--", "docs/benchmarks/")
    updates = []
    with subprocess.Popen(["git", "-C", str(ROOT), "cat-file", "--batch"],
                          stdin=subprocess.PIPE, stdout=subprocess.PIPE) as blobs:
        try:
            for entry in listing.split(b"\0"):
                if not entry:
                    continue
                metadata, path = entry.split(b"\t", 1)
                mode, oid, stage = metadata.split()
                if stage != b"0":
                    raise RuntimeError("Resolve benchmark merge conflicts before sanitizing")
                if mode not in (b"100644", b"100755"):
                    continue  # Never rewrite symlink targets or submodules.
                blobs.stdin.write(oid + b"\n")
                blobs.stdin.flush()
                header = blobs.stdout.readline().split()
                if len(header) != 3 or header[1] != b"blob":
                    raise RuntimeError("Cannot read staged benchmark blob")
                data = blobs.stdout.read(int(header[2]))
                if blobs.stdout.read(1) != b"\n":
                    raise RuntimeError("Incomplete staged benchmark blob")
                cleaned = sanitize_bytes(data)
                if cleaned != data:
                    updates.append((mode, path, cleaned))
        finally:
            blobs.stdin.close()
    if blobs.returncode:
        raise RuntimeError("Failed to read staged benchmark contents")
    if updates and not check:
        if git("ls-files", "--stage", "-z", "--", "docs/benchmarks/") != listing:
            raise RuntimeError("Staged benchmarks changed during sanitization; retry")
        records = []
        for mode, path, cleaned in updates:
            oid = git("hash-object", "-w", "--stdin", input=cleaned).strip()
            records.append(mode + b" " + oid + b"\t" + path + b"\0")
        git("update-index", "-z", "--index-info", input=b"".join(records))
    for _, path, _ in updates:
        print(path.decode("utf-8", errors="backslashreplace"))
    print(f"{len(updates)} staged file(s) "
          f"{'need sanitizing' if check else 'sanitized'}")
    return int(check and bool(updates))


def main():
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("directory", nargs="?", type=Path)
    parser.add_argument("--staged", action="store_true",
                        help="sanitize staged benchmarks only; preserve working files")
    parser.add_argument("--check", action="store_true",
                        help="report unsanitized files without changing them")
    args = parser.parse_args()
    if args.staged:
        if args.directory is not None:
            parser.error("--staged cannot be combined with a directory")
        return sanitize_staged(args.check)
    args.directory = args.directory or ROOT / "docs/benchmarks"
    if not args.directory.is_dir() or args.directory.is_symlink():
        parser.error("directory must be an existing non-symlink directory")
    changed = 0
    for path in sorted(args.directory.rglob("*")):
        if not path.is_file() or path.is_symlink():
            continue
        data = path.read_bytes()
        cleaned = sanitize_bytes(data)
        if cleaned != data:
            changed += 1
            print(path.relative_to(args.directory))
            if not args.check:
                path.write_bytes(cleaned)
    print(f"{changed} file(s) {'need sanitizing' if args.check else 'sanitized'}")
    return int(args.check and changed > 0)


if __name__ == "__main__":
    raise SystemExit(main())
