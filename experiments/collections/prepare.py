#!/usr/bin/env python3
"""Freeze both engine variants without touching the repository manifest/lock."""
import hashlib
import json
import os
import re
from pathlib import Path
import shutil
import sys

GENERATED_DIRECTORIES = {"target", "__pycache__"}


def ignore_generated(directory, names):
    return {name for name in names
            if name in GENERATED_DIRECTORIES and (Path(directory) / name).is_dir()}


def source_files(folder):
    # Prune before descending: vendor build trees can dwarf the actual source.
    for directory, directories, files in os.walk(folder):
        directories[:] = sorted(set(directories) - GENERATED_DIRECTORIES)
        for name in sorted(files):
            path = Path(directory) / name
            if path.is_file():
                yield path


root = Path(__file__).resolve().parents[2]
destination = Path(sys.argv[1]).resolve()
destination.mkdir(parents=True, exist_ok=False)
# Current source has a local redb dependency. Historical revisions without it
# remain reproducible; freeze the vendor tree whenever it is present.
vendor = ("vendor",) if (root / "vendor").is_dir() else ()
for backend in ("im", "imbl"):
    target = destination / backend
    target.mkdir()
    for name in ("src", "tests", "examples", "benches", "docs", "sdk") + vendor:
        shutil.copytree(root / name, target / name, ignore=ignore_generated)
    for name in ("Cargo.toml", "Cargo.lock", "README.md"):
        shutil.copy2(root / name, target / name)
    manifest = (target / "Cargo.toml").read_text()
    dependency = 'im = "=15.1.0"' if backend == "im" else 'im = { package = "imbl", version = "=7.0.2" }'
    manifest, replaced = re.subn(r'^im\s*=.*$', dependency, manifest, flags=re.MULTILINE)
    assert replaced == 1, "expected exactly one persistent collection dependency"
    (target / "Cargo.toml").write_text(manifest)

hashes = {str(p.relative_to(root)): hashlib.sha256(p.read_bytes()).hexdigest()
          for folder in ("src", "tests", "examples", "benches", "sdk") + vendor
          for p in source_files(root / folder)}
hashes.update({name:hashlib.sha256((root/name).read_bytes()).hexdigest() for name in ("Cargo.toml","Cargo.lock","README.md")})
(destination / "source-sha256.json").write_text(json.dumps(hashes, indent=2)+"\n")
print(destination)
