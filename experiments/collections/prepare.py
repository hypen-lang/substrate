#!/usr/bin/env python3
"""Freeze both engine variants without touching the repository manifest/lock."""
import hashlib
import json
from pathlib import Path
import shutil
import sys

root = Path(__file__).resolve().parents[2]
destination = Path(sys.argv[1]).resolve()
destination.mkdir(parents=True, exist_ok=False)
for backend in ("im", "imbl"):
    target = destination / backend
    target.mkdir()
    for name in ("src", "tests", "examples", "benches", "docs", "sdk"):
        shutil.copytree(root / name, target / name)
    for name in ("Cargo.toml", "Cargo.lock", "README.md"):
        shutil.copy2(root / name, target / name)
    if backend == "imbl":
        manifest = (target / "Cargo.toml").read_text()
        assert 'im = "15.1.0"' in manifest
        (target / "Cargo.toml").write_text(manifest.replace('im = "15.1.0"', 'im = { package = "imbl", version = "=7.0.2" }'))
hashes = {str(p.relative_to(root)): hashlib.sha256(p.read_bytes()).hexdigest()
          for folder in ("src", "tests", "examples", "benches", "sdk")
          for p in sorted((root / folder).rglob("*")) if p.is_file()}
hashes.update({name:hashlib.sha256((root/name).read_bytes()).hexdigest() for name in ("Cargo.toml","Cargo.lock","README.md")})
(destination / "source-sha256.json").write_text(json.dumps(hashes, indent=2)+"\n")
print(destination)
