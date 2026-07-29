# Releasing PeerLink to PyPI

After validation and version bump, run from the repo root (with `pyproject.toml`).

## Prerequisites
```bash
python -m pip install --upgrade build twine
```

## Build
```powershell
# Windows PowerShell
Remove-Item -Recurse -Force dist, build -ErrorAction SilentlyContinue
Get-ChildItem -Recurse -Filter "*.egg-info" | Remove-Item -Recurse -Force
python -m build
twine check dist/*
```

```bash
# Unix
rm -rf dist/ build/ *.egg-info/
python -m build
twine check dist/*
```

## Upload (API token)
Configure `~/.pypirc` or use env:
```bash
twine upload dist/*
```

## Verify
```bash
pip index versions peerlink
# or
pip install peerlink==1.1.0
python -c "import peerlink; print(peerlink.__version__)"
```

## Git (when repo is initialized)
```bash
git add .
git commit -m "Release v1.1.0: SEQUENCE channels, bounded queues, native async"
git tag v1.1.0
git push origin main
git push origin v1.1.0
```
