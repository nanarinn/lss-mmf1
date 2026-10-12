"""Atomic, timestamped and repeatable snapshot files."""
from datetime import datetime, timezone
import hashlib
import json
import os
from pathlib import Path
import tempfile


def write_capture(directory, records, metadata):
    body = json.dumps({'records': records, 'metadata': metadata},
                      ensure_ascii=False, sort_keys=True, indent=2).encode('utf-8')
    stamp = datetime.fromisoformat(metadata['completed_at_utc']).astimezone(timezone.utc).strftime('%Y%m%dT%H%M%S%fZ')
    name = 'MMF_' + stamp + '_' + hashlib.sha256(body).hexdigest()[:16] + '.json'
    folder = Path(directory)
    folder.mkdir(parents=True, exist_ok=True)
    target = folder / name
    if target.exists():
        return target
    temporary = None
    try:
        with tempfile.NamedTemporaryFile(dir=folder, suffix='.tmp', delete=False) as handle:
            temporary = Path(handle.name)
            handle.write(body)
            handle.flush()
            os.fsync(handle.fileno())
        os.replace(temporary, target)
        return target
    finally:
        if temporary is not None:
            temporary.unlink(missing_ok=True)
