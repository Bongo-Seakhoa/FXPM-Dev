from __future__ import annotations

import glob
import json
import os
from typing import Any, Dict, Iterable, List


_JSONL_EXTENSIONS = {".jsonl", ".ndjson"}


def iter_matching_files(pm_root: str, patterns: Iterable[str]) -> List[str]:
    if not pm_root:
        return []

    seen: Dict[str, str] = {}
    for pattern in patterns:
        if not pattern:
            continue
        if os.path.isabs(pattern):
            candidates = [pattern] if os.path.exists(pattern) else []
        else:
            candidates = glob.glob(os.path.join(pm_root, pattern), recursive=True)
        for candidate in candidates:
            if not os.path.isfile(candidate):
                continue
            key = os.path.normcase(os.path.abspath(candidate))
            seen[key] = candidate
    return sorted(seen.values())


def load_records_from_text(path: str, text: str) -> List[Dict[str, Any]]:
    ext = os.path.splitext(path)[1].lower()
    if ext in _JSONL_EXTENSIONS:
        return _load_jsonl_records(text)
    if ext != ".json":
        return []
    try:
        payload = json.loads(text)
    except json.JSONDecodeError:
        return []
    return _expand_json_payload(payload)


def load_records_from_file(path: str) -> List[Dict[str, Any]]:
    try:
        with open(path, "r", encoding="utf-8", errors="ignore") as handle:
            return load_records_from_text(path, handle.read())
    except OSError:
        return []


def _load_jsonl_records(text: str) -> List[Dict[str, Any]]:
    records: List[Dict[str, Any]] = []
    for line in text.splitlines():
        stripped = line.strip()
        if not stripped or stripped.startswith("#"):
            continue
        try:
            payload = json.loads(stripped)
        except json.JSONDecodeError:
            continue
        records.extend(_expand_json_payload(payload))
    return records


def _expand_json_payload(payload: Any) -> List[Dict[str, Any]]:
    records: List[Dict[str, Any]] = []

    if isinstance(payload, list):
        for item in payload:
            records.extend(_expand_json_payload(item))
        return records

    if isinstance(payload, dict):
        for key in ("entries", "signals", "recommendations", "data"):
            value = payload.get(key)
            if isinstance(value, list):
                for item in value:
                    records.extend(_expand_json_payload(item))
                return records
        records.append(payload)

    return records
