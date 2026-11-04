"""Compare an unsigned Tauri build with its NSIS payload, failing closed.

Tauri CLI 2.10.1 patches the FIRST UNK marker to NSS, then restores the
unpatched build binary. Reproduce precisely that one change, not a global
replacement or a broad exclusion of differing bytes.
https://github.com/tauri-apps/tauri/blob/tauri-cli-v2.10.1/crates/tauri-bundler/src/bundle.rs
"""
import hashlib
import json
from pathlib import Path
import sys

UNKNOWN = b'__TAURI_BUNDLE_TYPE_VAR_UNK'
NSIS = b'__TAURI_BUNDLE_TYPE_VAR_NSS'


def compare_payload(build: bytes, payload: bytes) -> dict:
    offset = build.find(UNKNOWN)
    if offset < 0:
        raise ValueError('Build binary is missing the Tauri UNK bundle marker')
    if len(build) != len(payload):
        raise ValueError('Installer payload and build binary sizes differ')
    expected = build[:offset] + NSIS + build[offset + len(UNKNOWN):]
    if payload != expected:
        raise ValueError('Installer payload differs beyond the first UNK-to-NSS marker substitution')
    # Normalize only the identified marker for an independently recorded digest.
    normalized = payload[:offset] + UNKNOWN + payload[offset + len(NSIS):]
    if normalized != build:
        raise ValueError('Normalized installer payload is not byte-for-byte identical')
    sha256 = lambda data: hashlib.sha256(data).hexdigest()
    return {
        'passed': True,
        'comparison': 'exact-first-tauri-bundle-marker-substitution',
        'marker_offset': offset,
        'original_marker': UNKNOWN.decode(),
        'installer_marker': NSIS.decode(),
        'build_executable_sha256': sha256(build),
        'payload_executable_sha256': sha256(payload),
        'normalized_payload_sha256': sha256(normalized),
        'normalized_equal': True,
        'size': len(build),
    }


if __name__ == '__main__':
    result = compare_payload(Path(sys.argv[1]).read_bytes(), Path(sys.argv[2]).read_bytes())
    Path(sys.argv[3]).write_text(json.dumps(result, indent=2) + '\n', encoding='utf-8')
    print(json.dumps(result))
