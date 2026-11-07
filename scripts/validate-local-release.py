"""Validate and inventory a clean CI Windows build before GitHub publication."""
import hashlib
import json
import os
from pathlib import Path
import subprocess
import zipfile


def git(*args):
    return subprocess.check_output(['git', *args], text=True).strip()


def digest(path):
    with path.open('rb') as stream:
        return hashlib.file_digest(stream, 'sha256').hexdigest()


def main():
    assets = Path('release-assets')
    logs = Path('validation')
    sha = git('rev-parse', 'HEAD')
    assert sha == os.environ['GITHUB_SHA'], 'Build commit differs from workflow commit'
    base = git('rev-parse', os.environ['UPSTREAM_SHA'] + '^{commit}')
    subprocess.run(['git', 'merge-base', '--is-ancestor', base, sha], check=True)
    version = json.loads(Path('package.json').read_text())['version']
    assert version == '4.0.27'
    assert json.loads(Path('src-tauri/tauri.conf.json').read_text())['version'] == version
    for name in ['cc-switch.exe', 'CC-Switch-4.0.27-x64-setup.exe']:
        path = assets / name
        with path.open('rb') as stream:
            assert stream.read(2) == b'MZ', f'{name} is not a Windows executable'
        assert path.stat().st_size > 1_000_000, f'{name} is unexpectedly small'
    ui = json.loads((logs / 'embedded-ui.json').read_text(encoding='utf-8-sig'))
    payload = json.loads((logs / 'installer-verification.json').read_text(encoding='utf-8-sig'))
    payload_ui = json.loads((logs / 'payload-embedded-ui.json').read_text(encoding='utf-8-sig'))
    comparison = json.loads((logs / 'payload-comparison.json').read_text(encoding='utf-8-sig'))
    assert ui['passed'] and ui['production_protocol'] and payload['passed']
    assert payload_ui['passed'] and payload_ui['production_protocol']
    assert payload['payload_ui_verified'] and payload['published_payload_identical']
    assert comparison['passed'] and comparison['normalized_equal']
    assert comparison['comparison'] == 'exact-first-tauri-bundle-marker-substitution'
    assert comparison['normalized_payload_sha256'] == comparison['build_executable_sha256']
    assert comparison['build_executable_sha256'] == payload['build_executable_sha256']
    assert comparison['payload_executable_sha256'] == payload['payload_executable_sha256']
    assert payload['payload_executable_sha256'] == payload['executable_sha256']
    assert payload['executable_sha256'] == digest(assets / 'cc-switch.exe')
    assert payload['installer_sha256'] == digest(assets / 'CC-Switch-4.0.27-x64-setup.exe')
    with zipfile.ZipFile(assets / 'source.zip') as archive:
        assert archive.testzip() is None
        assert json.loads(archive.read('package.json'))['version'] == version
        assert archive.read('src-tauri/src/main.rs') == subprocess.check_output(['git', 'show', 'HEAD:src-tauri/src/main.rs'])
    assert (assets / 'local-patches-vs-official.patch').stat().st_size > 0
    # Keep zero-byte successful logs in a ZIP, since Releases reject empty assets.
    with zipfile.ZipFile(assets / 'validation.zip', 'w', zipfile.ZIP_DEFLATED) as archive:
        for path in sorted(logs.rglob('*')):
            if path.is_file() and path.name != 'package-validation.log':
                archive.write(path, path.relative_to(logs))
    with zipfile.ZipFile(assets / 'validation.zip') as archive:
        assert archive.testzip() is None
    url = f"https://github.com/{os.environ['GITHUB_REPOSITORY']}/actions/runs/{os.environ['GITHUB_RUN_ID']}"
    notes = f'''CC Switch {version} local integration for Windows x64.

Source commit: {sha}
Official baseline: {os.environ['UPSTREAM_TAG']} ({base})
Build and test run: {url}

Frontend formatting, typecheck, unit tests and production build passed. Windows backend formatting, all-target Clippy with warnings denied, nextest and doctests passed. Existing ignored tests remain ignored; consult validation.zip for complete output. Both the build executable and extracted NSIS payload independently verified their embedded production pages and assets. Their complete bytes match after exactly one documented Tauri bundle marker substitution (UNK to NSS); any other difference fails validation. The published cc-switch.exe is the verified installer payload and matches it byte-for-byte. Raw and normalized hashes are recorded in validation.zip.

Database schema remains 24 when upgrading from 4.0.25. Upgrading from 4.0.17 still changes schema 20 to 24. Before first launch, fully exit CC Switch and separately back up its configuration directory and database (including any WAL/SHM files), plus relevant Codex configuration/history. The built-in pre-migration backup can fail without blocking migration. Downgrading to 4.0.17 requires restoring the pre-upgrade backup, not merely replacing the executable. Do not upload private backups.

Exit CC Switch, then install CC-Switch-{version}-x64-setup.exe. This is an unsigned fork build; Windows may show a publisher warning. Signing and automatic-updater artifacts are not included. Existing official updater endpoints and public key are unchanged. Installation and live-client/GUI acceptance have not been performed by this CI build.

Retained compatibility changes against the official release are in local-patches-vs-official.patch; full source is source.zip. SHA256SUMS.txt covers all other release assets. No user profiles, databases or authentication files are included.
'''
    (assets / 'RELEASE-NOTES.md').write_text(notes, encoding='utf-8', newline='\n')
    manifest = {
        'version': version, 'source_commit': sha, 'upstream_commit': base,
        'release_tag': os.environ['RELEASE_TAG'], 'build_run': url,
        'platform': 'windows-x86_64', 'unsigned': True,
        'embedded_ui_verified': True, 'installer_payload_verified': True,
        'payload_embedded_ui_verified': True, 'published_payload_identical': True,
        'payload_comparison': comparison,
        'gui_acceptance_performed': False,
        'files': [{'name': p.name, 'size': p.stat().st_size, 'sha256': digest(p)}
                  for p in sorted(assets.iterdir()) if p.is_file() and p.name not in ('manifest.json', 'SHA256SUMS.txt')],
    }
    (assets / 'manifest.json').write_text(json.dumps(manifest, indent=2) + '\n', encoding='utf-8', newline='\n')
    (assets / 'SHA256SUMS.txt').write_text(''.join(
        f'{digest(p)}  {p.name}\n' for p in sorted(assets.iterdir())
        if p.is_file() and p.name != 'SHA256SUMS.txt'), encoding='utf-8', newline='\n')
    print(json.dumps({'passed': True, 'source_commit': sha, 'asset_count': len(list(assets.iterdir()))}))


if __name__ == '__main__':
    main()
