#!/usr/bin/env python3
"""Restore and verify the exact qualified worktree bytes before optional package 3 CI."""
import argparse
import hashlib
import json
from pathlib import Path, PurePosixPath
import subprocess


def digest(data):
    return hashlib.sha256(data).hexdigest()


def main():
    parser = argparse.ArgumentParser()
    parser.add_argument('manifest', type=Path)
    parser.add_argument('--report', type=Path, required=True)
    parser.add_argument('--include-untracked', action='store_true')
    parser.add_argument('--check-only', action='store_true')
    args = parser.parse_args()
    root = Path(__file__).resolve().parents[1]
    manifest_path = args.manifest.resolve()
    if not manifest_path.is_relative_to(root):
        raise ValueError('Source manifest must be within this checkout')
    data = manifest_path.read_bytes()
    if len(data) > 2 * 1024 * 1024:
        raise ValueError('Oversized source manifest')
    manifest = json.loads(data)
    if manifest['schema'] != 'exocortex.package3.frozen-source.v1':
        raise ValueError('Unexpected source manifest schema')
    if not 1 <= len(manifest['files']) <= 10000:
        raise ValueError('Invalid source file count')
    excluded = {manifest_path.relative_to(root).as_posix()}
    command = ['git', 'ls-files', '--cached', '-z']
    if args.include_untracked:
        command += ['--others', '--exclude-standard']
    tracked = {p.decode('utf-8') for p in subprocess.check_output(command, cwd=root).split(b'\0') if p}
    expected = {entry['path'] for entry in manifest['files']}
    if len(expected) != len(manifest['files']) or tracked - excluded != expected:
        raise ValueError('Frozen file set differs from this checkout')
    prepared = []
    for entry in manifest['files']:
        relative = PurePosixPath(entry['path'])
        if relative.is_absolute() or '..' in relative.parts or relative.parts[0] == '.git':
            raise ValueError('Unsafe source path')
        target = root.joinpath(*relative.parts)
        if target.is_symlink() or not target.resolve().is_relative_to(root):
            raise ValueError('Source path escapes checkout')
        current = target.read_bytes()
        mode = entry['line_endings']
        canonical = current.replace(b'\r\n', b'\n') if mode != 'BINARY' else current
        if digest(canonical) != entry['canonical_sha256']:
            raise ValueError('Qualified source content differs: ' + entry['path'])
        if mode == 'CRLF':
            restored = canonical.replace(b'\n', b'\r\n')
        elif mode == 'MIXED':
            indices = set(entry['crlf_indices'])
            pieces = canonical.split(b'\n')
            restored = b''.join(piece + ((b'\r\n' if i in indices else b'\n') if i < len(pieces) - 1 else b'') for i, piece in enumerate(pieces))
        elif mode in ('LF', 'BINARY'):
            restored = canonical
        else:
            raise ValueError('Unexpected line-ending mode')
        if len(restored) != entry['bytes'] or digest(restored) != entry['sha256']:
            raise ValueError('Original qualified bytes could not be restored: ' + entry['path'])
        if args.check_only and current != restored:
            raise ValueError('Check-only requires exact original bytes: ' + entry['path'])
        prepared.append((target, restored, current != restored))
    changed = 0
    for target, restored, differs in prepared:
        if differs:
            target.write_bytes(restored)
            changed += 1
    for entry in manifest['files']:
        if digest(root.joinpath(*PurePosixPath(entry['path']).parts).read_bytes()) != entry['sha256']:
            raise ValueError('Post-restore source bytes differ: ' + entry['path'])
    report = {'status': 'PASS_SCOPED_FROZEN_QUALIFIED_WORKTREE_BYTE_EXACT',
              'manifest_sha256': digest(data), 'files': len(prepared), 'restored_files': changed,
              'native_product_test_invocations': 0, 'publisher_signatures': 0}
    with args.report.open('x', encoding='utf-8') as output:
        json.dump(report, output, indent=2)
        output.write('\n')
    print(json.dumps(report))


if __name__ == '__main__':
    main()
