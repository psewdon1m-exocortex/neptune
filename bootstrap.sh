#!/bin/sh
set -eu
umask 077

helper='__HOST_HELPER__'
version='__HOST_HELPER_VERSION__'
public_key='__HOST_HELPER_PUBLIC_KEY_BASE64__'
updater_version='__UPDATER_VERSION__'
updater_bootstrap_sha='__UPDATER_BOOTSTRAP_SHA256__'
repository="psewdon1m-exocortex/$helper"
base="https://github.com/$repository/releases/download/$helper-v$version"
fail() { printf '%s\n' "$helper bootstrap: $*" >&2; exit 1; }
[ "$(id -u)" -eq 0 ] || fail 'run as root'
for command in curl openssl python3 sha256sum; do command -v "$command" >/dev/null 2>&1 || fail "$command is required"; done
work=$(mktemp -d)
trap 'rm -rf "$work"' EXIT HUP INT TERM
printf '%s' "$public_key" | openssl base64 -d -A >"$work/$helper.pem" || fail 'embedded release key is invalid'
manifest="$helper-linux-release-linux-x64.json"
curl -fsSL --proto '=https' --proto-redir '=https' --connect-timeout 10 --max-time 60 --max-filesize 1048576 \
  "https://api.github.com/repos/$repository/releases/tags/$helper-v$version" -o "$work/published.json"
curl -fsSL --proto '=https' --proto-redir '=https' --connect-timeout 10 --max-time 90 --max-filesize 2097152 "$base/$manifest" -o "$work/$manifest"
curl -fsSL --proto '=https' --proto-redir '=https' --connect-timeout 10 --max-time 90 --max-filesize 16384 "$base/$manifest.sig.json" -o "$work/$manifest.sig.json"
artifact=$(python3 - "$work" "$helper" "$version" <<'PY'
import base64, hashlib, json, pathlib, re, subprocess, sys
root, helper, version = pathlib.Path(sys.argv[1]), sys.argv[2], sys.argv[3]
name = helper + '-linux-release-linux-x64.json'
published = json.loads((root/'published.json').read_bytes())
assert published.get('tag_name') == helper + '-v' + version and published.get('draft') is False and published.get('prerelease') is False
body = root/name
envelope = json.loads((root/(name+'.sig.json')).read_bytes())
manifest = json.loads(body.read_bytes())
key = root/(helper+'.pem')
assert envelope.get('schema') == 'exocortex.release-signature.v1' and envelope.get('algorithm') == 'RSA-PSS-SHA256'
der = subprocess.check_output(['openssl','pkey','-pubin','-in',str(key),'-outform','DER'])
assert hashlib.sha256(der).hexdigest() == envelope.get('key_id')
details = subprocess.check_output(['openssl','pkey','-pubin','-in',str(key),'-text','-noout'], text=True)
assert re.search(r'Public-Key: \((?:3072|4096|6144|8192) bit\)', details)
signature = root/'signature'; signature.write_bytes(base64.b64decode(envelope['signature'], validate=True))
subprocess.run(['openssl','dgst','-sha256','-verify',str(key),'-signature',str(signature),'-sigopt','rsa_padding_mode:pss',
                '-sigopt','rsa_pss_saltlen:32',str(body)], check=True, stdout=subprocess.DEVNULL)
assert manifest.get('schema') == 'exocortex.' + helper + '.release.v1' and manifest.get('product') == helper + '-linux'
assert manifest.get('version') == version and manifest.get('runtime') == 'linux-x64'
assert re.fullmatch(r'[A-Za-z0-9_.-]+\.tar\.gz', manifest.get('artifact',''))
assert re.fullmatch(r'[a-f0-9]{64}', manifest.get('sha256',''))
print(manifest['artifact'])
PY
) || fail 'signed helper release is invalid'
curl -fsSL --proto '=https' --proto-redir '=https' --connect-timeout 10 --max-time 300 --max-filesize 268435456 "$base/$artifact" -o "$work/$artifact"
expected=$(python3 - "$work/$manifest" <<'PY'
import json,sys
print(json.load(open(sys.argv[1]))['sha256'])
PY
)
printf '%s  %s\n' "$expected" "$work/$artifact" | sha256sum -c - >/dev/null || fail 'helper archive digest mismatch'
if ! command -v updater >/dev/null 2>&1 || ! updater host capabilities >/dev/null 2>&1; then
  updater_base="https://github.com/psewdon1m-exocortex/updater/releases/download/updater-v$updater_version"
  curl -fsSL --proto '=https' --proto-redir '=https' --connect-timeout 10 --max-time 120 --max-filesize 1048576 "$updater_base/bootstrap.sh" -o "$work/updater-bootstrap.sh"
  printf '%s  %s\n' "$updater_bootstrap_sha" "$work/updater-bootstrap.sh" | sha256sum -c - >/dev/null || fail 'pinned Updater bootstrap digest mismatch'
  sh "$work/updater-bootstrap.sh"
fi
updater host capabilities >/dev/null 2>&1 || fail 'Updater host-dependency protocol is unavailable; update Updater first'
updater "$helper" install --bundle "$work"
updater host seed-source "$helper" "https://github.com/$repository"
printf '%s\n' "$helper is installed. Configure external integrations later in sudo updater tui."
