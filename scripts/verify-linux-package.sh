#!/usr/bin/env bash
set -euo pipefail

sh -n packaging/linux/install.sh
sh -n packaging/linux/neptunectl
grep -Fx 'RuntimeDirectoryPreserve=yes' packaging/linux/neptune.service
grep -Fx 'RuntimeDirectory=neptune' packaging/linux/neptune.service

output="$(mktemp -d)"
trap 'rm -rf -- "$output"' EXIT
dotnet publish src/Neptune.Linux/Neptune.Linux.csproj -c Release -r linux-x64 \
  --self-contained true -p:PublishSingleFile=true -o "$output"
test -x "$output/Neptune.Linux"
test -s "$output/appsettings.json"
printf 'Linux daemon, package scripts and systemd runtime directory qualified.\n'
