#!/usr/bin/env sh
set -eu

if [ "$(id -u)" -ne 0 ]; then
  echo "Run Neptune installation as root." >&2
  exit 1
fi

if ! getent group neptune >/dev/null 2>&1; then
  groupadd --system neptune
fi
if ! getent group neptune-clients >/dev/null 2>&1; then
  groupadd --system neptune-clients
fi
if ! id neptune >/dev/null 2>&1; then
  useradd --system --gid neptune --home /var/lib/neptune --shell /usr/sbin/nologin neptune
fi
usermod -a -G neptune,neptune-clients neptune
install -d -m 0755 /usr/local/lib/neptune
install -m 0755 ./neptuned /usr/local/lib/neptune/neptuned
install -d -o root -g neptune -m 2750 /etc/neptune
if [ ! -f /etc/neptune/appsettings.json ]; then
  install -o root -g neptune -m 0640 ./appsettings.json /etc/neptune/appsettings.json
fi
install -m 0644 ./neptune.service /etc/systemd/system/neptune.service
systemctl daemon-reload
systemctl enable --now neptune.service
