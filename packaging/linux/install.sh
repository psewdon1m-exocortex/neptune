#!/usr/bin/env sh
set -eu

if [ "$(id -u)" -ne 0 ]; then
  echo "Run Neptune installation as root." >&2
  exit 1
fi

if command -v flock >/dev/null 2>&1; then
  lock_dir=/run/lock
  [ "${EXOCORTEX_PREPARED_HOST:-false}" != true ] || lock_dir=/run/exocortex
  exec 9>"$lock_dir/neptune-install.lock"
  flock -x 9
fi

if ! getent group neptune >/dev/null 2>&1; then
  groupadd --system neptune
fi
if ! getent group neptune-clients >/dev/null 2>&1; then
  groupadd --system neptune-clients
fi
if ! getent group updater >/dev/null 2>&1; then
  groupadd --system updater
fi
if ! id neptune >/dev/null 2>&1; then
  useradd --system --gid neptune --home /var/lib/neptune --shell /usr/sbin/nologin neptune
fi
[ "${EXOCORTEX_PREPARED_HOST:-false}" = true ] || usermod -a -G neptune,neptune-clients,updater neptune
install -d -m 0755 /usr/local/lib/neptune
install -m 0755 ./neptuned /usr/local/lib/neptune/neptuned
install -d -o root -g neptune -m 2750 /etc/neptune
install -d -o root -g neptune -m 2750 /etc/neptune/clients /etc/neptune/projects
install -d -o neptune -g neptune -m 0700 /var/lib/neptune
if [ -f /etc/neptune/projects.json ] && [ ! -f /var/lib/neptune/projects.json ]; then
  install -o neptune -g neptune -m 0600 /etc/neptune/projects.json /var/lib/neptune/projects.json
fi
if [ ! -f /etc/neptune/appsettings.json ]; then
  install -o root -g neptune -m 0640 ./appsettings.json /etc/neptune/appsettings.json
fi
if [ ! -s /etc/neptune/updater-agent.token ]; then
  umask 0077
  od -An -N32 -tx1 /dev/urandom | tr -d ' \n' > /etc/neptune/updater-agent.token
fi
chown root:updater /etc/neptune/updater-agent.token
chmod 0640 /etc/neptune/updater-agent.token
if [ "${EXOCORTEX_PREPARED_HOST:-false}" = true ]; then
  install -m 0644 ./neptune.service /etc/exocortex/units/neptune.service
  install -m 0755 ./neptunectl /usr/local/lib/neptune/neptunectl
else
  install -m 0644 ./neptune.service /etc/systemd/system/neptune.service
  install -m 0755 ./neptunectl /usr/local/sbin/neptunectl
fi

case "${NEPTUNE_KERNEL_URL:-}" in ""|https://*) ;; *) echo "NEPTUNE_KERNEL_URL must use HTTPS." >&2; exit 2 ;; esac
{
  printf 'Neptune__RegistryPath=/var/lib/neptune/projects.json\n'
  [ -z "${NEPTUNE_KERNEL_URL:-}" ] || printf 'Neptune__KernelOrigin=%s\n' "$NEPTUNE_KERNEL_URL"
} > /etc/neptune/neptune.env.tmp
chown root:neptune /etc/neptune/neptune.env.tmp
chmod 0640 /etc/neptune/neptune.env.tmp
mv -f /etc/neptune/neptune.env.tmp /etc/neptune/neptune.env
if [ -n "${NEPTUNE_KERNEL_TOKEN_FILE:-}" ] && [ "$NEPTUNE_KERNEL_TOKEN_FILE" != "/etc/neptune/kernel.token" ]; then
  echo "Neptune expects the host Kernel token at /etc/neptune/kernel.token." >&2
  exit 2
fi
if [ -f /etc/neptune/kernel.token ]; then
  chown root:neptune /etc/neptune/kernel.token
  chmod 0640 /etc/neptune/kernel.token
fi
systemctl daemon-reload
systemctl enable --now neptune.service
systemctl restart neptune.service
printf '%s\n' "Neptune installed. Run 'sudo neptunectl doctor' to verify the host."
