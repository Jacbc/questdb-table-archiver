#!/usr/bin/env bash
#
# mount-share.sh: mount an SMB/CIFS network share at boot, for a backup directory on a NAS.
#
#   sudo bash mount-share.sh //HOST/SHARE MOUNT_POINT --user NAME    (asks for the password)
#   sudo bash mount-share.sh //HOST/SHARE MOUNT_POINT --guest        (a share without a login)
#   add --print to only show what would be written
#
#   e.g. sudo bash mount-share.sh //nas.local/backups /mnt/backups --user backup
#
# 1. installs cifs-utils if mount.cifs is missing (apt/dnf)
# 2. with --user: stores the login in /etc/cifs-credentials/<name>, readable by root only
# 3. adds one line to /etc/fstab (a copy of the old file is kept next to it); a line for the same mount
#    point is replaced. The line mounts on first use (x-systemd.automount) and has nofail, so the machine still
#    boots when the NAS is down, and files belong to the user who ran sudo.
# 4. mounts it and checks that it can write there

set -euo pipefail

usage() { sed -n '3,17p' "$0" | sed 's/^# \{0,1\}//'; }
die() { echo "mount-share: $*" >&2; exit 1; }

SHARE="${1:-}"; MNT="${2:-}"
[[ "$SHARE" == //?*/?* && "$MNT" == /?* ]] || { usage >&2; exit 2; }
shift 2
LOGIN=""; GUEST=""; PRINT=""
while [[ $# -gt 0 ]]; do
  case "$1" in
    --user) LOGIN="${2:-}"; [[ -n "$LOGIN" ]] || die "--user needs a name"; shift 2 ;;
    --guest) GUEST=1; shift ;;
    --print) PRINT=1; shift ;;
    *) usage >&2; exit 2 ;;
  esac
done
[[ -n "$LOGIN" || -n "$GUEST" ]] || die "give --user NAME or --guest"
[[ -z "$LOGIN" || -z "$GUEST" ]] || die "give --user NAME or --guest, not both"
[[ "$SHARE" != *[[:space:]]* && "$MNT" != *[[:space:]]* ]] || die "spaces in the share or mount point are not supported"
MNT="${MNT%/}"

OWNER_UID="${SUDO_UID:-$(id -u)}"; OWNER_GID="${SUDO_GID:-$(id -g)}"
SAFE="$(echo "${SHARE#//}" | tr -c 'A-Za-z0-9._-' '_')"
CRED="/etc/cifs-credentials/${SAFE%_}"
if [[ -n "$GUEST" ]]; then AUTH="guest"; else AUTH="credentials=$CRED"; fi
OPTS="$AUTH,vers=3,uid=$OWNER_UID,gid=$OWNER_GID,file_mode=0640,dir_mode=0750,_netdev,nofail,x-systemd.automount,x-systemd.mount-timeout=30"
LINE="$SHARE $MNT cifs $OPTS 0 0"

if [[ -n "$PRINT" ]]; then
  echo "/etc/fstab line:"; echo "  $LINE"
  [[ -n "$LOGIN" ]] && echo "credentials file: $CRED (username=$LOGIN, password=..., mode 600, root only)"
  exit 0
fi
[[ "$(id -u)" == 0 ]] || die "run it with sudo"

# ---- 1. cifs-utils --------------------------------------------------------------------------------------------------
if ! command -v mount.cifs >/dev/null && [[ ! -x /sbin/mount.cifs && ! -x /usr/sbin/mount.cifs ]]; then
  echo "installing cifs-utils"
  if command -v apt-get >/dev/null; then apt-get install -y cifs-utils
  elif command -v dnf >/dev/null; then dnf install -y cifs-utils
  else die "install cifs-utils yourself, then run this again"; fi
fi

# ---- 2. login -------------------------------------------------------------------------------------------------------
if [[ -n "$LOGIN" ]]; then
  [[ -r /dev/tty ]] || die "needs a terminal to ask for the password"
  read -r -s -p "Password for $LOGIN on $SHARE: " PW </dev/tty; echo
  read -r -s -p "Again: " PW2 </dev/tty; echo
  [[ "$PW" == "$PW2" ]] || die "the passwords differ"
  install -d -m 0700 /etc/cifs-credentials
  ( umask 077; printf 'username=%s\npassword=%s\n' "$LOGIN" "$PW" > "$CRED" )
  unset PW PW2
  echo "login stored in $CRED (root only)"
fi

# ---- 3. fstab -------------------------------------------------------------------------------------------------------
BAK="/etc/fstab.before-mount-share-$(date +%Y%m%d-%H%M%S)"
cp -p /etc/fstab "$BAK"
TMP="$(mktemp)"
awk -v mnt="$MNT" '!($0 !~ /^[[:space:]]*#/ && $2 == mnt)' /etc/fstab > "$TMP"
echo "$LINE" >> "$TMP"
cat "$TMP" > /etc/fstab; rm -f "$TMP"
echo "fstab: added $SHARE at $MNT (old file kept as $BAK)"

# ---- 4. mount and test ----------------------------------------------------------------------------------------------
mkdir -p "$MNT"
if mountpoint -q "$MNT"; then umount "$MNT" || die "$MNT is in use (cd out of it, stop a running backup) and run this again"; fi
command -v systemctl >/dev/null || die "systemd is needed for the mount-on-first-use line"
systemctl daemon-reload
systemctl restart "$(systemd-escape -p --suffix=automount "$MNT")" || true
ls "$MNT" >/dev/null 2>&1 || true   # first use: systemd mounts it now
sleep 1
if ! grep -qs " $MNT cifs " /proc/mounts; then
  AUTOMOUNT="$(systemd-escape -p --suffix=automount "$MNT")"
  systemctl stop "$AUTOMOUNT" 2>/dev/null || true
  cp -p "$BAK" /etc/fstab; systemctl daemon-reload
  journalctl -b --no-pager 2>/dev/null | grep -i 'cifs' | tail -3 >&2 || true
  die "$SHARE did not mount at $MNT (wrong share name, user or password?); /etc/fstab is back as it was"
fi
T="$MNT/.mount-share-test.$$"
sudo -u "#$OWNER_UID" sh -c 'echo ok > "$1"' _ "$T" && rm -f "$T" || die "mounted, but $(id -nu "$OWNER_UID") cannot write in $MNT"
echo "mounted $SHARE at $MNT; $(id -nu "$OWNER_UID") can write there, and it is mounted again after a reboot"
