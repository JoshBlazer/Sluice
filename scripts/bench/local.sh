#!/usr/bin/env bash
# Rehearses run.sh on this machine. Starts Docker-in-Docker containers that stand
# in for the db, node and load hosts (each with its own Docker daemon and network),
# reachable over SSH. Its numbers mean nothing, since every "host" shares this
# machine's CPUs; use it to check the benchmark works before paying for machines.
#
#   scripts/bench/local.sh up [nodes]
#   SSH_OPTS="-F bench-local/ssh_config" JOBS=2000 SUBMITTERS=16 \
#     PG_SHARED_BUFFERS=128MB scripts/bench/run.sh bench-local/hosts.txt
#   scripts/bench/local.sh down
set -euo pipefail
cd "$(dirname "$0")/../.."
export MSYS_NO_PATHCONV=1 # Git Bash on Windows would rewrite container paths

NAME=sluice-bench-local
DIR=bench-local
SUBNET=172.30.99

case ${1:-} in
up)
  nodes=${2:-3}
  mkdir -p "$DIR"
  [ -f "$DIR/id_ed25519" ] || ssh-keygen -q -t ed25519 -N '' -f "$DIR/id_ed25519"
  docker build -q -t "$NAME" - > /dev/null <<'EOF'
FROM docker:27-dind
RUN apk upgrade --no-cache && apk add --no-cache openssh-server bash curl procps util-linux && ssh-keygen -A && \
    sed -i 's/^#\?PermitRootLogin.*/PermitRootLogin prohibit-password/' /etc/ssh/sshd_config && \
    sed -i 's/^root:[^:]*:/root:*:/' /etc/shadow
ENTRYPOINT ["sh", "-c", "/usr/sbin/sshd && exec dockerd-entrypoint.sh"]
EOF
  docker network create --subnet "$SUBNET.0/24" "$NAME" > /dev/null 2>&1 || true
  : > "$DIR/hosts.txt"
  : > "$DIR/ssh_config"
  add() { # role name ip port
    docker rm -f "$2" > /dev/null 2>&1 || true
    docker run -d --privileged --name "$2" --hostname "$2" --network "$NAME" --ip "$3" \
      -p "127.0.0.1:$4:22" "$NAME" > /dev/null
    docker exec -i "$2" sh -c 'mkdir -p /root/.ssh && cat > /root/.ssh/authorized_keys && chmod 700 /root/.ssh && chmod 600 /root/.ssh/authorized_keys' \
      < "$DIR/id_ed25519.pub"
    echo "$1 $2 $3" >> "$DIR/hosts.txt"
    printf 'Host %s\n  HostName 127.0.0.1\n  Port %s\n  User root\n  IdentityFile %s\n  StrictHostKeyChecking no\n  UserKnownHostsFile /dev/null\n  LogLevel ERROR\n' \
      "$2" "$4" "$PWD/$DIR/id_ed25519" >> "$DIR/ssh_config"
  }
  add db "$NAME-db" "$SUBNET.10" 2210
  for i in $(seq 1 "$nodes"); do add node "$NAME-node$i" "$SUBNET.$((10 + i))" "$((2210 + i))"; done
  add load "$NAME-load" "$SUBNET.30" 2230
  for h in $(awk '{print $2}' "$DIR/hosts.txt"); do
    for _ in $(seq 1 30); do docker exec "$h" docker info > /dev/null 2>&1 && break; sleep 1; done
  done
  echo "ready: $DIR/hosts.txt"
  ;;
down)
  docker ps -aq --filter "name=$NAME" | xargs -r docker rm -f -v > /dev/null
  docker network rm "$NAME" > /dev/null 2>&1 || true
  rm -rf "$DIR"
  ;;
*) echo "usage: $0 up [nodes] | down" >&2; exit 2 ;;
esac
