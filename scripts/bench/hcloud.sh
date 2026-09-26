#!/usr/bin/env bash
# Creates (up) or deletes (down) benchmark machines on Hetzner Cloud and writes
# scripts/bench/hosts.txt for run.sh. Machines bill by the hour until `down`.
#
#   hcloud context create sluice-bench        # paste an API token for your project
#   SSH_KEY=<key name in the project> scripts/bench/hcloud.sh up
#   scripts/bench/run.sh scripts/bench/hosts.txt
#   scripts/bench/hcloud.sh down
#
# Everything is labelled app=sluice-bench, so `down` removes exactly what `up`
# made. The firewall admits only SSH from outside; the hosts reach each other
# over the private network, which Hetzner firewalls don't filter.
set -euo pipefail
cd "$(dirname "$0")"

NAME=sluice-bench
LOCATION=${LOCATION:-fsn1}
NETWORK_ZONE=${NETWORK_ZONE:-eu-central} # must contain LOCATION
NODES=${NODES:-3}
NODE_TYPE=${NODE_TYPE:-cpx31} # 4 vCPU / 8 GB: the README's target node
DB_TYPE=${DB_TYPE:-ccx33}     # 8 dedicated vCPU / 32 GB, so the database isn't what's measured
LOAD_TYPE=${LOAD_TYPE:-cpx41} # 8 vCPU / 16 GB, so the load generator isn't either
OS_IMAGE=${OS_IMAGE:-docker-ce} # Hetzner's Ubuntu image with Docker preinstalled
# Same as run.sh; add e.g. "-i ~/.ssh/other_key" to use a key other than your default.
read -r -a SSH_OPTS <<< "${SSH_OPTS:--o StrictHostKeyChecking=accept-new -o BatchMode=yes -o ConnectTimeout=10}"

case ${1:-} in
up)
  : "${SSH_KEY:?set SSH_KEY to the name of an SSH key uploaded to your Hetzner project}"
  hcloud network create --name "$NAME" --ip-range 10.0.0.0/16 --label app="$NAME" > /dev/null
  hcloud network add-subnet "$NAME" --network-zone "$NETWORK_ZONE" --type cloud --ip-range 10.0.0.0/24 > /dev/null
  rules=$(mktemp)
  echo '[{"direction":"in","protocol":"tcp","port":"22","source_ips":["0.0.0.0/0","::/0"]}]' > "$rules"
  hcloud firewall create --name "$NAME" --rules-file "$rules" --label app="$NAME" > /dev/null
  rm "$rules"

  servers=("db:$DB_TYPE" "load:$LOAD_TYPE")
  for i in $(seq 1 "$NODES"); do servers+=("node$i:$NODE_TYPE"); done
  for s in "${servers[@]}"; do
    hcloud server create --name "$NAME-${s%%:*}" --type "${s#*:}" --image "$OS_IMAGE" \
      --location "$LOCATION" --ssh-key "$SSH_KEY" --network "$NAME" --firewall "$NAME" \
      --label app="$NAME" > /dev/null &
  done
  wait

  : > hosts.txt
  for s in "${servers[@]}"; do
    name=$NAME-${s%%:*}
    role=${s%%:*}; role=${role%%[0-9]*}
    public=$(hcloud server ip "$name")
    private=$(hcloud server describe "$name" -o format='{{ (index .PrivateNet 0).IP }}')
    echo "$role root@$public $private" >> hosts.txt
    for _ in $(seq 1 60); do
      ssh "${SSH_OPTS[@]}" "root@$public" \
        'cloud-init status --wait > /dev/null 2>&1; command -v vmstat > /dev/null' && break
      sleep 5
    done
  done
  sort -o hosts.txt hosts.txt
  cat hosts.txt
  echo "Machines are billing now. Run scripts/bench/run.sh scripts/bench/hosts.txt, then scripts/bench/hcloud.sh down."
  ;;
down)
  for s in $(hcloud server list -l app="$NAME" -o noheader -o columns=name); do
    hcloud server delete "$s" > /dev/null &
  done
  wait
  hcloud firewall delete "$NAME" > /dev/null 2>&1 || true
  hcloud network delete "$NAME" > /dev/null 2>&1 || true
  rm -f hosts.txt
  echo "deleted"
  ;;
*) echo "usage: $0 up | down" >&2; exit 2 ;;
esac
