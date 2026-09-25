#!/usr/bin/env bash
# Installs the Helm chart into the current kube context (e.g. a kind cluster)
# against the throwaway dependencies in deploy/kind/deps.yaml, then proves the
# deployment works end to end: migrations ran, every role is ready, and a job
# submitted through the API is executed by a worker.
#
# usage: IMAGE_REPO=sluice IMAGE_TAG=ci scripts/k8s-smoke.sh
set -euo pipefail

IMAGE_REPO=${IMAGE_REPO:-sluice}
IMAGE_TAG=${IMAGE_TAG:-ci}

kubectl apply -f deploy/kind/deps.yaml
kubectl rollout status deploy/postgres deploy/redis deploy/etcd --timeout=180s

helm install sluice deploy/helm --wait --timeout 5m \
  --set image.repository="$IMAGE_REPO" --set image.tag="$IMAGE_TAG" --set image.pullPolicy=Never \
  --set postgres.url="postgres://sluice:sluice@postgres:5432/sluice?sslmode=disable" \
  --set redis.addr="redis://:sluice@redis:6379/0" \
  --set etcd.endpoints="etcd:2379" \
  --set api.replicas=1 --set scheduler.replicas=2 --set worker.replicas=1 \
  --set worker.autoscaling.enabled=false

echo "== pods"
kubectl get pods -o wide

KEY=$(kubectl exec deploy/sluice-api -- /sluice-cli create-tenant -rate-limit 0 smoke | awk '/api key/{print $3}')
test -n "$KEY"

kubectl port-forward svc/sluice-api 18080:80 >/dev/null 2>&1 &
PF=$!
trap 'kill $PF 2>/dev/null || true' EXIT
for _ in $(seq 1 30); do curl -sf localhost:18080/readyz >/dev/null && break; sleep 1; done
echo "== readyz: $(curl -s localhost:18080/readyz)"

JOB=$(curl -sf -X POST localhost:18080/v1/jobs -H "Authorization: Bearer $KEY" \
  -d '{"type":"webhook","payload":{"url":"https://example.com/","method":"GET"}}' | sed -E 's/.*"id":"([^"]+)".*/\1/')
echo "== submitted job $JOB"

for _ in $(seq 1 60); do
  STATE=$(curl -sf "localhost:18080/v1/jobs/$JOB" -H "Authorization: Bearer $KEY" | sed -E 's/.*"state":"([^"]+)".*/\1/')
  echo "state: $STATE"
  [ "$STATE" = succeeded ] && { echo "SMOKE TEST PASSED"; exit 0; }
  [ "$STATE" = dead ] && break
  sleep 2
done
echo "SMOKE TEST FAILED"; kubectl logs -l app.kubernetes.io/component=worker --tail=50 || true
exit 1
