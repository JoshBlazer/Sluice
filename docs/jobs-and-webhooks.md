# Jobs and webhooks

## Jobs

A job is a **webhook**: an HTTP request (`GET`, `POST`, `PUT`, `PATCH` or `DELETE`) with optional headers and a JSON body. Any status below 400 counts as success.

```json
{
  "type": "webhook",
  "payload": {
    "url": "https://api.example.com/hooks/order",
    "method": "POST",
    "headers": {"X-Source": "sluice"},
    "body": {"order": 1234},
    "timeout_seconds": 25
  },
  "priority": 5,
  "max_retries": 3,
  "backoff_seconds": 30,
  "idempotency_key": "order-1234",
  "run_at": "2030-01-15T10:00:00Z"
}
```

| Field | Default | Meaning |
|-------|---------|---------|
| `payload.url` | required | Absolute `http`/`https` URL. Private and internal addresses are refused (see [Security](#security)) |
| `payload.timeout_seconds` | `25` | Bounds each attempt, 1–900 |
| `priority` | `5` | `1` (highest) to `10` (lowest), in three lanes: `1` = high, `2`–`5` = normal, `6`–`10` = low. A high-priority job from any tenant runs before every normal one; within a lane, tenants are served in proportion to their weight |
| `max_retries` | `3` | Retries after the first attempt, so `3` means up to 4 executions. `0` never retries |
| `backoff_seconds` | `30` | First retry delay; doubles each retry, capped at an hour, with ±20% jitter |
| `idempotency_key` | none | Submitting the same key again returns the original job instead of creating another |
| `run_at` | now | Run at a future time instead of immediately |

Jobs that exhaust their retries move to the **dead letter**, where they can be inspected and replayed (`POST /v1/jobs/{id}/replay`, the dashboard, or `sluice-cli replay`). `POST /v1/jobs/{id}/cancel` cancels a job that hasn't started or is waiting to retry.

### Recurring jobs

`POST /v1/schedules` registers a cron schedule (five fields, in the schedule's `timezone`) with a job template. `PATCH /v1/schedules/{id}` pauses (`"enabled": false`), resumes or edits it. Resuming, or changing the cron or timezone, recomputes the next run from now, so a schedule doesn't fire a burst of occurrences it missed while paused. Each occurrence fires at most once, even across scheduler failovers.

The full API is in the [OpenAPI spec](../internal/api/openapi.yaml), also served by the API at `/openapi.yaml`.

## Delivery guarantees

- **At-least-once.** A job runs at least once and, if its worker dies mid-request, may run again. Every request carries `webhook-id` (the job ID, identical across retries), so receivers can deduplicate.
- **Durable acceptance.** The API responds only after the job is committed to Postgres. Redis only holds queue order; anything lost from Redis is rebuilt from Postgres.
- **Recovery.** A crashed worker's jobs are picked up again within about 20 seconds; a stalled worker that comes back can't overwrite the result of a job that was reassigned.

## Verifying webhooks

Every request is signed with the tenant's secret using [Standard Webhooks](https://www.standardwebhooks.com/):

| Header | Value |
|--------|-------|
| `webhook-id` | The job ID; the same on every retry |
| `webhook-timestamp` | Unix seconds when the request was sent |
| `webhook-signature` | `v1,` + base64 HMAC-SHA256 of `{id}.{timestamp}.{body}` |

A job's own headers can't override these. Each tenant's signing secret (`whsec_…`) is shown by `sluice-cli create-tenant` and returned by `GET /v1/webhook-secret`. Verify requests with any [Standard Webhooks library](https://github.com/standard-webhooks/standard-webhooks/tree/main/libraries), for example in Node:

```js
import { Webhook } from "standardwebhooks";

const wh = new Webhook(process.env.SLUICE_WEBHOOK_SECRET);
// Throws if the signature is invalid or the timestamp is too old.
const payload = wh.verify(rawBody, request.headers);
```

`sluice-cli rotate-webhook-secret <tenant-id>` issues a new secret. Workers switch to it within 5 seconds (or immediately after `SIGHUP`), so accept both secrets briefly while rotating.

## Security

- **SSRF protection.** Webhooks refuse loopback, private, link-local (for example cloud metadata at `169.254.169.254`) and other non-public addresses. The check runs on the resolved IP when connecting, so it also covers redirects and DNS rebinding.
- **Validated input.** Payloads, URLs, methods, priorities, retry limits, timeouts, cron templates and request sizes are checked at the API boundary.
- **Tenant isolation.** Every API, stats and dashboard view is scoped to the caller's tenant; another tenant's job or schedule answers 404.
