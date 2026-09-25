import { getToken, UNAUTHORIZED_EVENT } from "./auth";
import { API_BASE } from "./config";

async function apiFetch<T>(path: string, options?: RequestInit): Promise<T> {
  const res = await fetch(`${API_BASE}${path}`, {
    headers: {
      Authorization: `Bearer ${getToken() ?? ""}`,
      "Content-Type": "application/json",
    },
    cache: "no-store",
    ...options,
  });
  if (res.status === 401) window.dispatchEvent(new Event(UNAUTHORIZED_EVENT));
  if (!res.ok) throw new Error(`${path} → ${res.status}`);
  return res.json();
}

export type QueueDepth = {
  tenant_id: string;
  tenant: string;
  priority: "high" | "normal" | "low";
  depth: number;
};

export type StatsResponse = {
  queues: QueueDepth[];
  jobs_by_state: Record<string, number>;
  timestamp?: number;
};

export type JobRun = {
  run_id: string;
  job_id: string;
  tenant_id: string;
  type: string;
  attempt: number;
  state: string;
  duration_ms: number | null;
  started_at: string;
  finished_at: string | null;
  error?: string;
};

export type DeadLetterEntry = {
  job_id: string;
  tenant_id: string;
  attempt_count: number;
  final_error?: string;
  moved_at: string;
};

export const fetchStats = () => apiFetch<StatsResponse>("/v1/stats");

export const fetchRuns = (limit = 50) =>
  apiFetch<{ runs: JobRun[]; count: number }>(`/v1/stats/runs?limit=${limit}`);

export const fetchDeadLetter = (limit = 50) =>
  apiFetch<{ entries: DeadLetterEntry[]; count: number }>(
    `/v1/stats/dead-letter?limit=${limit}`
  );

export type Job = {
  id: string;
  tenant_id: string;
  type: string;
  priority: number;
  state: string;
  run_at: string;
  attempt: number;
  max_retries: number;
  backoff_seconds: number;
  last_error?: string;
  created_at: string;
  completed_at?: string;
};

export const fetchRetriedJobs = (state = "", limit = 100) =>
  apiFetch<{ jobs: Job[] | null; count: number }>(
    `/v1/jobs?retried=true&limit=${limit}${state ? `&state=${state}` : ""}`
  );

export const fetchJobRuns = (jobId: string) =>
  apiFetch<{ runs: JobRun[]; count: number }>(`/v1/jobs/${jobId}/runs`);

export const replayJob = (jobId: string) =>
  apiFetch<unknown>(`/v1/jobs/${jobId}/replay`, { method: "POST" });
