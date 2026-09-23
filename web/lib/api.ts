const BASE = process.env.NEXT_PUBLIC_API_URL ?? "http://localhost:8080";
export const API_TOKEN = process.env.NEXT_PUBLIC_API_TOKEN ?? "dev-token";

async function apiFetch<T>(path: string, options?: RequestInit): Promise<T> {
  const res = await fetch(`${BASE}${path}`, {
    headers: {
      Authorization: `Bearer ${API_TOKEN}`,
      "Content-Type": "application/json",
    },
    cache: "no-store",
    ...options,
  });
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

export const replayJob = (jobId: string) =>
  apiFetch<unknown>(`/v1/jobs/${jobId}/replay`, { method: "POST" });
