"use client";
import { useQuery } from "@tanstack/react-query";
import { fetchJobRuns, fetchRetriedJobs, replayJob, type Job, type JobRun } from "@/lib/api";
import { Fragment, useEffect, useState } from "react";

const STATE_BADGE: Record<string, string> = {
  succeeded: "bg-emerald-950/60 text-emerald-400 border border-emerald-900/60",
  failed:    "bg-amber-950/60 text-amber-400 border border-amber-900/60",
  dead:      "bg-red-950/60 text-red-400 border border-red-900/60",
  running:   "bg-violet-950/60 text-violet-400 border border-violet-900/60",
  claimed:   "bg-purple-950/60 text-purple-400 border border-purple-900/60",
  pending:   "bg-blue-950/60 text-blue-400 border border-blue-900/60",
  cancelled: "bg-zinc-800/60 text-zinc-400 border border-zinc-700/60",
};

// A job in "failed" state is waiting out its backoff before the next attempt.
const STATE_LABEL: Record<string, string> = { failed: "retrying" };

const FILTERS = [
  { value: "", label: "All retried" },
  { value: "failed", label: "Retrying" },
  { value: "dead", label: "Dead" },
  { value: "succeeded", label: "Recovered" },
  { value: "cancelled", label: "Cancelled" },
];

function StateBadge({ state }: { state: string }) {
  return (
    <span className={`text-xs font-medium px-2 py-0.5 rounded-full ${STATE_BADGE[state] ?? "bg-zinc-800 text-zinc-400"}`}>
      {STATE_LABEL[state] ?? state}
    </span>
  );
}

function relative(dateStr: string): string {
  const diff = Math.round((new Date(dateStr).getTime() - Date.now()) / 1000);
  const abs = Math.abs(diff);
  const text =
    abs < 60 ? `${abs}s` : abs < 3600 ? `${Math.floor(abs / 60)}m` : abs < 86400 ? `${Math.floor(abs / 3600)}h` : `${Math.floor(abs / 86400)}d`;
  return diff >= 0 ? `in ${text}` : `${text} ago`;
}

function fmtDuration(ms: number | null): string {
  if (ms === null) return "—";
  if (ms < 1000) return `${ms}ms`;
  return `${(ms / 1000).toFixed(1)}s`;
}

// attempt counts failed attempts; the job's own current or successful attempt adds one.
function attemptsUsed(j: Job): number {
  return j.attempt + (["succeeded", "claimed", "running"].includes(j.state) ? 1 : 0);
}

function RunTimeline({ jobId }: { jobId: string }) {
  const { data, isLoading, error } = useQuery({
    queryKey: ["job-runs", jobId],
    queryFn: () => fetchJobRuns(jobId),
    refetchInterval: 5000,
  });

  if (isLoading) return <div className="text-xs text-zinc-500 py-2">Loading attempts…</div>;
  if (error) return <div className="text-xs text-red-400 py-2">Failed to load attempts</div>;
  if (!data || data.runs.length === 0) return <div className="text-xs text-zinc-500 py-2">No recorded attempts</div>;

  return (
    <ol className="relative border-l border-zinc-700 ml-2 space-y-3 py-1">
      {data.runs.map((r: JobRun) => (
        <li key={r.run_id} className="ml-4">
          <span
            className={`absolute -left-[5px] mt-1.5 h-2.5 w-2.5 rounded-full ${
              r.state === "succeeded" ? "bg-emerald-400" : r.state === "dead" ? "bg-red-400" : r.finished_at ? "bg-amber-400" : "bg-violet-400"
            }`}
          />
          <div className="flex flex-wrap items-center gap-x-3 gap-y-1 text-xs">
            <span className="font-semibold text-zinc-200">Attempt {r.attempt + 1}</span>
            <StateBadge state={r.state} />
            <span className="text-zinc-500" title={new Date(r.started_at).toLocaleString()}>
              started {relative(r.started_at)}
            </span>
            <span className="text-zinc-500">took {fmtDuration(r.duration_ms)}</span>
          </div>
          {r.error && <div className="mt-1 text-xs text-red-400/80 font-mono break-all">{r.error}</div>}
        </li>
      ))}
    </ol>
  );
}

export default function RetriesPage() {
  useEffect(() => { document.title = "Retries — Sluice"; }, []);

  const [stateFilter, setStateFilter] = useState("");
  const [expanded, setExpanded] = useState<Set<string>>(new Set());
  const [replaying, setReplaying] = useState<string | null>(null);
  const [replayErr, setReplayErr] = useState<string | null>(null);

  const { data, isLoading, error, refetch } = useQuery({
    queryKey: ["retried-jobs", stateFilter],
    queryFn: () => fetchRetriedJobs(stateFilter),
    refetchInterval: 10000,
  });
  const jobs = data?.jobs ?? [];

  function toggle(id: string) {
    setExpanded((s) => {
      const n = new Set(Array.from(s));
      if (n.has(id)) n.delete(id); else n.add(id);
      return n;
    });
  }

  async function handleReplay(jobId: string) {
    setReplaying(jobId);
    setReplayErr(null);
    try {
      await replayJob(jobId);
      refetch();
    } catch {
      setReplayErr(jobId);
    } finally {
      setReplaying(null);
    }
  }

  return (
    <div className="space-y-5">
      <div className="flex items-center justify-between flex-wrap gap-3">
        <div>
          <h1 className="text-2xl font-bold tracking-tight text-white">Retries</h1>
          <p className="text-zinc-500 text-sm mt-0.5">
            Jobs with at least one failed attempt{data ? ` · ${data.count.toLocaleString()} shown` : ""}
          </p>
        </div>
        <div className="flex items-center gap-2">
          <select
            value={stateFilter}
            onChange={(e) => setStateFilter(e.target.value)}
            className="text-xs bg-zinc-900 border border-zinc-700 rounded-lg px-3 py-1.5 text-zinc-300 focus:outline-none focus:border-zinc-500 cursor-pointer"
          >
            {FILTERS.map((f) => <option key={f.value} value={f.value}>{f.label}</option>)}
          </select>
          <button
            onClick={() => refetch()}
            className="text-xs text-zinc-400 hover:text-white border border-zinc-700 hover:border-zinc-500 rounded-lg px-3 py-1.5 transition-all"
          >
            ↺ Refresh
          </button>
        </div>
      </div>

      {replayErr && (
        <div className="text-sm text-red-400 bg-red-950/30 border border-red-900/50 rounded-xl px-4 py-2.5">
          Replay failed for job {replayErr.slice(0, 8)}…
        </div>
      )}

      {isLoading && <div className="text-sm text-zinc-500">Loading…</div>}

      {error && (
        <div className="flex items-center gap-3 text-sm text-red-400 bg-red-950/30 border border-red-900/50 rounded-xl px-4 py-3">
          Failed to load retried jobs
          <button onClick={() => refetch()} className="underline text-red-300 hover:text-red-100">Retry</button>
        </div>
      )}

      {data && (
        <div className="rounded-xl border border-zinc-700/60 overflow-hidden overflow-x-auto">
          <table className="w-full text-sm min-w-[760px]">
            <thead>
              <tr className="border-b border-zinc-700/60 bg-zinc-800/40">
                {["", "Job ID", "Type", "State", "Attempts", "Next retry", "Last error", ""].map((h, i) => (
                  <th key={i} className="text-left px-4 py-3 text-xs font-semibold text-zinc-500 uppercase tracking-wider">{h}</th>
                ))}
              </tr>
            </thead>
            <tbody>
              {jobs.map((j) => {
                const open = expanded.has(j.id);
                return (
                  <Fragment key={j.id}>
                    <tr
                      className="border-b border-zinc-800/60 hover:bg-zinc-800/30 transition-colors cursor-pointer"
                      onClick={() => toggle(j.id)}
                    >
                      <td className="pl-4 py-3 text-zinc-500 w-6">{open ? "▾" : "▸"}</td>
                      <td className="px-4 py-3 font-mono text-xs text-zinc-400" title={j.id}>{j.id.slice(0, 8)}…</td>
                      <td className="px-4 py-3 text-xs text-zinc-300">{j.type}</td>
                      <td className="px-4 py-3"><StateBadge state={j.state} /></td>
                      <td className="px-4 py-3 text-xs text-zinc-300" title="attempts used / attempts allowed (1 + max_retries)">
                        {attemptsUsed(j)} / {j.max_retries + 1}
                      </td>
                      <td className="px-4 py-3 text-xs text-zinc-500">
                        {j.state === "failed" ? relative(j.run_at) : "—"}
                      </td>
                      <td className="px-4 py-3 text-xs text-red-400/80 max-w-[280px] truncate" title={j.last_error}>
                        {j.last_error ?? <span className="text-zinc-700">—</span>}
                      </td>
                      <td className="px-4 py-3 text-right">
                        {j.state === "dead" && (
                          <button
                            onClick={(e) => { e.stopPropagation(); handleReplay(j.id); }}
                            disabled={replaying === j.id}
                            className="text-xs font-medium text-blue-300 hover:text-white bg-blue-950/50 hover:bg-blue-900/60 border border-blue-800/60 hover:border-blue-600 rounded-lg px-3 py-1 transition-all disabled:opacity-40"
                          >
                            {replaying === j.id ? "…" : "Replay"}
                          </button>
                        )}
                      </td>
                    </tr>
                    {open && (
                      <tr className="border-b border-zinc-800/60 bg-zinc-900/40">
                        <td />
                        <td colSpan={7} className="px-4 py-3"><RunTimeline jobId={j.id} /></td>
                      </tr>
                    )}
                  </Fragment>
                );
              })}
              {jobs.length === 0 && (
                <tr>
                  <td colSpan={8} className="px-4 py-12 text-center text-zinc-600">No retried jobs</td>
                </tr>
              )}
            </tbody>
          </table>
        </div>
      )}
    </div>
  );
}
