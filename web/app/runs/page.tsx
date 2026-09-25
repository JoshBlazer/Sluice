"use client";
import { useQuery } from "@tanstack/react-query";
import { fetchRuns, type JobRun } from "@/lib/api";
import { useEffect, useState } from "react";

const STATE_COLORS: Record<string, string> = {
  succeeded: "text-emerald-400",
  failed: "text-amber-400",
  dead: "text-red-400",
  claimed: "text-purple-400",
  running: "text-violet-400",
};

const STATE_BADGE: Record<string, string> = {
  succeeded: "bg-emerald-950/60 text-emerald-400 border border-emerald-900/60",
  failed:    "bg-amber-950/60 text-amber-400 border border-amber-900/60",
  dead:      "bg-red-950/60 text-red-400 border border-red-900/60",
  running:   "bg-violet-950/60 text-violet-400 border border-violet-900/60",
  claimed:   "bg-purple-950/60 text-purple-400 border border-purple-900/60",
};

function fmtDuration(ms: number | null): { text: string; cls: string } {
  if (ms === null) return { text: "—", cls: "text-zinc-600" };
  if (ms === 0)    return { text: "< 1ms", cls: "text-emerald-500" };
  if (ms < 500)    return { text: `${ms}ms`, cls: "text-emerald-400" };
  if (ms < 5000)   return { text: `${(ms / 1000).toFixed(1)}s`, cls: "text-amber-400" };
  return { text: `${(ms / 1000).toFixed(1)}s`, cls: "text-red-400" };
}

function CopyId({ id }: { id: string }) {
  const [copied, setCopied] = useState(false);
  return (
    <button
      className="font-mono text-xs text-zinc-400 hover:text-white transition-colors cursor-pointer"
      title={`Copy: ${id}`}
      onClick={() => { navigator.clipboard.writeText(id); setCopied(true); setTimeout(() => setCopied(false), 1500); }}
    >
      {copied ? <span className="text-emerald-400 font-medium">copied!</span> : `${id.slice(0, 8)}…`}
    </button>
  );
}

function Skeleton() {
  return (
    <div className="rounded-xl border border-zinc-700/60 overflow-hidden">
      <table className="w-full text-sm">
        <thead>
          <tr className="border-b border-zinc-700/60 bg-zinc-800/40">
            {["Job ID", "Type", "State", "Try", "Duration", "Started", "Error"].map((h) => (
              <th key={h} className="text-left px-4 py-3 text-xs font-semibold text-zinc-500 uppercase tracking-wider">{h}</th>
            ))}
          </tr>
        </thead>
        <tbody>
          {Array.from({ length: 8 }).map((_, i) => (
            <tr key={i} className="border-b border-zinc-800/60">
              {[80, 60, 70, 30, 45, 90, 0].map((w, j) => (
                <td key={j} className="px-4 py-3">
                  {w > 0 && <div className="animate-pulse bg-zinc-800 rounded-sm h-3" style={{ width: `${w}px` }} />}
                </td>
              ))}
            </tr>
          ))}
        </tbody>
      </table>
    </div>
  );
}

export default function RunsPage() {
  useEffect(() => { document.title = "Runs — Sluice"; }, []);
  const [stateFilter, setStateFilter] = useState("");

  const { data, isLoading, error, refetch } = useQuery({
    queryKey: ["runs"],
    queryFn: () => fetchRuns(100),
    refetchInterval: 5000,
  });

  const runs = (data?.runs ?? []).filter((r: JobRun) => !stateFilter || r.state === stateFilter);

  return (
    <div className="space-y-5">
      <div className="flex items-center justify-between flex-wrap gap-3">
        <div>
          <h1 className="text-2xl font-bold tracking-tight text-white">Recent Runs</h1>
          {data && <p className="text-zinc-500 text-sm mt-0.5">{runs.length} of {data.count} run{data.count !== 1 ? "s" : ""}</p>}
        </div>
        <div className="flex items-center gap-2">
          <select
            value={stateFilter}
            onChange={(e) => setStateFilter(e.target.value)}
            className="text-xs bg-zinc-900 border border-zinc-700 rounded-lg px-3 py-1.5 text-zinc-300 focus:outline-hidden focus:border-zinc-500 cursor-pointer"
          >
            <option value="">All states</option>
            <option value="succeeded">Succeeded</option>
            <option value="failed">Failed</option>
            <option value="dead">Dead</option>
            <option value="running">Running</option>
            <option value="claimed">Claimed</option>
          </select>
          <button
            onClick={() => refetch()}
            className="text-xs text-zinc-400 hover:text-white border border-zinc-700 hover:border-zinc-500 rounded-lg px-3 py-1.5 transition-all"
          >
            ↺ Refresh
          </button>
          <span className="text-xs text-zinc-600">auto-refreshes every 5s</span>
        </div>
      </div>

      {isLoading && <Skeleton />}

      {error && (
        <div className="flex items-center gap-3 text-sm text-red-400 bg-red-950/30 border border-red-900/50 rounded-xl px-4 py-3">
          Failed to load runs
          <button onClick={() => refetch()} className="underline text-red-300 hover:text-red-100">Retry</button>
        </div>
      )}

      {data && (
        <div className="rounded-xl border border-zinc-700/60 overflow-hidden overflow-x-auto">
          <table className="w-full text-sm min-w-[720px]">
            <thead>
              <tr className="border-b border-zinc-700/60 bg-zinc-800/40">
                {["Job ID", "Type", "State", "Try", "Duration", "Started", "Error"].map((h) => (
                  <th key={h} className="text-left px-4 py-3 text-xs font-semibold text-zinc-500 uppercase tracking-wider">{h}</th>
                ))}
              </tr>
            </thead>
            <tbody>
              {runs.map((r: JobRun) => {
                const dur = fmtDuration(r.duration_ms);
                const started = new Date(r.started_at);
                const badge = STATE_BADGE[r.state];
                return (
                  <tr key={r.run_id} className="border-b border-zinc-800/60 hover:bg-zinc-800/30 transition-colors">
                    <td className="px-4 py-3"><CopyId id={r.job_id} /></td>
                    <td className="px-4 py-3 text-zinc-300 font-medium">{r.type}</td>
                    <td className="px-4 py-3">
                      <span className={`text-xs font-medium px-2 py-0.5 rounded-full ${badge ?? (STATE_COLORS[r.state] ?? "text-zinc-300")}`}>
                        {r.state}
                      </span>
                    </td>
                    <td className="px-4 py-3 text-zinc-500 tabular-nums text-xs">{r.attempt + 1}</td>
                    <td className={`px-4 py-3 font-mono text-xs font-semibold ${dur.cls}`}>{dur.text}</td>
                    <td className="px-4 py-3 text-xs text-zinc-500 tabular-nums whitespace-nowrap">
                      {started.toLocaleDateString([], { month: "short", day: "numeric" })}{" "}
                      <span className="text-zinc-600">{started.toLocaleTimeString([], { hour: "2-digit", minute: "2-digit", second: "2-digit" })}</span>
                    </td>
                    <td className="px-4 py-3 text-xs text-red-400/80 max-w-[200px] truncate" title={r.error ?? ""}>{r.error ?? ""}</td>
                  </tr>
                );
              })}
              {runs.length === 0 && (
                <tr>
                  <td colSpan={7} className="px-4 py-12 text-center text-zinc-600">
                    {stateFilter ? `No ${stateFilter} runs` : "No runs yet"}
                  </td>
                </tr>
              )}
            </tbody>
          </table>
        </div>
      )}
    </div>
  );
}
