"use client";
import { useQuery } from "@tanstack/react-query";
import { fetchDeadLetter, replayJob, type DeadLetterEntry } from "@/lib/api";
import { useEffect, useState } from "react";

function relativeTime(dateStr: string): string {
  const seconds = Math.floor((Date.now() - new Date(dateStr).getTime()) / 1000);
  if (seconds < 60) return `${seconds}s ago`;
  const minutes = Math.floor(seconds / 60);
  if (minutes < 60) return `${minutes}m ago`;
  const hours = Math.floor(minutes / 60);
  if (hours < 24) return `${hours}h ago`;
  return `${Math.floor(hours / 24)}d ago`;
}

function CopyId({ id, dim }: { id: string; dim?: boolean }) {
  const [copied, setCopied] = useState(false);
  return (
    <button
      className={`font-mono text-xs transition-colors cursor-pointer hover:text-white ${dim ? "text-zinc-600" : "text-zinc-400"}`}
      title={`Copy: ${id}`}
      onClick={() => { navigator.clipboard.writeText(id); setCopied(true); setTimeout(() => setCopied(false), 1500); }}
    >
      {copied ? <span className="text-emerald-400 font-medium">copied!</span> : `${id.slice(0, 8)}…`}
    </button>
  );
}

function ExpandableError({ text }: { text: string | undefined }) {
  const [expanded, setExpanded] = useState(false);
  if (!text) return <span className="text-zinc-700">—</span>;
  const short = text.length > 48;
  return (
    <button
      className="text-left text-red-400/80 hover:text-red-300 text-xs transition-colors max-w-[240px]"
      onClick={() => short && setExpanded((v) => !v)}
      title={short ? (expanded ? "Click to collapse" : "Click to expand") : undefined}
    >
      {expanded || !short ? text : `${text.slice(0, 48)}…`}
    </button>
  );
}

function Skeleton() {
  return (
    <div className="rounded-xl border border-zinc-700/60 overflow-hidden">
      <table className="w-full text-sm">
        <thead>
          <tr className="border-b border-zinc-700/60 bg-zinc-800/40">
            {["Job ID", "Tenant", "Attempts", "Moved", "Error", ""].map((h, i) => (
              <th key={i} className="text-left px-4 py-3 text-xs font-semibold text-zinc-500 uppercase tracking-wider">{h}</th>
            ))}
          </tr>
        </thead>
        <tbody>
          {Array.from({ length: 6 }).map((_, i) => (
            <tr key={i} className="border-b border-zinc-800/60">
              {[80, 80, 40, 60, 140, 60].map((w, j) => (
                <td key={j} className="px-4 py-3">
                  <div className="animate-pulse bg-zinc-800 rounded-sm h-3" style={{ width: `${w}px` }} />
                </td>
              ))}
            </tr>
          ))}
        </tbody>
      </table>
    </div>
  );
}

export default function DeadLetterPage() {
  useEffect(() => { document.title = "Dead Letter — Sluice"; }, []);

  const [replaying, setReplaying] = useState<string | null>(null);
  const [replayDone, setReplayDone] = useState<Set<string>>(new Set());
  const [replayErr, setReplayErr] = useState<string | null>(null);

  const { data, isLoading, error, refetch } = useQuery({
    queryKey: ["dead-letter"],
    queryFn: () => fetchDeadLetter(100),
    refetchInterval: 10000,
  });

  async function handleReplay(jobId: string) {
    setReplaying(jobId);
    setReplayErr(null);
    try {
      await replayJob(jobId);
      setReplayDone((s) => { const n = new Set(Array.from(s)); n.add(jobId); return n; });
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
          <h1 className="text-2xl font-bold tracking-tight text-white">Dead Letter</h1>
          {data && (
            <p className="text-zinc-500 text-sm mt-0.5">
              <span className="text-red-400 font-semibold">{data.count.toLocaleString()}</span> entr{data.count !== 1 ? "ies" : "y"} waiting for inspection
            </p>
          )}
        </div>
        <button
          onClick={() => refetch()}
          className="text-xs text-zinc-400 hover:text-white border border-zinc-700 hover:border-zinc-500 rounded-lg px-3 py-1.5 transition-all"
        >
          ↺ Refresh
        </button>
      </div>

      {replayErr && (
        <div className="text-sm text-red-400 bg-red-950/30 border border-red-900/50 rounded-xl px-4 py-2.5">
          Replay failed for job {replayErr.slice(0, 8)}…
        </div>
      )}

      {isLoading && <Skeleton />}

      {error && (
        <div className="flex items-center gap-3 text-sm text-red-400 bg-red-950/30 border border-red-900/50 rounded-xl px-4 py-3">
          Failed to load dead letter queue
          <button onClick={() => refetch()} className="underline text-red-300 hover:text-red-100">Retry</button>
        </div>
      )}

      {data && (
        <div className="rounded-xl border border-zinc-700/60 overflow-hidden overflow-x-auto">
          <table className="w-full text-sm min-w-[700px]">
            <thead>
              <tr className="border-b border-zinc-700/60 bg-zinc-800/40">
                {["Job ID", "Tenant", "Attempts", "Moved", "Error", ""].map((h, i) => (
                  <th key={i} className={`px-4 py-3 text-xs font-semibold text-zinc-500 uppercase tracking-wider ${i === 5 ? "" : "text-left"}`}>{h}</th>
                ))}
              </tr>
            </thead>
            <tbody>
              {(data.entries ?? []).map((e: DeadLetterEntry) => (
                <tr key={e.job_id} className={`border-b border-zinc-800/60 hover:bg-zinc-800/30 transition-colors ${replayDone.has(e.job_id) ? "opacity-30" : ""}`}>
                  <td className="px-4 py-3"><CopyId id={e.job_id} /></td>
                  <td className="px-4 py-3"><CopyId id={e.tenant_id} dim /></td>
                  <td className="px-4 py-3">
                    <span className="text-xs font-bold text-amber-400 bg-amber-950/40 border border-amber-900/50 px-2 py-0.5 rounded-full">
                      {e.attempt_count}×
                    </span>
                  </td>
                  <td className="px-4 py-3 text-xs text-zinc-500" title={new Date(e.moved_at).toLocaleString()}>
                    {relativeTime(e.moved_at)}
                  </td>
                  <td className="px-4 py-3"><ExpandableError text={e.final_error} /></td>
                  <td className="px-4 py-3 text-right">
                    {replayDone.has(e.job_id) ? (
                      <span className="text-xs text-emerald-500 font-medium">✓ replayed</span>
                    ) : (
                      <button
                        onClick={() => handleReplay(e.job_id)}
                        disabled={replaying === e.job_id}
                        className="text-xs font-medium text-blue-300 hover:text-white bg-blue-950/50 hover:bg-blue-900/60 border border-blue-800/60 hover:border-blue-600 rounded-lg px-3 py-1 transition-all disabled:opacity-40 disabled:cursor-not-allowed"
                      >
                        {replaying === e.job_id ? "…" : "Replay"}
                      </button>
                    )}
                  </td>
                </tr>
              ))}
              {data.entries?.length === 0 && (
                <tr>
                  <td colSpan={6} className="px-4 py-12 text-center text-zinc-600">No dead-letter jobs</td>
                </tr>
              )}
            </tbody>
          </table>
        </div>
      )}
    </div>
  );
}
