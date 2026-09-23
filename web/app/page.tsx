"use client";
import { useLiveStats } from "@/lib/ws";
import {
  AreaChart, Area, XAxis, YAxis, Tooltip, ResponsiveContainer, CartesianGrid, Legend,
} from "recharts";
import { useEffect, useRef, useState } from "react";

type Snapshot = { t: string; high: number; normal: number; low: number };

const PRIORITY_COLORS: Record<string, string> = {
  high: "#f97316",
  normal: "#3b82f6",
  low: "#6b7280",
};

const PRIORITY_TINTS: Record<string, string> = {
  high: "rgba(249,115,22,0.08)",
  normal: "rgba(59,130,246,0.08)",
  low: "rgba(107,114,128,0.06)",
};

const PRIORITY_BORDERS: Record<string, string> = {
  high: "#f97316",
  normal: "#3b82f6",
  low: "#52525b",
};

const STATE_COLORS: Record<string, string> = {
  succeeded: "text-emerald-400",
  failed: "text-amber-400",
  dead: "text-red-400",
  pending: "text-blue-400",
  claimed: "text-purple-400",
  running: "text-violet-400",
  scheduled: "text-sky-400",
  cancelled: "text-zinc-400",
};

const STATE_BAR_COLORS: Record<string, string> = {
  succeeded: "#34d399",
  dead: "#f87171",
  failed: "#fbbf24",
  running: "#a78bfa",
  claimed: "#8b5cf6",
  pending: "#60a5fa",
  scheduled: "#38bdf8",
  cancelled: "#a1a1aa",
};

const ACTIVE_STATES = ["pending", "scheduled", "claimed", "running"];
const TERMINAL_STATES = ["succeeded", "failed", "dead", "cancelled"];

export default function QueuePage() {
  useEffect(() => { document.title = "Queue — Sluice"; }, []);

  const { data, connected } = useLiveStats();
  const [history, setHistory] = useState<Snapshot[]>([]);
  const lastTs = useRef("");

  if (data && String(data.timestamp) !== lastTs.current) {
    lastTs.current = String(data.timestamp);
    const totals = { high: 0, normal: 0, low: 0 };
    (data.queues ?? []).forEach((q) => {
      if (q.priority in totals) totals[q.priority as keyof typeof totals] += q.depth;
    });
    setHistory((h) => [...h.slice(-59), { t: new Date().toLocaleTimeString([], { hour: "2-digit", minute: "2-digit", second: "2-digit" }), ...totals }]);
  }

  const current = history[history.length - 1];
  const prev = history.length >= 10 ? history[history.length - 10] : null;
  const totalDepth = current ? current.high + current.normal + current.low : 0;

  const delta = (key: keyof Snapshot): number | null => {
    if (!current || !prev) return null;
    return (current[key] as number) - (prev[key] as number);
  };

  const jobs_by_state = data?.jobs_by_state ?? {};

  return (
    <div className="space-y-6">
      {/* Header */}
      <div className="flex items-center justify-between">
        <div>
          <h1 className="text-2xl font-bold tracking-tight text-white">Queue Depth</h1>
          <p className="text-zinc-500 text-sm mt-0.5">
            {totalDepth > 0 ? <span className="text-zinc-300 font-medium">{totalDepth.toLocaleString()}</span> : "0"} job{totalDepth !== 1 ? "s" : ""} waiting
          </p>
        </div>
        <span className={`text-xs px-3 py-1.5 rounded-full border flex items-center gap-2 font-medium ${connected ? "border-emerald-800 text-emerald-400 bg-emerald-950/40" : "border-zinc-700 text-zinc-500"}`}>
          <span className={`w-1.5 h-1.5 rounded-full ${connected ? "bg-emerald-400 animate-pulse" : "bg-zinc-500"}`} />
          {connected ? "live" : "reconnecting…"}
        </span>
      </div>

      {/* Priority stat cards */}
      <div className="grid grid-cols-1 sm:grid-cols-3 gap-4">
        {(["high", "normal", "low"] as const).map((p) => {
          const count = current?.[p] ?? 0;
          const d = delta(p);
          return (
            <div
              key={p}
              className="rounded-xl border border-zinc-700/60 p-5 relative overflow-hidden"
              style={{ background: `linear-gradient(135deg, ${PRIORITY_TINTS[p]} 0%, #18181b 60%)` }}
            >
              {/* Colored top accent */}
              <div className="absolute top-0 left-0 right-0 h-0.5 rounded-t-xl" style={{ backgroundColor: PRIORITY_BORDERS[p] }} />
              <p className="text-xs font-medium text-zinc-400 uppercase tracking-wider mb-3 capitalize">{p} priority</p>
              <div className="flex items-end justify-between">
                <p className="text-4xl font-bold tabular-nums" style={{ color: PRIORITY_COLORS[p] }}>{count.toLocaleString()}</p>
                {d !== null && d !== 0 && (
                  <span className={`text-sm font-mono font-semibold ${d > 0 ? "text-red-400" : "text-emerald-400"}`}>
                    {d > 0 ? `▲ ${d}` : `▼ ${Math.abs(d)}`}
                  </span>
                )}
              </div>
            </div>
          );
        })}
      </div>

      {/* Chart */}
      <div className="rounded-xl border border-zinc-700/60 bg-zinc-900/50 p-5">
        <p className="text-sm font-medium text-zinc-300 mb-4">Queue depth over time <span className="text-zinc-600 font-normal">(last 60s)</span></p>
        {history.length === 0 ? (
          <div className="h-[220px] flex items-center justify-center text-zinc-600 text-sm">
            Waiting for first update…
          </div>
        ) : (
          <ResponsiveContainer width="100%" height={220}>
            <AreaChart data={history}>
              <defs>
                {Object.entries(PRIORITY_COLORS).map(([k, color]) => (
                  <linearGradient key={k} id={`grad-${k}`} x1="0" y1="0" x2="0" y2="1">
                    <stop offset="5%" stopColor={color} stopOpacity={0.4} />
                    <stop offset="95%" stopColor={color} stopOpacity={0} />
                  </linearGradient>
                ))}
              </defs>
              <CartesianGrid strokeDasharray="3 3" stroke="#3f3f46" vertical={false} />
              <XAxis dataKey="t" tick={{ fill: "#52525b", fontSize: 11 }} axisLine={false} tickLine={false}
                interval={Math.max(0, Math.floor(history.length / 6) - 1)} />
              <YAxis tick={{ fill: "#52525b", fontSize: 11 }} allowDecimals={false} width={28} axisLine={false} tickLine={false} />
              <Tooltip
                contentStyle={{ background: "#18181b", border: "1px solid #3f3f46", borderRadius: "8px", fontSize: 12 }}
                labelStyle={{ color: "#a1a1aa" }}
                cursor={{ stroke: "#3f3f46" }}
              />
              <Legend
                content={() => (
                  <div style={{ display: "flex", justifyContent: "center", gap: "24px", fontSize: "12px", marginTop: "8px" }}>
                    {(["high", "normal", "low"] as const).map((p) => (
                      <span key={p} style={{ display: "flex", alignItems: "center", gap: "6px" }}>
                        <span style={{ display: "inline-block", width: "16px", height: "2px", backgroundColor: PRIORITY_COLORS[p], borderRadius: "1px" }} />
                        <span style={{ color: PRIORITY_COLORS[p], fontWeight: 500 }}>{p}</span>
                      </span>
                    ))}
                  </div>
                )}
              />
              {(["high", "normal", "low"] as const).map((p) => (
                <Area key={p} type="monotone" dataKey={p} stroke={PRIORITY_COLORS[p]}
                  fill={`url(#grad-${p})`} strokeWidth={2} dot={false} />
              ))}
            </AreaChart>
          </ResponsiveContainer>
        )}
      </div>

      {/* Jobs by state */}
      {data?.jobs_by_state && (
        <div className="rounded-xl border border-zinc-700/60 bg-zinc-900/50 p-5">
          <p className="text-sm font-medium text-zinc-300 mb-5">Jobs by state</p>
          <div className="grid grid-cols-1 md:grid-cols-2 gap-8">
            {[
              { label: "Active", states: ACTIVE_STATES, showPct: false },
              { label: "Terminal", states: TERMINAL_STATES, showPct: true },
            ].map(({ label, states, showPct }) => {
              const entries = states.map((s) => ({ state: s, count: jobs_by_state[s] ?? 0 }))
                .filter((e) => e.count > 0 || ACTIVE_STATES.includes(e.state));
              const max = Math.max(...entries.map((e) => e.count), 1);
              const groupTotal = entries.reduce((sum, e) => sum + e.count, 0);
              return (
                <div key={label}>
                  <p className="text-xs font-semibold text-zinc-500 uppercase tracking-widest mb-3">{label}</p>
                  <div className="space-y-3">
                    {entries.map(({ state, count }) => {
                      const pct = showPct && groupTotal > 0 ? Math.round((count / groupTotal) * 100) : null;
                      return (
                        <div key={state} className="flex items-center gap-3">
                          <span className={`w-20 text-xs text-right font-medium ${STATE_COLORS[state] ?? "text-zinc-400"}`}>{state}</span>
                          <div className="flex-1 bg-zinc-800/80 rounded-full h-2 overflow-hidden">
                            <div
                              className="h-full rounded-full transition-all duration-700"
                              style={{ width: `${(count / max) * 100}%`, backgroundColor: STATE_BAR_COLORS[state] ?? "#52525b" }}
                            />
                          </div>
                          {pct !== null && (
                            <span className="text-zinc-600 text-xs w-9 text-right tabular-nums">{pct}%</span>
                          )}
                          <span className="text-zinc-200 font-mono font-semibold text-xs w-12 text-right tabular-nums">
                            {count > 0 ? count.toLocaleString() : <span className="text-zinc-700">—</span>}
                          </span>
                        </div>
                      );
                    })}
                  </div>
                </div>
              );
            })}
          </div>
        </div>
      )}
    </div>
  );
}
