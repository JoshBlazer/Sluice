// NEXT_PUBLIC_API_URL can be relative ("/sluice-api") when the dashboard proxies
// the API through its own origin; see SLUICE_API_PROXY in next.config.mjs.
export const API_BASE = process.env.NEXT_PUBLIC_API_URL ?? "http://localhost:8080";

// The live-stats WebSocket follows API_BASE unless NEXT_PUBLIC_WS_URL overrides it.
export function wsURL(): string {
  if (process.env.NEXT_PUBLIC_WS_URL) return process.env.NEXT_PUBLIC_WS_URL;
  const url = new URL(API_BASE, window.location.href);
  url.protocol = url.protocol === "https:" ? "wss:" : "ws:";
  return url.href.replace(/\/$/, "") + "/ws";
}

// Pre-fills the sign-in form in demo environments (the Codespaces setup sets it to
// the local-only dev-token). Never set it for a deployment other people can reach.
export const DEMO_API_KEY = process.env.NEXT_PUBLIC_DEMO_API_KEY ?? "";
