import { useEffect, useRef, useState } from "react";
import type { StatsResponse } from "./api";
import { getToken } from "./auth";
import { wsURL } from "./config";

export function useLiveStats() {
  const [data, setData] = useState<StatsResponse | null>(null);
  const [connected, setConnected] = useState(false);
  const ws = useRef<WebSocket | null>(null);

  useEffect(() => {
    let stopped = false;
    let retry: ReturnType<typeof setTimeout> | undefined;

    function connect() {
      // Browsers can't send an Authorization header on a WebSocket handshake,
      // so the API key goes in the query string.
      const sock = new WebSocket(`${wsURL()}?token=${encodeURIComponent(getToken() ?? "")}`);
      ws.current = sock;

      sock.onopen = () => setConnected(true);
      sock.onclose = () => {
        setConnected(false);
        if (!stopped) retry = setTimeout(connect, 3000);
      };
      sock.onerror = () => sock.close();
      sock.onmessage = (e) => {
        try {
          setData(JSON.parse(e.data));
        } catch {}
      };
    }
    connect();
    return () => {
      stopped = true;
      clearTimeout(retry);
      ws.current?.close();
    };
  }, []);

  return { data, connected };
}
