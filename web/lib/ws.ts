import { useEffect, useRef, useState } from "react";
import type { StatsResponse } from "./api";
import { getToken } from "./auth";

const WS_URL = process.env.NEXT_PUBLIC_WS_URL ?? "ws://localhost:8080/ws";

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
      const sock = new WebSocket(`${WS_URL}?token=${encodeURIComponent(getToken() ?? "")}`);
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
