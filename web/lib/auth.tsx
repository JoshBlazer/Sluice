"use client";
import { createContext, useCallback, useContext, useEffect, useState } from "react";
import { API_BASE, DEMO_API_KEY } from "./config";

// The dashboard never has an API key built in: each viewer signs in with their
// tenant's key. It is kept in sessionStorage, so it lasts for the browser tab and
// is not sent anywhere except the Sluice API.
const STORAGE_KEY = "sluice.apiKey";

export function getToken(): string | null {
  try {
    return sessionStorage.getItem(STORAGE_KEY);
  } catch {
    return null;
  }
}

function storeToken(token: string | null) {
  try {
    if (token) sessionStorage.setItem(STORAGE_KEY, token);
    else sessionStorage.removeItem(STORAGE_KEY);
  } catch {
    // Storage unavailable (private mode, blocked): the key lives in memory only.
  }
}

// Fired by apiFetch when the API rejects the key, so the app returns to sign-in.
export const UNAUTHORIZED_EVENT = "sluice:unauthorized";

type Auth = { token: string | null; signOut: () => void };
const AuthContext = createContext<Auth>({ token: null, signOut: () => {} });

export const useAuth = () => useContext(AuthContext);

export function AuthGate({ children }: { children: React.ReactNode }) {
  const [token, setToken] = useState<string | null>(null);
  const [ready, setReady] = useState(false);

  useEffect(() => {
    setToken(getToken());
    setReady(true);
    const onUnauthorized = () => {
      storeToken(null);
      setToken(null);
    };
    window.addEventListener(UNAUTHORIZED_EVENT, onUnauthorized);
    return () => window.removeEventListener(UNAUTHORIZED_EVENT, onUnauthorized);
  }, []);

  const signOut = useCallback(() => {
    storeToken(null);
    setToken(null);
  }, []);

  if (!ready) return null;
  if (!token) {
    return (
      <SignIn
        onSignedIn={(t) => {
          storeToken(t);
          setToken(t);
        }}
      />
    );
  }
  return <AuthContext.Provider value={{ token, signOut }}>{children}</AuthContext.Provider>;
}

function SignIn({ onSignedIn }: { onSignedIn: (token: string) => void }) {
  const [key, setKey] = useState(DEMO_API_KEY);
  const [error, setError] = useState<string | null>(null);
  const [checking, setChecking] = useState(false);

  async function submit(e: React.FormEvent) {
    e.preventDefault();
    const candidate = key.trim();
    if (!candidate) return;
    setChecking(true);
    setError(null);
    try {
      const res = await fetch(`${API_BASE}/v1/stats`, {
        headers: { Authorization: `Bearer ${candidate}` },
        cache: "no-store",
      });
      if (res.status === 401) setError("That API key was not accepted.");
      else if (!res.ok) setError(`The API returned ${res.status}. Try again.`);
      else onSignedIn(candidate);
    } catch {
      setError(`Couldn't reach the Sluice API at ${API_BASE}.`);
    } finally {
      setChecking(false);
    }
  }

  return (
    <div className="min-h-[70vh] flex items-center justify-center">
      <form onSubmit={submit} className="w-full max-w-sm space-y-4 rounded-xl border border-zinc-700/60 bg-zinc-900/60 p-6">
        <div>
          <h1 className="text-xl font-bold text-white">Sign in to Sluice</h1>
          <p className="text-sm text-zinc-500 mt-1">
            {DEMO_API_KEY ? "Demo environment: the local dev tenant's key is filled in." : "Paste a tenant API key. You'll see that tenant's jobs."}
          </p>
        </div>
        <input
          type="password"
          autoComplete="off"
          value={key}
          onChange={(e) => setKey(e.target.value)}
          placeholder="sk_…"
          aria-label="API key"
          className="w-full rounded-lg bg-zinc-950 border border-zinc-700 px-3 py-2 text-sm text-zinc-100 focus:outline-none focus:border-zinc-500"
        />
        {error && <p className="text-sm text-red-400">{error}</p>}
        <button
          type="submit"
          disabled={checking || !key.trim()}
          className="w-full rounded-lg bg-blue-600 hover:bg-blue-500 disabled:opacity-40 text-sm font-medium text-white py-2 transition-colors"
        >
          {checking ? "Checking…" : "Sign in"}
        </button>
      </form>
    </div>
  );
}
