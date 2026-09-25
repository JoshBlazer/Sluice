package api

import (
	"context"
	"log/slog"
	"net/http"
	"time"

	"github.com/gorilla/websocket"
	"github.com/sluice/internal/storage"
)

// Browsers can't set an Authorization header on a WebSocket handshake, so the
// API key travels as ?token=. Origin checks add nothing on top of that: the key
// is never sent ambiently the way a cookie would be.
var upgrader = websocket.Upgrader{
	CheckOrigin: func(r *http.Request) bool { return true },
}

// handleWebSocket authenticates the caller, then streams a snapshot of their
// tenant's queue depths and job counts every second.
func (s *Server) handleWebSocket(w http.ResponseWriter, r *http.Request) {
	key := r.URL.Query().Get("token")
	if key == "" {
		key = extractBearerToken(r)
	}
	if key == "" {
		writeError(w, http.StatusUnauthorized, "missing api key")
		return
	}
	t, err := s.tenants.lookup(r.Context(), key)
	if err != nil {
		if err != storage.ErrNotFound {
			slog.Error("ws tenant lookup", "err", err)
		}
		writeError(w, http.StatusUnauthorized, "invalid api key")
		return
	}

	conn, err := upgrader.Upgrade(w, r, nil)
	if err != nil {
		slog.Error("ws upgrade", "err", err)
		return
	}
	defer conn.Close()

	ctx, cancel := context.WithCancel(r.Context())
	defer cancel()

	// Close ctx when the client disconnects.
	go func() {
		for {
			if _, _, err := conn.NextReader(); err != nil {
				cancel()
				return
			}
		}
	}()

	ticker := time.NewTicker(time.Second)
	defer ticker.Stop()
	for {
		select {
		case <-ctx.Done():
			return
		case <-ticker.C:
			snap, err := s.snapshot(ctx, t)
			if err != nil {
				slog.Warn("ws snapshot", "tenant_id", t.ID, "err", err)
				continue
			}
			if err := conn.WriteJSON(snap); err != nil {
				return
			}
		}
	}
}
