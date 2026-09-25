package tenant

import (
	"context"

	"github.com/sluice/internal/storage"
)

type Tenant = storage.Tenant

type contextKey struct{}

// FromContext retrieves the tenant injected by the auth middleware.
func FromContext(ctx context.Context) (*Tenant, bool) {
	t, ok := ctx.Value(contextKey{}).(*Tenant)
	return t, ok && t != nil
}

// WithTenant returns a context carrying the authenticated tenant.
func WithTenant(ctx context.Context, t *Tenant) context.Context {
	return context.WithValue(ctx, contextKey{}, t)
}
