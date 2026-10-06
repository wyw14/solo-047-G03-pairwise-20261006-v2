package route

import (
	"context"
	"fmt"
	"time"

	"github.com/wyw14/cry-103/internal/topology"
)

type Activator struct {
	topology *topology.Registry
}

func NewActivator(registry *topology.Registry) *Activator {
	return &Activator{topology: registry}
}

func (a *Activator) Protection(ctx context.Context, route *Route, at time.Time) (Snapshot, error) {
	select {
	case <-ctx.Done():
		return route.Snapshot(), fmt.Errorf("protection activation: %w", ctx.Err())
	default:
	}
	if _, err := a.topology.Open(route.Snapshot().SpanID, topology.PathPrimary, at); err != nil {
		return route.Snapshot(), err
	}
	if _, err := a.topology.Connect(route.Snapshot().SpanID, topology.PathProtection, at); err != nil {
		return route.Snapshot(), err
	}
	if _, err := a.topology.Switch(route.Snapshot().SpanID, topology.PathProtection, at); err != nil {
		return route.Snapshot(), err
	}
	return route.update(StateProtectionActive, topology.PathProtection, 1_000_000, at), nil
}

// ConnectProtection cross-connects the protection path assuming the primary
// path has already been opened by the caller.
func (a *Activator) ConnectProtection(ctx context.Context, route *Route, at time.Time) (Snapshot, error) {
	select {
	case <-ctx.Done():
		return route.Snapshot(), fmt.Errorf("protection connect: %w", ctx.Err())
	default:
	}
	if _, err := a.topology.Connect(route.Snapshot().SpanID, topology.PathProtection, at); err != nil {
		return route.Snapshot(), err
	}
	if _, err := a.topology.Switch(route.Snapshot().SpanID, topology.PathProtection, at); err != nil {
		return route.Snapshot(), err
	}
	return route.update(StateProtectionActive, topology.PathProtection, 1_000_000, at), nil
}

func (a *Activator) Primary(ctx context.Context, route *Route, at time.Time) (Snapshot, error) {
	select {
	case <-ctx.Done():
		return route.Snapshot(), ctx.Err()
	default:
	}
	if _, err := a.topology.Connect(route.Snapshot().SpanID, topology.PathPrimary, at); err != nil {
		return route.Snapshot(), err
	}
	if _, err := a.topology.Switch(route.Snapshot().SpanID, topology.PathPrimary, at); err != nil {
		return route.Snapshot(), err
	}
	return route.update(StatePrimary, topology.PathPrimary, 1_000_000, at), nil
}
