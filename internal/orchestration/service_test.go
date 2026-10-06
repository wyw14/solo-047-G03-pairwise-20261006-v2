package orchestration

import (
	"context"
	"strings"
	"testing"
	"time"

	"github.com/wyw14/cry-103/internal/platform"
	"github.com/wyw14/cry-103/internal/route"
	"github.com/wyw14/cry-103/internal/topology"
)

func newTestService(t *testing.T, drainDelay time.Duration, timeouts Timeouts) (*Service, *route.Route, *topology.Registry) {
	t.Helper()
	now := func() time.Time { return time.Date(2026, 8, 24, 3, 0, 0, 0, time.UTC) }
	registry := topology.NewRegistry()
	if _, err := registry.Register("S3", topology.PathPrimary, now()); err != nil {
		t.Fatal(err)
	}
	target, err := route.New("R-S3", "S3", now())
	if err != nil {
		t.Fatal(err)
	}
	drainer := route.NewDrainer(drainDelay)
	activator := route.NewActivator(registry)
	service := NewService(drainer, activator, registry, platform.UUIDGenerator{}, now, timeouts)
	return service, target, registry
}

func generousTimeouts() Timeouts {
	return Timeouts{Drain: time.Second, Connect: time.Second, Verify: time.Second}
}

func TestFailoverHappyPath(t *testing.T) {
	service, target, registry := newTestService(t, 0, generousTimeouts())
	snapshot, err := service.StartFailover(context.Background(), target)
	if err != nil {
		t.Fatalf("failover: %v", err)
	}
	if snapshot.State != StateProtected {
		t.Fatalf("state=%s, want %s", snapshot.State, StateProtected)
	}
	if len(snapshot.Stages) != 3 {
		t.Fatalf("stages=%d, want 3", len(snapshot.Stages))
	}
	for _, stage := range snapshot.Stages {
		if stage.Status != StageCompleted {
			t.Fatalf("stage %s status=%s", stage.Name, stage.Status)
		}
		if stage.Facts == nil {
			t.Fatalf("stage %s has no device facts", stage.Name)
		}
		if stage.TimeoutMs <= 0 {
			t.Fatalf("stage %s has no timeout recorded", stage.Name)
		}
	}
	if len(snapshot.Risks) != 0 {
		t.Fatalf("unexpected risks: %v", snapshot.Risks)
	}
	topologySnapshot, err := registry.Snapshot("S3")
	if err != nil {
		t.Fatal(err)
	}
	if topologySnapshot.ActivePath != topology.PathProtection {
		t.Fatalf("active path=%s", topologySnapshot.ActivePath)
	}
	if routeSnapshot := target.Snapshot(); routeSnapshot.State != route.StateProtectionActive {
		t.Fatalf("route state=%s", routeSnapshot.State)
	}
	last := snapshot.Stages[len(snapshot.Stages)-1]
	if last.Facts.Topology.ActivePath != topology.PathProtection {
		t.Fatalf("verify facts active_path=%s", last.Facts.Topology.ActivePath)
	}
}

func TestFailoverBlockedWhenPrimaryNotDrained(t *testing.T) {
	service, target, registry := newTestService(t, 20*time.Millisecond, generousTimeouts())
	ctx, cancel := context.WithTimeout(context.Background(), 10*time.Millisecond)
	defer cancel()
	snapshot, err := service.StartFailover(ctx, target)
	if err != nil {
		t.Fatalf("blocked failover must not return an error, got %v", err)
	}
	if snapshot.State != StateBlocked {
		t.Fatalf("state=%s, want %s", snapshot.State, StateBlocked)
	}
	if len(snapshot.Risks) != 1 || snapshot.Risks[0] != "primary-not-drained" {
		t.Fatalf("risks=%v, want primary-not-drained retained", snapshot.Risks)
	}
	if snapshot.Stages[0].Status != StageFailed {
		t.Fatalf("drain stage status=%s", snapshot.Stages[0].Status)
	}
	for _, stage := range snapshot.Stages[1:] {
		if stage.Status != StageBlocked {
			t.Fatalf("stage %s status=%s, want blocked", stage.Name, stage.Status)
		}
	}
	// The misconnection must not happen: the plant stays on primary.
	topologySnapshot, err := registry.Snapshot("S3")
	if err != nil {
		t.Fatal(err)
	}
	if topologySnapshot.ActivePath != topology.PathPrimary {
		t.Fatalf("active path=%s, cross-connect should have been blocked", topologySnapshot.ActivePath)
	}
	if traffic := target.Snapshot().TrafficBPS; traffic <= 0 {
		t.Fatalf("traffic_bps=%d, undrained traffic should remain", traffic)
	}

	// The same operation continues once the primary can be drained.
	resumed, err := service.Resume(context.Background(), snapshot.ID)
	if err != nil {
		t.Fatalf("resume: %v", err)
	}
	if resumed.State != StateProtected {
		t.Fatalf("resumed state=%s, want %s", resumed.State, StateProtected)
	}
	if resumed.ID != snapshot.ID {
		t.Fatalf("resumed ID=%s, want same operation %s", resumed.ID, snapshot.ID)
	}
	if len(resumed.Risks) != 1 {
		t.Fatalf("risks=%v, retained risk must survive the resume", resumed.Risks)
	}
	if resumed.Stages[0].Status != StageCompleted || resumed.Stages[0].Facts == nil {
		t.Fatalf("drain stage after resume: %+v", resumed.Stages[0])
	}
}

func TestResumeFromProtectedRestoresPrimary(t *testing.T) {
	service, target, registry := newTestService(t, 0, generousTimeouts())
	started, err := service.StartFailover(context.Background(), target)
	if err != nil {
		t.Fatalf("failover: %v", err)
	}
	resumed, err := service.Resume(context.Background(), started.ID)
	if err != nil {
		t.Fatalf("resume: %v", err)
	}
	if resumed.State != StateCompleted {
		t.Fatalf("state=%s, want %s", resumed.State, StateCompleted)
	}
	if resumed.Phase != PhaseRestore {
		t.Fatalf("phase=%s, want %s", resumed.Phase, PhaseRestore)
	}
	if len(resumed.Stages) != 7 {
		t.Fatalf("stages=%d, want 7 (failover + restore)", len(resumed.Stages))
	}
	for _, stage := range resumed.Stages {
		if stage.Status != StageCompleted {
			t.Fatalf("stage %s status=%s", stage.Name, stage.Status)
		}
		if stage.Facts == nil {
			t.Fatalf("stage %s has no device facts", stage.Name)
		}
	}
	topologySnapshot, err := registry.Snapshot("S3")
	if err != nil {
		t.Fatal(err)
	}
	if topologySnapshot.ActivePath != topology.PathPrimary {
		t.Fatalf("active path=%s, want primary", topologySnapshot.ActivePath)
	}
	if routeSnapshot := target.Snapshot(); routeSnapshot.State != route.StatePrimary {
		t.Fatalf("route state=%s, want primary", routeSnapshot.State)
	}
	if _, err := service.Resume(context.Background(), started.ID); err == nil {
		t.Fatal("completed operation must not resume again")
	}
}

func TestAbortSafelyTerminates(t *testing.T) {
	service, target, _ := newTestService(t, 0, generousTimeouts())
	started, err := service.StartFailover(context.Background(), target)
	if err != nil {
		t.Fatalf("failover: %v", err)
	}
	aborted, err := service.Abort(started.ID)
	if err != nil {
		t.Fatalf("abort: %v", err)
	}
	if aborted.State != StateTerminated {
		t.Fatalf("state=%s, want %s", aborted.State, StateTerminated)
	}
	last := aborted.Stages[len(aborted.Stages)-1]
	if last.Name != stageTerminate || last.Status != StageCompleted {
		t.Fatalf("last stage=%+v, want completed terminate stage", last)
	}
	if last.Facts == nil || last.Facts.Topology.ActivePath != topology.PathProtection {
		t.Fatalf("terminate facts=%+v, want protection path recorded", last.Facts)
	}
	if _, err := service.Abort(started.ID); err == nil {
		t.Fatal("terminated operation must not abort again")
	}
	if _, err := service.Resume(context.Background(), started.ID); err == nil {
		t.Fatal("terminated operation must not resume")
	}
}

func TestCrossConnectFailureRecordsRollback(t *testing.T) {
	now := func() time.Time { return time.Date(2026, 8, 24, 3, 0, 0, 0, time.UTC) }
	registry := topology.NewRegistry()
	target, err := route.New("R-SX", "SX", now())
	if err != nil {
		t.Fatal(err)
	}
	service := NewService(route.NewDrainer(0), route.NewActivator(registry), registry, platform.UUIDGenerator{}, now, generousTimeouts())
	snapshot, err := service.StartFailover(context.Background(), target)
	if err == nil {
		t.Fatal("cross-connect on an unknown span must fail")
	}
	if snapshot.State != StateFailed {
		t.Fatalf("state=%s, want %s", snapshot.State, StateFailed)
	}
	if snapshot.Stages[1].Status != StageFailed {
		t.Fatalf("cross-connect stage status=%s", snapshot.Stages[1].Status)
	}
	if len(snapshot.Rollback) != 1 {
		t.Fatalf("rollback records=%d, want 1", len(snapshot.Rollback))
	}
	rollback := snapshot.Rollback[0]
	if rollback.Action != "reconnect-primary" {
		t.Fatalf("rollback action=%s", rollback.Action)
	}
	if !strings.Contains(rollback.Message, "rollback failed") {
		t.Fatalf("rollback message=%q, want recorded failure", rollback.Message)
	}
	if _, err := service.Resume(context.Background(), snapshot.ID); err == nil {
		t.Fatal("failed operation must not resume")
	}
}
