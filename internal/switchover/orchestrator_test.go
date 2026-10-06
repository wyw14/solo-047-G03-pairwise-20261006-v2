package switchover

import (
	"context"
	"strings"
	"testing"
	"time"

	"github.com/wyw14/cry-103/internal/platform"
	"github.com/wyw14/cry-103/internal/route"
	"github.com/wyw14/cry-103/internal/topology"
)

type fixture struct {
	registry  *topology.Registry
	target    *route.Route
	drainer   *route.Drainer
	activator *route.Activator
	ids       platform.IDGenerator
	now       func() time.Time
}

func newFixture(t *testing.T, drainDelay time.Duration) *fixture {
	t.Helper()
	now := func() time.Time { return time.Date(2026, 10, 5, 12, 0, 0, 0, time.UTC) }
	registry := topology.NewRegistry()
	if _, err := registry.Register("S3", topology.PathPrimary, now()); err != nil {
		t.Fatal(err)
	}
	target, err := route.New("R-S3", "S3", now())
	if err != nil {
		t.Fatal(err)
	}
	drainer := route.NewDrainer(drainDelay)
	return &fixture{
		registry: registry, target: target, drainer: drainer,
		activator: route.NewActivator(registry),
		ids:       platform.UUIDGenerator{}, now: now,
	}
}

func (f *fixture) orchestrator(drainTimeout time.Duration) *Orchestrator {
	return NewOrchestrator(f.drainer, f.activator, f.registry, f.ids, f.now, drainTimeout)
}

func stageOf(t *testing.T, plan PlanSnapshot, name string) Stage {
	t.Helper()
	for _, stage := range plan.Stages {
		if stage.Name == name {
			return stage
		}
	}
	t.Fatalf("stage %s missing from plan %+v", name, plan.Stages)
	return Stage{}
}

func TestOrchestratorFailoverThenRestore(t *testing.T) {
	f := newFixture(t, time.Millisecond)
	orchestrator := f.orchestrator(time.Second)

	plan := orchestrator.Start(context.Background(), f.target, "cable cut")
	snapshot := plan.Snapshot()
	if snapshot.State != PlanProtected {
		t.Fatalf("state=%s want %s", snapshot.State, PlanProtected)
	}
	for _, name := range failoverStages {
		stage := stageOf(t, snapshot, name)
		if stage.Status != StageSucceeded {
			t.Fatalf("stage %s status=%s", name, stage.Status)
		}
		if stage.Facts == nil {
			t.Fatalf("stage %s has no device facts", name)
		}
	}
	connect := stageOf(t, snapshot, StageConnectProtection)
	if connect.Facts.ActivePath != topology.PathProtection || !connect.Facts.PrimaryOpen {
		t.Fatalf("unexpected connect facts: %+v", connect.Facts)
	}
	if stageOf(t, snapshot, StageDrainPrimary).TimeoutMS != time.Second.Milliseconds() {
		t.Fatalf("drain stage timeout not recorded: %+v", stageOf(t, snapshot, StageDrainPrimary))
	}
	if stageOf(t, snapshot, StageConnectProtection).Rollback == "" {
		t.Fatal("connect stage must carry rollback information")
	}
	if f.target.Snapshot().State != route.StateProtectionActive {
		t.Fatalf("route state=%s", f.target.Snapshot().State)
	}

	resumed, err := orchestrator.Resume(context.Background(), plan, f.target)
	if err != nil {
		t.Fatal(err)
	}
	if resumed.State != PlanCompleted {
		t.Fatalf("state=%s want %s", resumed.State, PlanCompleted)
	}
	restore := stageOf(t, resumed, StageVerifyRestore)
	if restore.Facts == nil || restore.Facts.ActivePath != topology.PathPrimary || restore.Facts.RouteState != route.StatePrimary {
		t.Fatalf("unexpected restore facts: %+v", restore.Facts)
	}
	if f.target.Snapshot().State != route.StatePrimary {
		t.Fatalf("route state=%s", f.target.Snapshot().State)
	}
	topologySnapshot, err := f.registry.Snapshot("S3")
	if err != nil {
		t.Fatal(err)
	}
	if topologySnapshot.ActivePath != topology.PathPrimary {
		t.Fatalf("active path=%s", topologySnapshot.ActivePath)
	}
}

func TestOrchestratorBlocksConnectWhenPrimaryNotDrained(t *testing.T) {
	// Drain takes 50ms but the stage only allows 5ms: the primary is not
	// drained, the risk must be retained and the cross-connect blocked.
	f := newFixture(t, 50*time.Millisecond)
	orchestrator := f.orchestrator(5 * time.Millisecond)

	plan := orchestrator.Start(context.Background(), f.target, "cable cut")
	snapshot := plan.Snapshot()
	if snapshot.State != PlanRiskHeld {
		t.Fatalf("state=%s want %s", snapshot.State, PlanRiskHeld)
	}
	if len(snapshot.Risks) == 0 {
		t.Fatal("expected retained risk")
	}
	if stageOf(t, snapshot, StageDrainPrimary).Status != StageBlocked {
		t.Fatalf("drain stage=%s", stageOf(t, snapshot, StageDrainPrimary).Status)
	}
	if stageOf(t, snapshot, StageConnectProtection).Status != StagePending {
		t.Fatal("protection must not be connected while the primary is not drained")
	}
	if f.target.Snapshot().TrafficBPS == 0 {
		t.Fatal("traffic should still be on the primary path")
	}
	topologySnapshot, err := f.registry.Snapshot("S3")
	if err != nil {
		t.Fatal(err)
	}
	if topologySnapshot.ActivePath != topology.PathPrimary {
		t.Fatalf("active path=%s, cross-connect must be blocked", topologySnapshot.ActivePath)
	}

	// The drain completes in time on retry: resume the same operation with a
	// wider drain budget and it must carry through to the protection path.
	resumed, err := f.orchestrator(time.Second).Resume(context.Background(), plan, f.target)
	if err != nil {
		t.Fatal(err)
	}
	if resumed.State != PlanProtected {
		t.Fatalf("state=%s want %s", resumed.State, PlanProtected)
	}
	if f.target.Snapshot().State != route.StateProtectionActive {
		t.Fatalf("route state=%s", f.target.Snapshot().State)
	}
}

func TestOrchestratorTerminateRollsBackPartialFailover(t *testing.T) {
	f := newFixture(t, 50*time.Millisecond)
	orchestrator := f.orchestrator(5 * time.Millisecond)

	plan := orchestrator.Start(context.Background(), f.target, "cable cut")
	if plan.Snapshot().State != PlanRiskHeld {
		t.Fatalf("state=%s", plan.Snapshot().State)
	}
	terminated, err := f.orchestrator(time.Second).Terminate(context.Background(), plan, f.target)
	if err != nil {
		t.Fatal(err)
	}
	if terminated.State != PlanTerminated {
		t.Fatalf("state=%s want %s", terminated.State, PlanTerminated)
	}
	if stageOf(t, terminated, StageConnectProtection).Status != StageSkipped {
		t.Fatal("pending connect stage should be skipped after terminate")
	}
	if f.target.Snapshot().State != route.StatePrimary {
		t.Fatalf("route state=%s", f.target.Snapshot().State)
	}
	topologySnapshot, err := f.registry.Snapshot("S3")
	if err != nil {
		t.Fatal(err)
	}
	if topologySnapshot.ActivePath != topology.PathPrimary || topologySnapshot.PrimaryOpen {
		t.Fatalf("unexpected topology after rollback: %+v", topologySnapshot)
	}
}

func TestOrchestratorTerminateOnProtectionIsSafe(t *testing.T) {
	f := newFixture(t, time.Millisecond)
	orchestrator := f.orchestrator(time.Second)

	plan := orchestrator.Start(context.Background(), f.target, "cable cut")
	if plan.Snapshot().State != PlanProtected {
		t.Fatalf("state=%s", plan.Snapshot().State)
	}
	terminated, err := orchestrator.Terminate(context.Background(), plan, f.target)
	if err != nil {
		t.Fatal(err)
	}
	if terminated.State != PlanTerminated {
		t.Fatalf("state=%s want %s", terminated.State, PlanTerminated)
	}
	if !strings.Contains(terminated.Message, "protection") {
		t.Fatalf("message=%q", terminated.Message)
	}
	if stageOf(t, terminated, StageDrainProtection).Status != StageSkipped {
		t.Fatal("restore stages should be skipped")
	}
	if f.target.Snapshot().State != route.StateProtectionActive {
		t.Fatalf("route state=%s, traffic must stay on protection", f.target.Snapshot().State)
	}
	if _, err := orchestrator.Terminate(context.Background(), plan, f.target); err == nil {
		t.Fatal("terminating a finalized plan must fail")
	}
	if _, err := orchestrator.Resume(context.Background(), plan, f.target); err == nil {
		t.Fatal("resuming a finalized plan must fail")
	}
}
