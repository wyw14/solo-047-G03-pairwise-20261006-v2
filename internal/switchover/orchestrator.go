package switchover

import (
	"context"
	"fmt"
	"sync"
	"time"

	"github.com/wyw14/cry-103/internal/platform"
	"github.com/wyw14/cry-103/internal/route"
	"github.com/wyw14/cry-103/internal/topology"
)

// Stage names in execution order: failover stages first, restore stages once
// the protection path has taken over and the primary path is ready again.
const (
	StageDrainPrimary      = "drain-primary"
	StageOpenPrimary       = "open-primary"
	StageConnectProtection = "connect-protection"
	StageVerifySwitch      = "verify-switch"
	StageDrainProtection   = "drain-protection"
	StageOpenProtection    = "open-protection"
	StageConnectPrimary    = "connect-primary"
	StageVerifyRestore     = "verify-restore"
)

var (
	failoverStages = []string{StageDrainPrimary, StageOpenPrimary, StageConnectProtection, StageVerifySwitch}
	restoreStages  = []string{StageDrainProtection, StageOpenProtection, StageConnectPrimary, StageVerifyRestore}
)

type StageStatus string

const (
	StagePending    StageStatus = "pending"
	StageRunning    StageStatus = "running"
	StageSucceeded  StageStatus = "succeeded"
	StageFailed     StageStatus = "failed"
	StageBlocked    StageStatus = "blocked"
	StageSkipped    StageStatus = "skipped"
	StageRolledBack StageStatus = "rolled-back"
)

// DeviceFacts captures the observed device state at the end of a stage so the
// page can render what the plant actually looked like at that point.
type DeviceFacts struct {
	SpanID         string        `json:"span_id"`
	Generation     uint64        `json:"generation"`
	ActivePath     topology.Path `json:"active_path"`
	PrimaryOpen    bool          `json:"primary_open"`
	ProtectionOpen bool          `json:"protection_open"`
	RouteState     route.State   `json:"route_state"`
	TrafficBPS     int64         `json:"traffic_bps"`
	ObservedAt     time.Time     `json:"observed_at"`
}

type Stage struct {
	Name       string       `json:"name"`
	Status     StageStatus  `json:"status"`
	TimeoutMS  int64        `json:"timeout_ms"`
	Rollback   string       `json:"rollback,omitempty"`
	Message    string       `json:"message,omitempty"`
	StartedAt  time.Time    `json:"started_at,omitempty"`
	FinishedAt time.Time    `json:"finished_at,omitempty"`
	Facts      *DeviceFacts `json:"facts,omitempty"`
}

type PlanState string

const (
	PlanCreated    PlanState = "created"
	PlanRunning    PlanState = "running"
	PlanRiskHeld   PlanState = "risk-held"
	PlanProtected  PlanState = "protected"
	PlanRestoring  PlanState = "restoring"
	PlanCompleted  PlanState = "completed"
	PlanTerminated PlanState = "terminated"
	PlanFailed     PlanState = "failed"
)

type PlanSnapshot struct {
	ID        string    `json:"id"`
	RouteID   string    `json:"route_id"`
	SpanID    string    `json:"span_id"`
	Reason    string    `json:"reason,omitempty"`
	State     PlanState `json:"state"`
	Message   string    `json:"message,omitempty"`
	Stages    []Stage   `json:"stages"`
	Risks     []string  `json:"risks"`
	CreatedAt time.Time `json:"created_at"`
	UpdatedAt time.Time `json:"updated_at"`
}

type Plan struct {
	mu       sync.RWMutex
	snapshot PlanSnapshot
}

func newPlan(id, routeID, spanID, reason string, drainTimeout time.Duration, at time.Time) *Plan {
	names := append(append([]string{}, failoverStages...), restoreStages...)
	stages := make([]Stage, 0, len(names))
	for _, name := range names {
		stages = append(stages, Stage{
			Name:      name,
			Status:    StagePending,
			TimeoutMS: stageTimeout(name, drainTimeout).Milliseconds(),
			Rollback:  rollbackHint(name),
		})
	}
	return &Plan{snapshot: PlanSnapshot{
		ID: id, RouteID: routeID, SpanID: spanID, Reason: reason,
		State: PlanCreated, Stages: stages, Risks: []string{},
		CreatedAt: at.UTC(), UpdatedAt: at.UTC(),
	}}
}

func stageTimeout(stage string, drainTimeout time.Duration) time.Duration {
	if stage == StageDrainPrimary || stage == StageDrainProtection {
		return drainTimeout
	}
	return 0
}

func rollbackHint(stage string) string {
	switch stage {
	case StageDrainPrimary:
		return "resume traffic on the primary path"
	case StageOpenPrimary:
		return "reconnect the primary path"
	case StageConnectProtection:
		return "open the protection path and reconnect primary"
	case StageDrainProtection:
		return "resume traffic on the protection path"
	case StageOpenProtection:
		return "reconnect the protection path"
	case StageConnectPrimary:
		return "open the primary path and reconnect protection"
	default:
		return ""
	}
}

func (p *Plan) Snapshot() PlanSnapshot {
	p.mu.RLock()
	defer p.mu.RUnlock()
	result := p.snapshot
	result.Stages = append([]Stage(nil), result.Stages...)
	result.Risks = append([]string(nil), result.Risks...)
	return result
}

func (p *Plan) setState(state PlanState, message string, at time.Time) {
	p.mu.Lock()
	defer p.mu.Unlock()
	p.snapshot.State = state
	p.snapshot.Message = message
	p.snapshot.UpdatedAt = at.UTC()
}

func (p *Plan) addRisk(risk string, at time.Time) {
	p.mu.Lock()
	defer p.mu.Unlock()
	p.snapshot.Risks = append(p.snapshot.Risks, risk)
	p.snapshot.UpdatedAt = at.UTC()
}

func (p *Plan) beginStage(name string, at time.Time) {
	p.mu.Lock()
	defer p.mu.Unlock()
	for i := range p.snapshot.Stages {
		if p.snapshot.Stages[i].Name == name {
			p.snapshot.Stages[i].Status = StageRunning
			p.snapshot.Stages[i].StartedAt = at.UTC()
			p.snapshot.Stages[i].FinishedAt = time.Time{}
			p.snapshot.Stages[i].Message = ""
			p.snapshot.Stages[i].Facts = nil
		}
	}
	p.snapshot.UpdatedAt = at.UTC()
}

func (p *Plan) finishStage(name string, status StageStatus, message string, facts DeviceFacts, at time.Time) {
	p.mu.Lock()
	defer p.mu.Unlock()
	for i := range p.snapshot.Stages {
		if p.snapshot.Stages[i].Name == name {
			p.snapshot.Stages[i].Status = status
			p.snapshot.Stages[i].Message = message
			p.snapshot.Stages[i].FinishedAt = at.UTC()
			observed := facts
			p.snapshot.Stages[i].Facts = &observed
		}
	}
	p.snapshot.UpdatedAt = at.UTC()
}

func (p *Plan) skipRemaining(message string, at time.Time) {
	p.mu.Lock()
	defer p.mu.Unlock()
	for i := range p.snapshot.Stages {
		if p.snapshot.Stages[i].Status == StagePending {
			p.snapshot.Stages[i].Status = StageSkipped
			p.snapshot.Stages[i].Message = message
			p.snapshot.Stages[i].FinishedAt = at.UTC()
		}
	}
	p.snapshot.UpdatedAt = at.UTC()
}

func (p *Plan) markRolledBack(message string, facts DeviceFacts, at time.Time) {
	p.mu.Lock()
	defer p.mu.Unlock()
	for i := range p.snapshot.Stages {
		stage := &p.snapshot.Stages[i]
		switch stage.Status {
		case StageSucceeded:
			stage.Status = StageRolledBack
			stage.Message = message
			observed := facts
			stage.Facts = &observed
			stage.FinishedAt = at.UTC()
		case StagePending, StageBlocked, StageFailed, StageRunning:
			stage.Status = StageSkipped
			stage.Message = message
			stage.FinishedAt = at.UTC()
		}
	}
	p.snapshot.UpdatedAt = at.UTC()
}

func (p *Plan) stageSucceeded(name string) bool {
	p.mu.RLock()
	defer p.mu.RUnlock()
	for _, stage := range p.snapshot.Stages {
		if stage.Name == name {
			return stage.Status == StageSucceeded
		}
	}
	return false
}

func (p *Plan) failoverComplete() bool {
	for _, name := range failoverStages {
		if !p.stageSucceeded(name) {
			return false
		}
	}
	return true
}

// Orchestrator drives a primary/protection switch as a staged plan built on
// the existing drainer, cross-connect registry and route state. Every stage
// records its timeout, rollback hint and the device facts observed when it
// finished, so an operation can be resumed or safely terminated later.
type Orchestrator struct {
	drainer      *route.Drainer
	activator    *route.Activator
	topology     *topology.Registry
	ids          platform.IDGenerator
	now          func() time.Time
	drainTimeout time.Duration
}

func NewOrchestrator(drainer *route.Drainer, activator *route.Activator, registry *topology.Registry, ids platform.IDGenerator, now func() time.Time, drainTimeout time.Duration) *Orchestrator {
	return &Orchestrator{drainer: drainer, activator: activator, topology: registry, ids: ids, now: now, drainTimeout: drainTimeout}
}

// Start creates a switch plan for the route and runs the failover stages.
func (o *Orchestrator) Start(ctx context.Context, target *route.Route, reason string) *Plan {
	snapshot := target.Snapshot()
	plan := newPlan(o.ids.New("plan"), snapshot.ID, snapshot.SpanID, reason, o.drainTimeout, o.now())
	plan.setState(PlanRunning, "failing over to the protection path", o.now())
	o.runFailover(ctx, plan, target)
	return plan
}

// Resume continues an existing plan: it retries an interrupted failover, or
// restores the primary path once the protection path has recovered.
func (o *Orchestrator) Resume(ctx context.Context, plan *Plan, target *route.Route) (PlanSnapshot, error) {
	snapshot := plan.Snapshot()
	switch snapshot.State {
	case PlanRiskHeld, PlanFailed:
		if plan.failoverComplete() {
			plan.setState(PlanRestoring, "restoring the recovered primary path", o.now())
			o.runRestore(ctx, plan, target)
		} else {
			plan.setState(PlanRunning, "resuming failover to the protection path", o.now())
			o.runFailover(ctx, plan, target)
		}
	case PlanProtected:
		plan.setState(PlanRestoring, "restoring the recovered primary path", o.now())
		o.runRestore(ctx, plan, target)
	default:
		return plan.Snapshot(), fmt.Errorf("switch plan %s cannot resume from state %s", snapshot.ID, snapshot.State)
	}
	return plan.Snapshot(), nil
}

// Terminate ends the plan safely: traffic already on the protection path is
// left there, otherwise completed stages are rolled back to the primary path.
func (o *Orchestrator) Terminate(ctx context.Context, plan *Plan, target *route.Route) (PlanSnapshot, error) {
	snapshot := plan.Snapshot()
	switch snapshot.State {
	case PlanProtected:
		plan.skipRemaining("operation terminated; traffic remains on the protection path", o.now())
		plan.setState(PlanTerminated, "terminated safely on the protection path", o.now())
		return plan.Snapshot(), nil
	case PlanCreated, PlanRunning, PlanRestoring, PlanRiskHeld, PlanFailed:
		if err := o.rollback(ctx, plan, target); err != nil {
			plan.setState(PlanFailed, err.Error(), o.now())
			return plan.Snapshot(), err
		}
		plan.setState(PlanTerminated, "rolled back to the primary path and terminated", o.now())
		return plan.Snapshot(), nil
	default:
		return plan.Snapshot(), fmt.Errorf("switch plan %s is already finalized in state %s", snapshot.ID, snapshot.State)
	}
}

func (o *Orchestrator) runFailover(ctx context.Context, plan *Plan, target *route.Route) {
	if !plan.stageSucceeded(StageDrainPrimary) && !o.drainStage(ctx, plan, target, StageDrainPrimary, "primary") {
		return
	}
	if !plan.stageSucceeded(StageOpenPrimary) && !o.openStage(plan, target, StageOpenPrimary, topology.PathPrimary) {
		return
	}
	if !plan.stageSucceeded(StageConnectProtection) && !o.connectProtectionStage(ctx, plan, target) {
		return
	}
	if !plan.stageSucceeded(StageVerifySwitch) && !o.verifyStage(plan, target, StageVerifySwitch, topology.PathProtection, route.StateProtectionActive) {
		return
	}
	plan.setState(PlanProtected, "protection path carries traffic; resume to restore primary or terminate", o.now())
}

func (o *Orchestrator) runRestore(ctx context.Context, plan *Plan, target *route.Route) {
	if !plan.stageSucceeded(StageDrainProtection) && !o.drainStage(ctx, plan, target, StageDrainProtection, "protection") {
		return
	}
	if !plan.stageSucceeded(StageOpenProtection) && !o.openStage(plan, target, StageOpenProtection, topology.PathProtection) {
		return
	}
	if !plan.stageSucceeded(StageConnectPrimary) && !o.connectPrimaryStage(ctx, plan, target) {
		return
	}
	if !plan.stageSucceeded(StageVerifyRestore) && !o.verifyStage(plan, target, StageVerifyRestore, topology.PathPrimary, route.StatePrimary) {
		return
	}
	plan.setState(PlanCompleted, "primary path restored; switch plan complete", o.now())
}

func (o *Orchestrator) drainStage(ctx context.Context, plan *Plan, target *route.Route, stage, label string) bool {
	plan.beginStage(stage, o.now())
	timeout := o.drainTimeout
	if timeout <= 0 {
		timeout = o.drainer.Delay()
	}
	drainCtx, cancel := context.WithTimeout(ctx, timeout)
	_, err := o.drainer.Drain(drainCtx, target, o.now())
	cancel()
	facts := o.facts(target)
	if err != nil {
		risk := fmt.Sprintf("%s path not drained within %s: %d bps still at risk", label, timeout, facts.TrafficBPS)
		plan.addRisk(risk, o.now())
		plan.finishStage(stage, StageBlocked, err.Error()+"; cross-connect blocked until the path drains", facts, o.now())
		plan.setState(PlanRiskHeld, risk, o.now())
		return false
	}
	if facts.TrafficBPS > 0 {
		risk := fmt.Sprintf("%s path still carries %d bps; refusing to cross-connect", label, facts.TrafficBPS)
		plan.addRisk(risk, o.now())
		plan.finishStage(stage, StageBlocked, risk, facts, o.now())
		plan.setState(PlanRiskHeld, risk, o.now())
		return false
	}
	plan.finishStage(stage, StageSucceeded, label+" path drained", facts, o.now())
	return true
}

func (o *Orchestrator) openStage(plan *Plan, target *route.Route, stage string, path topology.Path) bool {
	plan.beginStage(stage, o.now())
	if _, err := o.topology.Open(target.Snapshot().SpanID, path, o.now()); err != nil {
		plan.finishStage(stage, StageFailed, err.Error(), o.facts(target), o.now())
		plan.setState(PlanFailed, "open "+string(path)+" failed: "+err.Error(), o.now())
		return false
	}
	plan.finishStage(stage, StageSucceeded, string(path)+" path opened", o.facts(target), o.now())
	return true
}

func (o *Orchestrator) connectProtectionStage(ctx context.Context, plan *Plan, target *route.Route) bool {
	plan.beginStage(StageConnectProtection, o.now())
	facts := o.facts(target)
	if facts.TrafficBPS > 0 || !facts.PrimaryOpen {
		risk := fmt.Sprintf("refusing to connect protection: primary_open=%t traffic_bps=%d", facts.PrimaryOpen, facts.TrafficBPS)
		plan.addRisk(risk, o.now())
		plan.finishStage(StageConnectProtection, StageBlocked, risk, facts, o.now())
		plan.setState(PlanRiskHeld, risk, o.now())
		return false
	}
	if _, err := o.activator.ConnectProtection(ctx, target, o.now()); err != nil {
		plan.finishStage(StageConnectProtection, StageFailed, err.Error(), o.facts(target), o.now())
		plan.setState(PlanFailed, "connect protection failed: "+err.Error(), o.now())
		return false
	}
	plan.finishStage(StageConnectProtection, StageSucceeded, "protection path connected and carrying traffic", o.facts(target), o.now())
	return true
}

func (o *Orchestrator) connectPrimaryStage(ctx context.Context, plan *Plan, target *route.Route) bool {
	plan.beginStage(StageConnectPrimary, o.now())
	facts := o.facts(target)
	if facts.TrafficBPS > 0 || !facts.ProtectionOpen {
		risk := fmt.Sprintf("refusing to connect primary: protection_open=%t traffic_bps=%d", facts.ProtectionOpen, facts.TrafficBPS)
		plan.addRisk(risk, o.now())
		plan.finishStage(StageConnectPrimary, StageBlocked, risk, facts, o.now())
		plan.setState(PlanRiskHeld, risk, o.now())
		return false
	}
	if _, err := o.activator.Primary(ctx, target, o.now()); err != nil {
		plan.finishStage(StageConnectPrimary, StageFailed, err.Error(), o.facts(target), o.now())
		plan.setState(PlanFailed, "connect primary failed: "+err.Error(), o.now())
		return false
	}
	plan.finishStage(StageConnectPrimary, StageSucceeded, "primary path connected and carrying traffic", o.facts(target), o.now())
	return true
}

func (o *Orchestrator) verifyStage(plan *Plan, target *route.Route, stage string, path topology.Path, state route.State) bool {
	plan.beginStage(stage, o.now())
	facts := o.facts(target)
	if facts.ActivePath != path || facts.RouteState != state {
		message := fmt.Sprintf("verification failed: active_path=%s route_state=%s, want %s/%s", facts.ActivePath, facts.RouteState, path, state)
		plan.finishStage(stage, StageFailed, message, facts, o.now())
		plan.setState(PlanFailed, message, o.now())
		return false
	}
	plan.finishStage(stage, StageSucceeded, fmt.Sprintf("device facts confirm %s path active", path), facts, o.now())
	return true
}

// rollback undoes completed failover stages so the route carries traffic on
// the primary path again before the plan is terminated.
func (o *Orchestrator) rollback(ctx context.Context, plan *Plan, target *route.Route) error {
	facts := o.facts(target)
	if facts.ActivePath == topology.PathProtection {
		if _, err := o.topology.Open(target.Snapshot().SpanID, topology.PathProtection, o.now()); err != nil {
			return fmt.Errorf("rollback open protection: %w", err)
		}
	}
	if facts.PrimaryOpen || facts.ActivePath != topology.PathPrimary || facts.RouteState != route.StatePrimary {
		if _, err := o.activator.Primary(ctx, target, o.now()); err != nil {
			return fmt.Errorf("rollback connect primary: %w", err)
		}
	}
	plan.markRolledBack("rolled back to the primary path", o.facts(target), o.now())
	return nil
}

func (o *Orchestrator) facts(target *route.Route) DeviceFacts {
	snapshot := target.Snapshot()
	facts := DeviceFacts{
		SpanID: snapshot.SpanID, RouteState: snapshot.State,
		TrafficBPS: snapshot.TrafficBPS, ObservedAt: o.now().UTC(),
	}
	if topologySnapshot, err := o.topology.Snapshot(snapshot.SpanID); err == nil {
		facts.Generation = topologySnapshot.Generation
		facts.ActivePath = topologySnapshot.ActivePath
		facts.PrimaryOpen = topologySnapshot.PrimaryOpen
		facts.ProtectionOpen = topologySnapshot.ProtectOpen
	}
	return facts
}
