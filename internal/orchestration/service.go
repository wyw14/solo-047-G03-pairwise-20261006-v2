package orchestration

import (
	"context"
	"fmt"
	"sort"
	"sync"
	"time"

	"github.com/wyw14/cry-103/internal/platform"
	"github.com/wyw14/cry-103/internal/route"
	"github.com/wyw14/cry-103/internal/topology"
)

const (
	stageDrainPrimary        = "drain-primary"
	stageCrossConnectProtect = "cross-connect-protection"
	stageVerifyProtection    = "verify-protection"
	stageDrainProtection     = "drain-protection"
	stageOpenProtection      = "open-protection"
	stageCrossConnectPrimary = "cross-connect-primary"
	stageVerifyPrimary       = "verify-primary"
	stageTerminate           = "terminate"
)

type Timeouts struct {
	Drain   time.Duration
	Connect time.Duration
	Verify  time.Duration
}

type entry struct {
	operation *Operation
	target    *route.Route
}

// Service orchestrates primary/protection path switching as staged
// operations on top of the existing drainer, cross-connect activator and
// route state. Every stage records its timeout and the device facts
// observed, retains risks instead of hiding them, and records rollback
// actions when a stage fails after mutating the plant.
type Service struct {
	drainer   *route.Drainer
	activator *route.Activator
	topology  *topology.Registry
	ids       platform.IDGenerator
	now       func() time.Time
	timeouts  Timeouts

	mu      sync.RWMutex
	entries map[string]*entry
}

func NewService(drainer *route.Drainer, activator *route.Activator, registry *topology.Registry, ids platform.IDGenerator, now func() time.Time, timeouts Timeouts) *Service {
	if timeouts.Drain <= 0 {
		timeouts.Drain = drainer.Delay()
	}
	if timeouts.Connect <= 0 {
		timeouts.Connect = 10 * time.Millisecond
	}
	if timeouts.Verify <= 0 {
		timeouts.Verify = 5 * time.Millisecond
	}
	return &Service{
		drainer: drainer, activator: activator, topology: registry,
		ids: ids, now: now, timeouts: timeouts, entries: make(map[string]*entry),
	}
}

// StartFailover opens a new operation and runs the failover stages:
// drain the primary path, cross-connect to protection, then verify the
// device facts. An undrained primary retains a risk and blocks the
// cross-connect stage instead of allowing a misconnection.
func (s *Service) StartFailover(ctx context.Context, target *route.Route) (Snapshot, error) {
	routeSnapshot := target.Snapshot()
	operation := newOperation(s.ids.New("swop"), routeSnapshot.ID, routeSnapshot.SpanID, s.now())
	operation.appendStages(s.now(),
		Stage{Name: stageDrainPrimary, TimeoutMs: s.timeouts.Drain.Milliseconds()},
		Stage{Name: stageCrossConnectProtect, TimeoutMs: s.timeouts.Connect.Milliseconds()},
		Stage{Name: stageVerifyProtection, TimeoutMs: s.timeouts.Verify.Milliseconds()},
	)
	item := &entry{operation: operation, target: target}
	s.mu.Lock()
	s.entries[operation.snapshot.ID] = item
	s.mu.Unlock()
	err := s.run(ctx, item)
	return operation.Snapshot(), err
}

// Resume continues the same operation: a blocked operation retries the
// stages that were held back, and a protected operation proceeds to the
// restore stages that bring the recovered primary path back into service.
func (s *Service) Resume(ctx context.Context, id string) (Snapshot, error) {
	item, ok := s.find(id)
	if !ok {
		return Snapshot{}, fmt.Errorf("switch operation %s not found", id)
	}
	snapshot := item.operation.Snapshot()
	switch snapshot.State {
	case StateBlocked:
		index := firstIncomplete(snapshot.Stages)
		if index < 0 {
			return snapshot, fmt.Errorf("switch operation %s has no stage to resume", id)
		}
		item.operation.resetFrom(index, s.now())
	case StateProtected:
		if snapshot.Phase != PhaseFailover {
			return snapshot, fmt.Errorf("switch operation %s cannot continue from phase %s", id, snapshot.Phase)
		}
		item.operation.setPhase(PhaseRestore, s.now())
		item.operation.transition(StateRestoring, s.now())
		item.operation.appendStages(s.now(),
			Stage{Name: stageDrainProtection, TimeoutMs: s.timeouts.Drain.Milliseconds()},
			Stage{Name: stageOpenProtection, TimeoutMs: s.timeouts.Connect.Milliseconds()},
			Stage{Name: stageCrossConnectPrimary, TimeoutMs: s.timeouts.Connect.Milliseconds()},
			Stage{Name: stageVerifyPrimary, TimeoutMs: s.timeouts.Verify.Milliseconds()},
		)
	default:
		return snapshot, fmt.Errorf("switch operation %s cannot resume from state %s", id, snapshot.State)
	}
	err := s.run(ctx, item)
	return item.operation.Snapshot(), err
}

// Abort safely terminates an operation that is blocked or holding on
// protection: no further cross-connect actions are taken and the final
// device facts are recorded so the page shows where traffic was left.
func (s *Service) Abort(id string) (Snapshot, error) {
	item, ok := s.find(id)
	if !ok {
		return Snapshot{}, fmt.Errorf("switch operation %s not found", id)
	}
	snapshot := item.operation.Snapshot()
	if snapshot.State != StateBlocked && snapshot.State != StateProtected {
		return snapshot, fmt.Errorf("switch operation %s cannot safely terminate from state %s", id, snapshot.State)
	}
	now := s.now()
	item.operation.appendStages(now, Stage{
		Name: stageTerminate, Status: StageCompleted,
		Message:   "operation safely terminated; no further cross-connect actions",
		Facts:     s.facts(item.target),
		StartedAt: now, EndedAt: now,
	})
	item.operation.transition(StateTerminated, now)
	return item.operation.Snapshot(), nil
}

func (s *Service) Get(id string) (Snapshot, bool) {
	item, ok := s.find(id)
	if !ok {
		return Snapshot{}, false
	}
	return item.operation.Snapshot(), true
}

func (s *Service) List(routeID string) []Snapshot {
	s.mu.RLock()
	defer s.mu.RUnlock()
	var result []Snapshot
	for _, item := range s.entries {
		snapshot := item.operation.Snapshot()
		if routeID == "" || snapshot.RouteID == routeID {
			result = append(result, snapshot)
		}
	}
	sort.Slice(result, func(i, j int) bool {
		if result[i].CreatedAt.Equal(result[j].CreatedAt) {
			return result[i].ID < result[j].ID
		}
		return result[i].CreatedAt.Before(result[j].CreatedAt)
	})
	return result
}

func (s *Service) find(id string) (*entry, bool) {
	s.mu.RLock()
	defer s.mu.RUnlock()
	item, ok := s.entries[id]
	return item, ok
}

func (s *Service) facts(target *route.Route) *Facts {
	result := &Facts{Route: target.Snapshot()}
	if snapshot, err := s.topology.Snapshot(result.Route.SpanID); err == nil {
		result.Topology = snapshot
	}
	return result
}

func firstIncomplete(stages []Stage) int {
	for i := range stages {
		if stages[i].Status != StageCompleted {
			return i
		}
	}
	return -1
}

// run executes pending stages in order. A stage that retains a risk stops
// the run in the blocked state; a stage that fails after touching the plant
// rolls back and stops in the failed state.
func (s *Service) run(ctx context.Context, item *entry) error {
	for {
		snapshot := item.operation.Snapshot()
		index := -1
		for i := range snapshot.Stages {
			if snapshot.Stages[i].Status == StagePending {
				index = i
				break
			}
		}
		if index < 0 {
			if snapshot.Phase == PhaseFailover {
				item.operation.transition(StateProtected, s.now())
			} else {
				item.operation.transition(StateCompleted, s.now())
			}
			return nil
		}
		if err := ctx.Err(); err != nil {
			item.operation.finishStage(index, StageFailed, "stage interrupted: "+err.Error(), s.facts(item.target), s.now())
			item.operation.transition(StateFailed, s.now())
			return fmt.Errorf("switch operation %s: %w", snapshot.ID, err)
		}
		blocked, err := s.executeStage(ctx, item, index, snapshot.Stages[index].Name)
		if blocked || err != nil {
			return err
		}
	}
}

// executeStage runs one stage and reports whether the run must stop because
// a retained risk blocked the remaining stages.
func (s *Service) executeStage(ctx context.Context, item *entry, index int, name string) (bool, error) {
	switch name {
	case stageDrainPrimary:
		return s.stageDrain(ctx, item, index, topology.PathPrimary)
	case stageDrainProtection:
		return s.stageDrain(ctx, item, index, topology.PathProtection)
	case stageOpenProtection:
		return s.stageOpenProtection(item, index)
	case stageCrossConnectProtect:
		return s.stageCrossConnect(ctx, item, index, topology.PathProtection)
	case stageCrossConnectPrimary:
		return s.stageCrossConnect(ctx, item, index, topology.PathPrimary)
	case stageVerifyProtection:
		return s.stageVerify(item, index, topology.PathProtection)
	case stageVerifyPrimary:
		return s.stageVerify(item, index, topology.PathPrimary)
	default:
		return false, fmt.Errorf("unknown stage %q", name)
	}
}

func (s *Service) stageDrain(ctx context.Context, item *entry, index int, path topology.Path) (bool, error) {
	now := s.now()
	if item.operation.Snapshot().Phase == PhaseFailover {
		item.operation.transition(StateDraining, now)
	} else {
		item.operation.transition(StateRestoring, now)
	}
	item.operation.beginStage(index, now)
	drainCtx, cancel := context.WithTimeout(ctx, s.timeouts.Drain)
	_, drainErr := s.drainer.Drain(drainCtx, item.target, s.now())
	cancel()
	facts := s.facts(item.target)
	if drainErr != nil || facts.Route.TrafficBPS > 0 {
		message := fmt.Sprintf("%s path not fully drained (traffic_bps=%d)", path, facts.Route.TrafficBPS)
		if drainErr != nil {
			message = drainErr.Error() + "; " + message
		}
		now = s.now()
		item.operation.finishStage(index, StageFailed, message, facts, now)
		item.operation.addRisk(string(path)+"-not-drained", now)
		item.operation.blockFrom(index+1, "connection blocked: "+string(path)+" path not drained", now)
		item.operation.transition(StateBlocked, now)
		return true, nil
	}
	item.operation.finishStage(index, StageCompleted, string(path)+" path drained", facts, s.now())
	return false, nil
}

func (s *Service) stageOpenProtection(item *entry, index int) (bool, error) {
	now := s.now()
	item.operation.transition(StateRestoring, now)
	item.operation.beginStage(index, now)
	_, err := s.topology.Open(item.target.Snapshot().SpanID, topology.PathProtection, s.now())
	facts := s.facts(item.target)
	if err != nil {
		item.operation.finishStage(index, StageFailed, err.Error(), facts, s.now())
		item.operation.transition(StateFailed, s.now())
		return false, fmt.Errorf("open protection path: %w", err)
	}
	item.operation.finishStage(index, StageCompleted, "protection path opened", facts, s.now())
	return false, nil
}

func (s *Service) stageCrossConnect(ctx context.Context, item *entry, index int, target topology.Path) (bool, error) {
	now := s.now()
	if target == topology.PathProtection {
		item.operation.transition(StateSwitching, now)
	} else {
		item.operation.transition(StateRestoring, now)
	}
	item.operation.beginStage(index, now)
	var err error
	if target == topology.PathProtection {
		_, err = s.activator.Protection(ctx, item.target, s.now())
	} else {
		_, err = s.activator.Primary(ctx, item.target, s.now())
	}
	facts := s.facts(item.target)
	if err != nil {
		now = s.now()
		item.operation.finishStage(index, StageFailed, err.Error(), facts, now)
		s.rollback(ctx, item, index, target)
		item.operation.blockFrom(index+1, "skipped after cross-connect failure", s.now())
		item.operation.transition(StateFailed, s.now())
		return false, fmt.Errorf("cross-connect %s: %w", target, err)
	}
	item.operation.finishStage(index, StageCompleted, string(target)+" path connected and carrying traffic", facts, s.now())
	return false, nil
}

// rollback puts traffic back on the path that was carrying it before the
// failed cross-connect and records the attempt, success or failure.
func (s *Service) rollback(ctx context.Context, item *entry, index int, target topology.Path) {
	stageName := item.operation.Snapshot().Stages[index].Name
	var action string
	var err error
	if target == topology.PathProtection {
		action = "reconnect-primary"
		_, err = s.activator.Primary(ctx, item.target, s.now())
	} else {
		action = "reconnect-protection"
		_, err = s.activator.Protection(ctx, item.target, s.now())
	}
	record := RollbackRecord{Stage: stageName, Action: action, At: s.now()}
	if err != nil {
		record.Message = "rollback failed: " + err.Error()
	} else {
		record.Message = "traffic restored on previous path"
	}
	item.operation.addRollback(record)
}

func (s *Service) stageVerify(item *entry, index int, expected topology.Path) (bool, error) {
	item.operation.beginStage(index, s.now())
	facts := s.facts(item.target)
	expectedState := route.StateProtectionActive
	if expected == topology.PathPrimary {
		expectedState = route.StatePrimary
	}
	if facts.Route.State != expectedState || facts.Topology.ActivePath != expected {
		message := fmt.Sprintf("device facts do not confirm %s path (route=%s active_path=%s)", expected, facts.Route.State, facts.Topology.ActivePath)
		item.operation.finishStage(index, StageFailed, message, facts, s.now())
		item.operation.transition(StateFailed, s.now())
		return false, fmt.Errorf("verify %s path: %s", expected, message)
	}
	item.operation.finishStage(index, StageCompleted, "device facts confirm "+string(expected)+" path active", facts, s.now())
	return false, nil
}
