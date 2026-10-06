package orchestration

import (
	"sync"
	"time"

	"github.com/wyw14/cry-103/internal/route"
	"github.com/wyw14/cry-103/internal/topology"
)

type State string

const (
	StateDraining   State = "draining"
	StateSwitching  State = "switching"
	StateProtected  State = "protected"
	StateRestoring  State = "restoring"
	StateCompleted  State = "completed"
	StateBlocked    State = "blocked"
	StateFailed     State = "failed"
	StateTerminated State = "terminated"
)

const (
	PhaseFailover = "failover"
	PhaseRestore  = "restore"
)

type StageStatus string

const (
	StagePending   StageStatus = "pending"
	StageRunning   StageStatus = "running"
	StageCompleted StageStatus = "completed"
	StageFailed    StageStatus = "failed"
	StageBlocked   StageStatus = "blocked"
)

// Facts captures the device truth (route and cross-connect state) observed
// at the end of a stage, so the page can render what the plant actually did.
type Facts struct {
	Route    route.Snapshot    `json:"route"`
	Topology topology.Snapshot `json:"topology"`
}

type Stage struct {
	Name      string      `json:"name"`
	Status    StageStatus `json:"status"`
	TimeoutMs int64       `json:"timeout_ms"`
	Message   string      `json:"message,omitempty"`
	Facts     *Facts      `json:"facts,omitempty"`
	StartedAt time.Time   `json:"started_at,omitempty"`
	EndedAt   time.Time   `json:"ended_at,omitempty"`
}

type RollbackRecord struct {
	Stage   string    `json:"stage"`
	Action  string    `json:"action"`
	Message string    `json:"message"`
	At      time.Time `json:"at"`
}

type Snapshot struct {
	ID        string           `json:"id"`
	RouteID   string           `json:"route_id"`
	SpanID    string           `json:"span_id"`
	Phase     string           `json:"phase"`
	State     State            `json:"state"`
	Risks     []string         `json:"risks"`
	Stages    []Stage          `json:"stages"`
	Rollback  []RollbackRecord `json:"rollback"`
	CreatedAt time.Time        `json:"created_at"`
	UpdatedAt time.Time        `json:"updated_at"`
}

type Operation struct {
	mu       sync.RWMutex
	snapshot Snapshot
}

func newOperation(id, routeID, spanID string, at time.Time) *Operation {
	return &Operation{snapshot: Snapshot{
		ID: id, RouteID: routeID, SpanID: spanID, Phase: PhaseFailover,
		State: StateDraining, CreatedAt: at.UTC(), UpdatedAt: at.UTC(),
	}}
}

func (o *Operation) Snapshot() Snapshot {
	o.mu.RLock()
	defer o.mu.RUnlock()
	return copySnapshot(o.snapshot)
}

func copySnapshot(snapshot Snapshot) Snapshot {
	snapshot.Risks = append([]string(nil), snapshot.Risks...)
	snapshot.Stages = append([]Stage(nil), snapshot.Stages...)
	snapshot.Rollback = append([]RollbackRecord(nil), snapshot.Rollback...)
	return snapshot
}

func (o *Operation) transition(state State, at time.Time) {
	o.mu.Lock()
	defer o.mu.Unlock()
	o.snapshot.State = state
	o.snapshot.UpdatedAt = at.UTC()
}

func (o *Operation) setPhase(phase string, at time.Time) {
	o.mu.Lock()
	defer o.mu.Unlock()
	o.snapshot.Phase = phase
	o.snapshot.UpdatedAt = at.UTC()
}

func (o *Operation) appendStages(at time.Time, stages ...Stage) {
	o.mu.Lock()
	defer o.mu.Unlock()
	for _, stage := range stages {
		if stage.Status == "" {
			stage.Status = StagePending
		}
		o.snapshot.Stages = append(o.snapshot.Stages, stage)
	}
	o.snapshot.UpdatedAt = at.UTC()
}

func (o *Operation) beginStage(index int, at time.Time) {
	o.mu.Lock()
	defer o.mu.Unlock()
	stage := &o.snapshot.Stages[index]
	stage.Status = StageRunning
	stage.StartedAt = at.UTC()
	o.snapshot.UpdatedAt = at.UTC()
}

func (o *Operation) finishStage(index int, status StageStatus, message string, facts *Facts, at time.Time) {
	o.mu.Lock()
	defer o.mu.Unlock()
	stage := &o.snapshot.Stages[index]
	stage.Status = status
	stage.Message = message
	stage.Facts = facts
	stage.EndedAt = at.UTC()
	o.snapshot.UpdatedAt = at.UTC()
}

// blockFrom marks every pending stage from index onward as blocked so a
// retained risk (for example an undrained primary) cannot be silently
// followed by a cross-connect.
func (o *Operation) blockFrom(index int, message string, at time.Time) {
	o.mu.Lock()
	defer o.mu.Unlock()
	for i := index; i < len(o.snapshot.Stages); i++ {
		if o.snapshot.Stages[i].Status == StagePending {
			o.snapshot.Stages[i].Status = StageBlocked
			o.snapshot.Stages[i].Message = message
		}
	}
	o.snapshot.UpdatedAt = at.UTC()
}

// resetFrom returns stages from index onward to pending so the same
// operation can be resumed after a blocked stage is cleared.
func (o *Operation) resetFrom(index int, at time.Time) {
	o.mu.Lock()
	defer o.mu.Unlock()
	for i := index; i < len(o.snapshot.Stages); i++ {
		stage := &o.snapshot.Stages[i]
		if stage.Status == StageCompleted {
			continue
		}
		stage.Status = StagePending
		stage.Message = ""
		stage.Facts = nil
		stage.StartedAt = time.Time{}
		stage.EndedAt = time.Time{}
	}
	o.snapshot.UpdatedAt = at.UTC()
}

// addRisk retains a risk on the operation; risks are never cleared so the
// page keeps showing that the risk was taken, even after a later resume.
func (o *Operation) addRisk(risk string, at time.Time) {
	o.mu.Lock()
	defer o.mu.Unlock()
	for _, existing := range o.snapshot.Risks {
		if existing == risk {
			return
		}
	}
	o.snapshot.Risks = append(o.snapshot.Risks, risk)
	o.snapshot.UpdatedAt = at.UTC()
}

func (o *Operation) addRollback(record RollbackRecord) {
	o.mu.Lock()
	defer o.mu.Unlock()
	o.snapshot.Rollback = append(o.snapshot.Rollback, record)
	o.snapshot.UpdatedAt = record.At
}
