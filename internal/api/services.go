package api

import (
	"context"
	"fmt"
	"sort"
	"sync"
	"time"

	"github.com/wyw14/cry-103/internal/feed"
	"github.com/wyw14/cry-103/internal/incident"
	"github.com/wyw14/cry-103/internal/landing"
	"github.com/wyw14/cry-103/internal/otdr"
	"github.com/wyw14/cry-103/internal/platform"
	"github.com/wyw14/cry-103/internal/repair"
	"github.com/wyw14/cry-103/internal/repeater"
	"github.com/wyw14/cry-103/internal/route"
	"github.com/wyw14/cry-103/internal/span"
	"github.com/wyw14/cry-103/internal/store"
	"github.com/wyw14/cry-103/internal/switchover"
	"github.com/wyw14/cry-103/internal/telemetry"
	"github.com/wyw14/cry-103/internal/topology"
)

type Services struct {
	Clock               platform.Clock
	IDs                 platform.IDGenerator
	Journal             *store.Journal
	Spans               map[string]*span.Span
	Routes              map[string]*route.Route
	Repeaters           map[string]*repeater.Repeater
	Topology            *topology.Registry
	OTDR                *otdr.SessionService
	Switchovers         *switchover.SessionService
	Orchestrator        *switchover.Orchestrator
	Permits             *repair.PermitService
	Receipts            *landing.ReceiptRegistry
	Acceptance          *repair.AcceptanceService
	Telemetry           *telemetry.Ingestor
	TelemetryRepository *telemetry.Repository
	Admission           *incident.AdmissionService
	Classifier          *incident.Classifier
	Correlator          *otdr.Correlator
	Health              *landing.HealthService
	Station             *landing.Station
	WetPlantTests       *repair.TestService
	Ramps               *repeater.RampController
	Assets              *repair.Assets
	RepeaterHealth      *repeater.HealthProjector
	Actuator            *feed.Actuator
	Tests               map[string]*repair.TestSession
	Incidents           map[string]*incident.Incident
	SwitchPlans         map[string]*switchover.Plan
	mu                  sync.RWMutex
}

func (s *Services) Span(id string) (*span.Span, error) {
	s.mu.RLock()
	defer s.mu.RUnlock()
	target := s.Spans[id]
	if target == nil {
		return nil, fmt.Errorf("span %s not found", id)
	}
	return target, nil
}

func (s *Services) Route(id string) (*route.Route, error) {
	s.mu.RLock()
	defer s.mu.RUnlock()
	target := s.Routes[id]
	if target == nil {
		return nil, fmt.Errorf("route %s not found", id)
	}
	return target, nil
}

func (s *Services) Repeater(id string) (*repeater.Repeater, error) {
	s.mu.RLock()
	defer s.mu.RUnlock()
	target := s.Repeaters[id]
	if target == nil {
		return nil, fmt.Errorf("repeater %s not found", id)
	}
	return target, nil
}

func (s *Services) Persist(kind, entityID string, value any) error {
	return s.Journal.Append(kind, entityID, value, s.Clock.Now())
}

func (s *Services) StartWetPlantTest(ctx context.Context, spanID string) repair.TestSnapshot {
	s.mu.Lock()
	defer s.mu.Unlock()
	session := s.WetPlantTests.Start(ctx, spanID)
	snapshot := s.WetPlantTests.Snapshot(session)
	s.Tests[snapshot.ID] = session
	return snapshot
}

func (s *Services) FinishWetPlantTest(id, action string) (repair.TestSnapshot, error) {
	s.mu.Lock()
	session := s.Tests[id]
	delete(s.Tests, id)
	s.mu.Unlock()
	if session == nil {
		return repair.TestSnapshot{}, fmt.Errorf("wet-plant test %s not found", id)
	}
	if action == "complete" {
		return s.WetPlantTests.Complete(session), nil
	}
	return s.WetPlantTests.Cancel(session), nil
}

func (s *Services) StartSwitchPlan(ctx context.Context, target *route.Route, reason string) switchover.PlanSnapshot {
	plan := s.Orchestrator.Start(ctx, target, reason)
	s.mu.Lock()
	if s.SwitchPlans == nil {
		s.SwitchPlans = make(map[string]*switchover.Plan)
	}
	s.SwitchPlans[plan.Snapshot().ID] = plan
	s.mu.Unlock()
	return plan.Snapshot()
}

func (s *Services) SwitchPlan(id string) (*switchover.Plan, error) {
	s.mu.RLock()
	defer s.mu.RUnlock()
	plan := s.SwitchPlans[id]
	if plan == nil {
		return nil, fmt.Errorf("switch plan %s not found", id)
	}
	return plan, nil
}

func (s *Services) SwitchPlanList() []switchover.PlanSnapshot {
	s.mu.RLock()
	defer s.mu.RUnlock()
	return s.switchPlanListLocked()
}

// switchPlanListLocked must be called with s.mu held.
func (s *Services) switchPlanListLocked() []switchover.PlanSnapshot {
	plans := make([]*switchover.Plan, 0, len(s.SwitchPlans))
	for _, plan := range s.SwitchPlans {
		plans = append(plans, plan)
	}
	sort.Slice(plans, func(i, j int) bool {
		left, right := plans[i].Snapshot(), plans[j].Snapshot()
		if left.CreatedAt.Equal(right.CreatedAt) {
			return left.ID < right.ID
		}
		return left.CreatedAt.Before(right.CreatedAt)
	})
	result := make([]switchover.PlanSnapshot, 0, len(plans))
	for _, plan := range plans {
		result = append(result, plan.Snapshot())
	}
	return result
}

func (s *Services) Incident(id, spanID string) *incident.Incident {
	s.mu.Lock()
	defer s.mu.Unlock()
	current := s.Incidents[id]
	if current == nil {
		current = incident.New(id, spanID, s.Clock.Now().Add(10*time.Second), s.Clock.Now())
		s.Incidents[id] = current
	}
	return current
}

func (s *Services) TelemetryHistory(assetID string) []telemetry.Sample {
	items := s.TelemetryRepository.History(assetID)
	if latest, ok := s.TelemetryRepository.Latest(assetID, telemetry.KindTemperature); ok {
		_ = latest
	}
	return items
}
