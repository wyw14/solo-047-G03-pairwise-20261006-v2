package api

import (
	"net/http"

	"github.com/go-chi/chi/v5"
)

func (s *Server) startSwitchPlan(writer http.ResponseWriter, request *http.Request) {
	target, err := s.services.Route(chi.URLParam(request, "id"))
	if err != nil {
		writeError(writer, http.StatusNotFound, err)
		return
	}
	var input SwitchPlanRequest
	if request.ContentLength > 0 {
		if err := decodeJSON(request, &input); err != nil {
			writeError(writer, http.StatusBadRequest, err)
			return
		}
	}
	result := s.services.StartSwitchPlan(request.Context(), target, input.Reason)
	if err := s.services.Persist("switch-plan", result.ID, result); err != nil {
		writeError(writer, http.StatusInternalServerError, err)
		return
	}
	writeJSON(writer, http.StatusAccepted, result)
}

func (s *Server) getSwitchPlan(writer http.ResponseWriter, request *http.Request) {
	plan, err := s.services.SwitchPlan(chi.URLParam(request, "id"))
	if err != nil {
		writeError(writer, http.StatusNotFound, err)
		return
	}
	writeJSON(writer, http.StatusOK, plan.Snapshot())
}

func (s *Server) listSwitchPlans(writer http.ResponseWriter, _ *http.Request) {
	writeJSON(writer, http.StatusOK, map[string]any{"items": s.services.SwitchPlanList()})
}

func (s *Server) resumeSwitchPlan(writer http.ResponseWriter, request *http.Request) {
	plan, err := s.services.SwitchPlan(chi.URLParam(request, "id"))
	if err != nil {
		writeError(writer, http.StatusNotFound, err)
		return
	}
	target, err := s.services.Route(plan.Snapshot().RouteID)
	if err != nil {
		writeError(writer, http.StatusNotFound, err)
		return
	}
	result, err := s.services.Orchestrator.Resume(request.Context(), plan, target)
	if err != nil {
		writeError(writer, http.StatusConflict, err)
		return
	}
	if err := s.services.Persist("switch-plan", result.ID, result); err != nil {
		writeError(writer, http.StatusInternalServerError, err)
		return
	}
	writeJSON(writer, http.StatusOK, result)
}

func (s *Server) terminateSwitchPlan(writer http.ResponseWriter, request *http.Request) {
	plan, err := s.services.SwitchPlan(chi.URLParam(request, "id"))
	if err != nil {
		writeError(writer, http.StatusNotFound, err)
		return
	}
	target, err := s.services.Route(plan.Snapshot().RouteID)
	if err != nil {
		writeError(writer, http.StatusNotFound, err)
		return
	}
	result, err := s.services.Orchestrator.Terminate(request.Context(), plan, target)
	if err != nil {
		writeError(writer, http.StatusConflict, err)
		return
	}
	if err := s.services.Persist("switch-plan", result.ID, result); err != nil {
		writeError(writer, http.StatusInternalServerError, err)
		return
	}
	writeJSON(writer, http.StatusOK, result)
}
