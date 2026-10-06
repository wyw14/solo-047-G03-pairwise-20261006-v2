package api

import (
	"net/http"

	"github.com/go-chi/chi/v5"

	"github.com/wyw14/cry-103/internal/orchestration"
)

func (s *Server) startSwitchOperation(writer http.ResponseWriter, request *http.Request) {
	target, err := s.services.Route(chi.URLParam(request, "id"))
	if err != nil {
		writeError(writer, http.StatusNotFound, err)
		return
	}
	var input SwitchOperationRequest
	if err := decodeJSON(request, &input); err != nil {
		writeError(writer, http.StatusBadRequest, err)
		return
	}
	result, err := s.services.Orchestrator.StartFailover(request.Context(), target)
	s.persistOperation(result)
	if err != nil {
		writeJSON(writer, http.StatusConflict, result)
		return
	}
	writeJSON(writer, http.StatusAccepted, result)
}

func (s *Server) listSwitchOperations(writer http.ResponseWriter, request *http.Request) {
	routeID := chi.URLParam(request, "id")
	if _, err := s.services.Route(routeID); err != nil {
		writeError(writer, http.StatusNotFound, err)
		return
	}
	writeJSON(writer, http.StatusOK, map[string]any{"items": s.services.Orchestrator.List(routeID)})
}

func (s *Server) getSwitchOperation(writer http.ResponseWriter, request *http.Request) {
	snapshot, ok := s.services.Orchestrator.Get(chi.URLParam(request, "id"))
	if !ok {
		writeError(writer, http.StatusNotFound, errOperationNotFound())
		return
	}
	writeJSON(writer, http.StatusOK, snapshot)
}

func (s *Server) resumeSwitchOperation(writer http.ResponseWriter, request *http.Request) {
	id := chi.URLParam(request, "id")
	if _, ok := s.services.Orchestrator.Get(id); !ok {
		writeError(writer, http.StatusNotFound, errOperationNotFound())
		return
	}
	result, err := s.services.Orchestrator.Resume(request.Context(), id)
	s.persistOperation(result)
	if err != nil {
		writeJSON(writer, http.StatusConflict, result)
		return
	}
	writeJSON(writer, http.StatusAccepted, result)
}

func (s *Server) abortSwitchOperation(writer http.ResponseWriter, request *http.Request) {
	id := chi.URLParam(request, "id")
	if _, ok := s.services.Orchestrator.Get(id); !ok {
		writeError(writer, http.StatusNotFound, errOperationNotFound())
		return
	}
	result, err := s.services.Orchestrator.Abort(id)
	s.persistOperation(result)
	if err != nil {
		writeJSON(writer, http.StatusConflict, result)
		return
	}
	writeJSON(writer, http.StatusOK, result)
}

func (s *Server) persistOperation(snapshot orchestration.Snapshot) {
	if snapshot.ID == "" {
		return
	}
	_ = s.services.Persist("switch-operation", snapshot.ID, snapshot)
}

func errOperationNotFound() error {
	return &operationNotFoundError{}
}

type operationNotFoundError struct{}

func (*operationNotFoundError) Error() string {
	return "switch operation not found"
}
