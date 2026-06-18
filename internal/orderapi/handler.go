package orderapi

import (
	"encoding/json"
	"log/slog"
	"net/http"

	"github.com/google/uuid"
)

type Handler struct {
	log *slog.Logger
}

func NewHandler(log *slog.Logger) *Handler {
	return &Handler{
		log: log,
	}
}

func (h *Handler) Register(mux *http.ServeMux) {
	mux.HandleFunc("POST /orders", h.order)
	mux.HandleFunc("GET /health", h.health)
}

func writeJSON(w http.ResponseWriter, data any, status int) {
	w.Header().Set("Content-Type", "application/json")
	w.WriteHeader(status)

	json.NewEncoder(w).Encode(data)
}

func writeError(w http.ResponseWriter, message string, status int) {
	writeJSON(w, ErrorResponse{Message: message}, status)
}

func (h *Handler) order(w http.ResponseWriter, r *http.Request) {
	defer r.Body.Close()

	var order CreateOrderRequest

	if err := json.NewDecoder(r.Body).Decode(&order); err != nil {
		h.log.Info("failed to decode json", "error", err)
		writeError(w, "bad request", http.StatusBadRequest)
		return
	}

	if err := order.Validate(); err != nil {
		h.log.Info("invalid order", "error", err, "order", order)
		writeError(w, err.Error(), http.StatusBadRequest)
		return
	}

	id := uuid.NewString()
	response := CreateOrderResponse{
		OrderID: id,
	}

	// kafka producer

	writeJSON(w, response, http.StatusCreated)
}

func (h *Handler) health(w http.ResponseWriter, _ *http.Request) {
	w.WriteHeader(200)
}
