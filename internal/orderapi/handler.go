package orderapi

import (
	"context"
	"encoding/json"
	"log/slog"
	"net/http"
	"time"

	"github.com/google/uuid"
)

type OrderPublisher interface {
	Write(ctx context.Context, key string, data any) error
}

type Handler struct {
	log       *slog.Logger
	publisher OrderPublisher
}

func NewHandler(log *slog.Logger, publisher OrderPublisher) *Handler {
	return &Handler{
		log:       log,
		publisher: publisher,
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

	orderID := uuid.NewString()
	eventID := uuid.NewString()
	response := CreateOrderResponse{
		OrderID: orderID,
	}

	event := OrderCreatedEvent{
		EventID:   eventID,
		EventType: "order_created",
		Version:   1,
		OrderID:   orderID,
		UserID:    order.UserID,
		Items:     order.Items,
		Amount:    order.Amount,
		CreatedAt: time.Now().UTC(),
	}

	if err := h.publisher.Write(r.Context(), orderID, event); err != nil {
		h.log.Error("failed to publish order created event", "error", err, "order_id", orderID)
		writeError(w, "internal error", http.StatusInternalServerError)
		return
	}
	h.log.Info("published order created event", "order_id", orderID, "event_id", eventID)

	writeJSON(w, response, http.StatusCreated)
}

func (h *Handler) health(w http.ResponseWriter, _ *http.Request) {
	w.WriteHeader(200)
}
