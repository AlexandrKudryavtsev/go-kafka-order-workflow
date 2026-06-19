package event

import "time"

type Item struct {
	SKU      string `json:"sku"`
	Quantity int    `json:"quantity"`
}

type OrderCreatedEvent struct {
	EventID   string    `json:"eventId"`
	EventType string    `json:"eventType"`
	Version   int       `json:"version"`
	OrderID   string    `json:"orderId"`
	UserID    string    `json:"userId"`
	Items     []Item    `json:"items"`
	Amount    int64     `json:"amount"`
	CreatedAt time.Time `json:"createdAt"`
}

const (
	EventTypeOrderCreated      = "order_created"
	EventTypeInventoryReserved = "inventory_reserved"
	EventTypeInventoryRejected = "inventory_rejected"
)

type InventoryReservedEvent struct {
	EventID    string    `json:"eventId"`
	EventType  string    `json:"eventType"`
	Version    int       `json:"version"`
	OrderID    string    `json:"orderId"`
	Items      []Item    `json:"items"`
	ReservedAt time.Time `json:"reservedAt"`
}

type InventoryRejectedEvent struct {
	EventID    string    `json:"eventId"`
	EventType  string    `json:"eventType"`
	Version    int       `json:"version"`
	OrderID    string    `json:"orderId"`
	Reason     string    `json:"reason"`
	RejectedAt time.Time `json:"rejectedAt"`
}
