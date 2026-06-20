package events

import (
	"time"
)

const (
	EventTypeOrderCreated      = "order_created"
	EventTypeInventoryReserved = "inventory_reserved"
	EventTypeInventoryRejected = "inventory_rejected"
	EventTypePaymentSucceeded  = "payment_succeeded"
	EventTypePaymentFailed     = "payment_failed"
	EventTypeShipmentCreated   = "shipment_created"
	EventTypeDeadLetter        = "dead_letter_event"
)

type Item struct {
	SKU      string `json:"sku"`
	Quantity int    `json:"quantity"`
}

type DLQEvent struct {
	EventID       string `json:"eventId"`
	OriginalEvent string `json:"originalEvent"`

	Reason        string    `json:"reason"`
	Error         string    `json:"error"`
	SourceTopic   string    `json:"sourceTopic"`
	ConsumerGroup string    `json:"consumerGroup"`
	Attempts      int       `json:"attempts"`
	FailedAt      time.Time `json:"failedAt"`
}

type OrderCreatedEvent struct {
	EventID   string `json:"eventId"`
	EventType string `json:"eventType"`
	Version   int    `json:"version"`
	OrderID   string `json:"orderId"`

	UserID    string    `json:"userId"`
	Items     []Item    `json:"items"`
	Amount    int64     `json:"amount"`
	CreatedAt time.Time `json:"createdAt"`
}

type InventoryReservedEvent struct {
	EventID   string `json:"eventId"`
	EventType string `json:"eventType"`
	Version   int    `json:"version"`
	OrderID   string `json:"orderId"`

	Items      []Item    `json:"items"`
	ReservedAt time.Time `json:"reservedAt"`
}

type InventoryRejectedEvent struct {
	EventID   string `json:"eventId"`
	EventType string `json:"eventType"`
	Version   int    `json:"version"`
	OrderID   string `json:"orderId"`

	Reason     string    `json:"reason"`
	RejectedAt time.Time `json:"rejectedAt"`
}
