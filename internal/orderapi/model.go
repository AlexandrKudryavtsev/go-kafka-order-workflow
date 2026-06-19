package orderapi

import (
	"errors"
	"fmt"
	"strings"

	"github.com/AlexandrKudryavtsev/go-kafka-order-workflow/internal/events"
)

type CreateOrderRequest struct {
	UserID string        `json:"userId"`
	Items  []events.Item `json:"items"`
	Amount int64         `json:"amount"`
}

type CreateOrderResponse struct {
	OrderID string `json:"orderId"`
}

type ErrorResponse struct {
	Message string `json:"message"`
}

func (o *CreateOrderRequest) Validate() error {
	// normalization
	o.UserID = strings.TrimSpace(o.UserID)
	items := make([]events.Item, 0, len(o.Items))
	for _, item := range o.Items {
		items = append(items, events.Item{
			SKU:      strings.TrimSpace(item.SKU),
			Quantity: item.Quantity,
		})
	}
	o.Items = items

	// validation
	if o.UserID == "" {
		return errors.New("invalid userId")
	}
	if o.Amount <= 0 {
		return errors.New("invalid amount")
	}
	if len(o.Items) == 0 {
		return errors.New("empty items")
	}
	for i, item := range o.Items {
		if item.SKU == "" {
			return fmt.Errorf("item %d: invalid sku", i)
		}
		if item.Quantity <= 0 {
			return fmt.Errorf("item %d: invalid quantity", i)
		}
	}

	return nil
}
