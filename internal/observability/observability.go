package observability

import "sync/atomic"

type Observability struct {
	serviceName string

	processedEventsTotal atomic.Uint64
	duplicateEventsTotal atomic.Uint64
	publishedEventsTotal atomic.Uint64
	dlqEventsTotal       atomic.Uint64
	handlerErrorsTotal   atomic.Uint64
}

type State struct {
	ServiceName string `json:"service"`

	ProcessedEventsTotal uint64 `json:"processed_events_total"`
	DuplicateEventsTotal uint64 `json:"duplicate_events_total"`
	PublishedEventsTotal uint64 `json:"published_events_total"`
	DLQEventsTotal       uint64 `json:"dlq_events_total"`
	HandlerErrorsTotal   uint64 `json:"handler_errors_total"`
}

func New(serviceName string) *Observability {
	return &Observability{
		serviceName: serviceName,
	}
}

func (o *Observability) MarkProcessed() {
	o.processedEventsTotal.Add(1)
}

func (o *Observability) MarkDuplicate() {
	o.duplicateEventsTotal.Add(1)
}

func (o *Observability) MarkPublished() {
	o.publishedEventsTotal.Add(1)
}

func (o *Observability) MarkDLQ() {
	o.dlqEventsTotal.Add(1)
}

func (o *Observability) MarkHandlerError() {
	o.handlerErrorsTotal.Add(1)
}

func (o *Observability) State() State {
	return State{
		ServiceName:          o.serviceName,
		ProcessedEventsTotal: o.processedEventsTotal.Load(),
		DuplicateEventsTotal: o.duplicateEventsTotal.Load(),
		PublishedEventsTotal: o.publishedEventsTotal.Load(),
		DLQEventsTotal:       o.dlqEventsTotal.Load(),
		HandlerErrorsTotal:   o.handlerErrorsTotal.Load(),
	}
}
