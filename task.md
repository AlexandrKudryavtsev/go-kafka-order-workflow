# Техническое задание

## Проект

Inventory Reservation System на Go с использованием Kafka.

## Цель проекта

Прокачать event-driven архитектуру поверх Kafka:

- несколько topic’ов;
- producer + consumer в одном сервисе;
- event chaining;
- consumer groups;
- retry;
- DLQ;
- idempotency;
- stateful services;
- graceful shutdown;
- structured logging.

Тесты пишутся **в конце проекта**, после реализации основного функционала.

## Архитектура

```text
Client
  |
  v
Order API
  |
  | publishes
  v
Kafka topic: order-events
  |
  v
Inventory Service
  |
  | publishes
  v
Kafka topic: inventory-events
  |
  v
Payment Service
  |
  | publishes
  v
Kafka topic: payment-events
  |
  v
Shipping Service
  |
  | publishes
  v
Kafka topic: shipping-events
```

## Основной сценарий

Клиент создаёт заказ:

```http
POST /orders
```

Request:

```json
{
  "userId": "42",
  "items": [
    {
      "sku": "book-1",
      "quantity": 2
    }
  ],
  "amount": 1500
}
```

Order API публикует событие:

```json
{
  "eventId": "uuid",
  "eventType": "order_created",
  "version": 1,
  "orderId": "uuid",
  "userId": "42",
  "items": [
    {
      "sku": "book-1",
      "quantity": 2
    }
  ],
  "amount": 1500,
  "createdAt": "2026-06-18T12:00:00Z"
}
```

## Сервисы

### 1. Order API

Endpoint:

```http
POST /orders
```

Должен:

- валидировать request;
- генерировать `orderId`;
- генерировать `eventId`;
- публиковать `order_created` в topic `order-events`;
- возвращать `orderId`.

Response:

```json
{
  "orderId": "uuid"
}
```

### 2. Inventory Service

Consumer group:

```text
inventory-service
```

Читает:

```text
order-events
```

Обрабатывает:

```text
order_created
```

Если товаров хватает, публикует в `inventory-events`:

```json
{
  "eventId": "uuid",
  "eventType": "inventory_reserved",
  "version": 1,
  "orderId": "uuid",
  "items": [
    {
      "sku": "book-1",
      "quantity": 2
    }
  ],
  "reservedAt": "2026-06-18T12:00:01Z"
}
```

Если товаров не хватает:

```json
{
  "eventId": "uuid",
  "eventType": "inventory_rejected",
  "version": 1,
  "orderId": "uuid",
  "reason": "not_enough_stock",
  "rejectedAt": "2026-06-18T12:00:01Z"
}
```

### 3. Payment Service

Consumer group:

```text
payment-service
```

Читает:

```text
inventory-events
```

Обрабатывает:

```text
inventory_reserved
```

При успехе публикует в `payment-events`:

```json
{
  "eventId": "uuid",
  "eventType": "payment_succeeded",
  "version": 1,
  "orderId": "uuid",
  "amount": 1500,
  "paidAt": "2026-06-18T12:00:02Z"
}
```

При ошибке:

```json
{
  "eventId": "uuid",
  "eventType": "payment_failed",
  "version": 1,
  "orderId": "uuid",
  "reason": "payment_declined",
  "failedAt": "2026-06-18T12:00:02Z"
}
```

### 4. Shipping Service

Consumer group:

```text
shipping-service
```

Читает:

```text
payment-events
```

Обрабатывает:

```text
payment_succeeded
```

Публикует в `shipping-events`:

```json
{
  "eventId": "uuid",
  "eventType": "shipment_created",
  "version": 1,
  "orderId": "uuid",
  "shipmentId": "uuid",
  "createdAt": "2026-06-18T12:00:03Z"
}
```

## Kafka Topics

Создать topics:

```text
order-events
inventory-events
payment-events
shipping-events
dead-letter-events
```

Для локальной разработки:

```text
partitions: 3
replicationFactor: 1
```

## Retry

Каждый consumer должен:

- делать retry при ошибке бизнес-обработки;
- использовать `maxRetries`;
- логировать каждую попытку;
- commit делать только после успешной обработки или успешной отправки в DLQ.

## DLQ

Если обработка не удалась после retry, сообщение отправляется в:

```text
dead-letter-events
```

DLQ event:

```json
{
  "eventId": "uuid",
  "originalEvent": {},
  "reason": "handler_failed",
  "error": "error text",
  "sourceTopic": "inventory-events",
  "consumerGroup": "payment-service",
  "attempts": 3,
  "failedAt": "2026-06-18T12:00:05Z"
}
```

## Idempotency

Каждый service должен хранить обработанные `eventId`.

Если событие пришло повторно:

- не выполнять бизнес-логику повторно;
- залогировать duplicate event;
- commit offset.

На первом этапе можно использовать in-memory storage.  
На следующем этапе заменить на SQLite/Postgres.

## Logging

Использовать `slog`.

Логировать:

- старт сервиса;
- остановку сервиса;
- получение события;
- успешную обработку;
- публикацию нового события;
- retry;
- DLQ;
- duplicate event;
- ошибки Kafka;
- graceful shutdown.

## Docker Compose

Одной командой:

```bash
docker compose up
```

Должны запускаться:

- Kafka;
- Kafka UI;
- Order API;
- Inventory Service;
- Payment Service;
- Shipping Service.

Topics должны создаваться автоматически.

## Этапы

1. Docker Compose + Kafka + Kafka UI + auto topic creation.
2. Order API + publish `order_created`.
3. Inventory Service + consume `order_created`.
4. Inventory Service publishes `inventory_reserved` / `inventory_rejected`.
5. Payment Service consumes inventory events.
6. Payment Service publishes payment events.
7. Shipping Service consumes payment events.
8. Retry.
9. DLQ.
10. Idempotency.
11. README.
12. Тесты в конце.
