# Техническое задание 2

## Проект

Production Upgrade для Kafka Order Workflow.

## Цель

Доработать текущий event-driven workflow так, чтобы он был ближе к production-подходу:

- persistent idempotency;
- несколько инстансов consumer group;
- осознанная key/partition strategy;
- retry policy с backoff;
- классификация ошибок;
- observability;
- запуск всей системы через Docker Compose.

Текущий функционал проекта уже реализован:

```text
Order API
  -> order-events
  -> Inventory Service
  -> inventory-events
  -> Payment Service
  -> payment-events
  -> Shipping Service
  -> shipping-events
```

Теперь нужно усилить надежность и эксплуатационную часть.

## Этап 1. Persistent Idempotency

Сейчас idempotency хранится в in-memory map. Нужно заменить ее на постоянное хранилище.

Можно использовать:

- SQLite;
- PostgreSQL.

Рекомендуемый вариант для начала: SQLite.

### Требования

Создать таблицу:

```sql
processed_events
```

Поля:

```text
event_id
service_name
processed_at
```

Ограничение:

```text
UNIQUE(service_name, event_id)
```

Каждый worker должен:

- проверять, был ли входной `eventId` уже обработан этим сервисом;
- если был, не выполнять бизнес-логику повторно;
- не публиковать новое output event;
- делать commit offset;
- логировать duplicate event;
- после успешной публикации output event помечать входной event как processed.

### Проверка

1. Отправить один и тот же event дважды.
2. Убедиться, что output event создан только один раз.
3. Перезапустить сервис.
4. Отправить этот же event еще раз.
5. Убедиться, что output event все равно не создается повторно.

## Этап 2. Multi-Instance Consumer Groups

Запустить несколько инстансов каждого worker-а.

### Требования

Запустить:

```text
inventory-service-1
inventory-service-2
payment-service-1
payment-service-2
shipping-service-1
shipping-service-2
```

Инстансы одного сервиса должны использовать один и тот же `group-id`:

```text
inventory-service
payment-service
shipping-service
```

### Проверка

1. Отправить 20 заказов.
2. По логам убедиться, что события распределяются между инстансами.
3. Убедиться, что один input event не обрабатывается двумя инстансами одновременно.
4. Убедиться, что duplicate output events не появляются.

## Этап 3. Partition Awareness

Нужно явно зафиксировать strategy для Kafka message key.

### Требования

Все события одного заказа должны публиковаться с key:

```text
orderId
```

Это касается всех topic-ов:

```text
order-events
inventory-events
payment-events
shipping-events
```

Добавить в логирование, если возможно:

```text
topic
partition
offset
key
event_id
order_id
```

### Проверка

1. Создать несколько событий с одним `orderId`.
2. Убедиться, что они попадают в одну partition внутри одного topic-а.
3. Создать события с разными `orderId`.
4. Убедиться, что они распределяются по partitions.

## Этап 4. Retry Policy

Текущий retry делает несколько попыток подряд. Нужно сделать его ближе к реальности.

### Требования

Добавить:

- backoff между попытками;
- разные типы ошибок;
- разные DLQ reason.

Типы ошибок:

```text
retryable
non_retryable
```

Примеры:

```text
invalid_event_payload      -> non_retryable
unsupported_event_version  -> non_retryable
handler_failed             -> retryable
```

Non-retryable error должна сразу отправляться в DLQ без повторов.

Retryable error должна ретраиться до `max_retries`.

### Проверка

1. Отправить битый JSON.
2. Убедиться, что он сразу уходит в DLQ.
3. Смоделировать временную ошибку handler-а.
4. Убедиться, что worker делает retry.
5. После исчерпания retry событие должно уйти в DLQ.

## Этап 5. Observability

Добавить минимальную наблюдаемость.

### Требования

Добавить HTTP endpoint:

```http
GET /debug/state
```

или:

```http
GET /metrics
```

Минимальные counters:

```text
processed_events_total
duplicate_events_total
published_events_total
dlq_events_total
handler_errors_total
```

В structured logs должны быть поля:

```text
service
topic
group
event_id
event_type
order_id
attempt
error
```

### Проверка

1. Запустить систему.
2. Отправить несколько заказов.
3. Отправить duplicate event.
4. Отправить invalid event.
5. Проверить counters через HTTP endpoint.

## Этап 6. Docker Compose

Довести запуск проекта до одной команды.

### Требования

Через Docker Compose должны запускаться:

```text
Kafka
Kafka UI
Order API
Inventory Service
Payment Service
Shipping Service
Idempotency DB
```

Topics должны создаваться автоматически:

```text
order-events
inventory-events
payment-events
shipping-events
dead-letter-events
```

Нужны healthchecks для основных сервисов.

### Проверка

```bash
docker compose up --build
```

После запуска:

```bash
curl -X POST http://localhost:8000/orders \
  -H "Content-Type: application/json" \
  -d '{"userId":"42","items":[{"sku":"book-1","quantity":1}],"amount":500}'
```

Событие должно пройти всю цепочку до `shipping-events`.

## Этап 7. README

Обновить README.

README должен содержать:

- описание архитектуры;
- схему event flow;
- список сервисов;
- список Kafka topics;
- список event types;
- message key strategy;
- delivery semantics;
- почему проект не дает strict exactly-once;
- как работает idempotency;
- как работает retry;
- как работает DLQ;
- как запустить проект;
- как проверить happy path;
- как проверить failed payment;
- как проверить DLQ;
- как проверить duplicate event.

## Финальная проверка

Проект считается готовым, если:

1. Все сервисы запускаются через Docker Compose.
2. Happy path проходит до `shipment_created`.
3. Failed payment не создает shipment.
4. Invalid event уходит в DLQ.
5. Duplicate event не создает повторный output event.
6. Duplicate event не создает повторный output event после рестарта сервиса.
7. Два инстанса одного сервиса работают в одной consumer group.
8. В README описаны гарантии и ограничения системы.

## Важное ограничение

Цель этого задания - получить production-like проект, но не обещать strict exactly-once.

Корректная формулировка guarantees:

```text
at-least-once delivery + idempotent processing
```

или:

```text
effectively-once processing for business side effects
```

Strict exactly-once требует Kafka transactions и аккуратной работы с внешним хранилищем.
