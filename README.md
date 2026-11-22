# Distributed Order Processing System

Event-driven order processing built on RabbitMQ and the in-repository `rmq`
C++17 client library. Each service is a separate executable (and a separate
container in the Compose stack) that communicates only through RabbitMQ.

> Development setup. State is kept in memory and the payment gateway is
> simulated; this is not a production deployment.


## Services

| Executable | Responsibility |
|---|---|
| `order_service` | Accepts order requests, validates them, generates order IDs, publishes `order.created`, tracks inventory/payment results and publishes status, completion and cancellation events. |
| `inventory_service` | Reserves stock, rejects insufficient stock, releases stock on cancellation, deduplicates reservations per order. |
| `payment_service` | Simulates authorization, emits success/failure, refunds on cancellation, deduplicates payments per order. |
| `notification_service` | Consumes lifecycle events and logs customer-facing notifications. |
| `order_monitor_service` | Tracks every order's history, reports failures and flags orders that stay incomplete for too long. |
| `order_cli` | Development producer that submits sample order requests. |

### Events and queues

All events are JSON envelopes:

```json
{
  "event_type": "order.created",
  "order_id": "ORD-1763840000000-1",
  "timestamp": "2026-11-22T20:00:00Z",
  "data": { "...": "..." }
}
```

| Exchange | Routing keys |
|---|---|
| `order.events` (topic, durable) | `order.request`, `order.created`, `order.rejected`, `order.status.updated`, `order.completed`, `order.cancelled`, `inventory.reserved`, `inventory.rejected`, `inventory.released`, `payment.completed`, `payment.failed`, `payment.refunded` |

| Queue | Bound routing keys | Consumer |
|---|---|---|
| `order.requests` | `order.request` | order-service |
| `order.inventory.results` | `inventory.reserved`, `inventory.rejected`, `inventory.released` | order-service |
| `order.payment.results` | `payment.completed`, `payment.failed`, `payment.refunded` | order-service |
| `inventory.events` | `order.created`, `order.cancelled` | inventory-service |
| `payment.events` | `order.created`, `order.cancelled` | payment-service |
| `notification.events` | lifecycle events | notification-service |
| `monitor.events` | `order.*`, `inventory.*`, `payment.*` | order-monitor-service |

## Build

Prerequisites: CMake >= 3.16, a C++17 compiler, `liblog4cxx-dev`, and network
access on the first configure (CMake fetches AMQP-CPP for `rabbitmq-lib`).

```bash
cmake -S . -B build -DCMAKE_BUILD_TYPE=Debug
cmake --build build -j
```

The executables are created in `build/`:

```
order_service  inventory_service  payment_service
notification_service  order_monitor_service  order_cli
```

## Run locally

Start RabbitMQ (the devcontainer already provides one on `localhost:5672`):

```bash
docker run --rm -d --name rabbitmq -p 5672:5672 -p 15672:15672 rabbitmq:4-management
```

Then start each service in its own terminal (from the repository root, so
`app.conf` is found):

```bash
./build/order_service
./build/inventory_service
./build/payment_service
./build/notification_service
./build/order_monitor_service
```

Submit orders:

```bash
./build/order_cli                           # interactive prompt in a terminal
./build/order_cli --interactive             # force interactive mode
./build/order_cli --demo
./build/order_cli --customer C-1 --product P-100 --quantity 2 --price 19.99
./build/order_cli --quantity 1000          # insufficient stock
./build/order_cli --force-payment-failure  # payment failure
```

In interactive mode the CLI asks for customer ID, product ID, quantity, unit
price and whether to force a payment failure, then submits another order if
requested.

The RabbitMQ management UI is at <http://localhost:15672>
(`developer` / `developer123`).

## Docker Compose

Build and start RabbitMQ plus all five services:

```bash
docker compose up --build
```

Submit demo orders in a throw-away container:

```bash
docker compose --profile tools run --rm order-cli --demo
```

Stop everything (keep RabbitMQ data) or remove it:

```bash
docker compose down
docker compose down -v
```

## Configuration

Values are read from `app.conf` and can be overridden by environment variables
of the same name (used by Compose):

| Variable | Default | Meaning |
|---|---|---|
| `RABBITMQ_HOST` | `localhost` | Broker host. |
| `RABBITMQ_PORT` | `5672` | Broker AMQP port. |
| `RABBITMQ_DEFAULT_USER` / `RABBITMQ_DEFAULT_PASS` | `developer` / `developer123` | Credentials. |
| `RABBITMQ_VHOST` | `/` | Virtual host. |
| `RABBITMQ_EXCHANGE` | `order.events` | Topic exchange name. |
| `RABBITMQ_HEARTBEAT_SECONDS` | `60` | AMQP heartbeat. |
| `RABBITMQ_AUTO_RECONNECT` | `true` | Reconnect with back-off. |
| `INVENTORY_DEFAULT_STOCK` | `100` | Initial stock for an unseen product. |
| `PAYMENT_FAILURE_RATE` | `0.0` | Random payment failure probability. |
| `PAYMENT_MAX_AMOUNT` | `5000` | Payments above this amount are declined. |
| `MONITOR_INCOMPLETE_TIMEOUT_SECONDS` | `60` | Incomplete-order warning threshold. |
| `MONITOR_INTERVAL_SECONDS` | `5` | Monitor scan interval. |

## Behaviour notes

- Order lifecycle: `CREATED → INVENTORY_RESERVED → PAYMENT_COMPLETED → COMPLETED`.
  A rejection or failure moves the order to `CANCELLED`; the other service then
  compensates (inventory release or payment refund).
- Inventory and payment are idempotent per `order_id`, so redelivered
  `order.created` events do not reserve or charge twice.
- Every service acknowledges messages immediately after copying them to its
  worker thread. Processing is therefore at-most-once across a crash; state is
  in memory. Durable queues and persistent messages are still used so the broker
  retains messages while a service is briefly down.
- Handlers never block the `rmq` IO thread: all business logic and publishing
  runs on the service worker thread.

  ## Example logs output

  <img width="1868" height="857" alt="image" src="https://github.com/user-attachments/assets/5320822e-8a9d-4ba9-8f52-6c68d3212d90" />

