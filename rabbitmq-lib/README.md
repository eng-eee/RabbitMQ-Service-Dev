# rmq - a small C++17 RabbitMQ client library

`rmq` is a thin, thread-friendly layer on top of [AMQP-CPP](https://github.com/CopernicaMarketingSoftware/AMQP-CPP).
You get **publish/subscribe** and **request/reply (RPC)** with automatic reconnection, publisher
confirms and an **internal IO thread** - your application never has to run an event loop.

- C++17, Linux/POSIX, CMake >= 3.16
- No AMQP-CPP types in the public headers (it is a private dependency)
- Everything survives broker restarts and network drops (reconnect with exponential back-off)
- Blocking **and** asynchronous styles for publishing and RPC

---

## Table of contents

1. [Building and linking](#1-building-and-linking)
2. [Quick start](#2-quick-start)
3. [How it works (mental model)](#3-how-it-works-mental-model)
4. [API guide](#4-api-guide)
   - [Connection](#41-connection) - [Message / Delivery](#42-message-and-delivery) - [Publisher](#43-publisher)
   - [Subscriber](#44-subscriber) - [RpcServer](#45-rpcserver) - [RpcClient](#46-rpcclient) - [Errors](#47-errors)
5. [Delivery guarantees](#5-delivery-guarantees)
6. [Things to be careful about](#6-things-to-be-careful-about)
7. [Troubleshooting](#7-troubleshooting)
8. [Testing](#8-testing)
9. [Limitations / roadmap](#9-limitations--roadmap)
10. [Project layout](#10-project-layout)

---

## 1. Building and linking

Put the `rmq` folder in your project and add it as a subdirectory:

```cmake
cmake_minimum_required(VERSION 3.16)
project(my_app LANGUAGES CXX)

set(RMQ_BUILD_EXAMPLES OFF CACHE BOOL "" FORCE)
set(RMQ_BUILD_TESTS    OFF CACHE BOOL "" FORCE)
add_subdirectory(third_party/rmq)

add_executable(my_app main.cpp)
target_compile_features(my_app PRIVATE cxx_std_17)
target_link_libraries(my_app PRIVATE rmq::rmq)      # pulls in Threads automatically
```

or fetch it from your git server:

```cmake
include(FetchContent)
FetchContent_Declare(rmq GIT_REPOSITORY https://your.git.host/rmq.git GIT_TAG main)
set(RMQ_BUILD_EXAMPLES OFF CACHE BOOL "" FORCE)
set(RMQ_BUILD_TESTS    OFF CACHE BOOL "" FORCE)
FetchContent_MakeAvailable(rmq)
target_link_libraries(my_app PRIVATE rmq::rmq)
```

```bash
cmake -S . -B build -DCMAKE_BUILD_TYPE=Release
cmake --build build -j
```

The first configure downloads AMQP-CPP v4.3.27 from GitHub. **Offline / vendored build:**
`-DFETCHCONTENT_SOURCE_DIR_AMQPCPP=/path/to/AMQP-CPP`.

| CMake option | Default | Meaning |
|---|---|---|
| `RMQ_BUILD_EXAMPLES` | `ON` | builds `rmq_connect_demo`, `rmq_pubsub_demo`, `rmq_rpc_demo` |
| `RMQ_BUILD_TESTS` | `ON` | builds `rmq_integration_test` (needs a running broker) |
| `RMQ_WARNINGS_AS_ERRORS` | `OFF` | `-Werror` for the library sources |

Include everything with one header: `#include <rmq/rmq.hpp>`.

---

## 2. Quick start

A complete program: one connection, one consumer, one publisher.

```cpp
#include <rmq/rmq.hpp>

#include <chrono>
#include <iostream>
#include <thread>

using namespace std::chrono_literals;

int main() {
    try {
        // 1) One Connection per broker. It owns the IO thread.
        rmq::ConnectionOptions opts;
        opts.host = "127.0.0.1";
        opts.username = "guest";
        opts.password = "guest";

        rmq::Connection conn(opts);
        conn.setErrorCallback([](const rmq::Status& s) {          // runs on the IO thread
            std::cerr << "[rmq] " << rmq::to_string(s.code) << ": " << s.message << "\n";
        });
        conn.start();                                              // returns immediately
        if (!conn.waitConnected(10s)) {
            std::cerr << "broker not reachable (still retrying in the background)\n";
            return 1;
        }

        // 2) Consume from a queue. The Subscriber declares the queue for you.
        rmq::SubscriberOptions so;
        so.queue = "app.events";
        rmq::Subscriber sub(conn, so);
        sub.start([](const rmq::Delivery& d) {                     // runs on the IO thread: keep it short
            std::cout << "received: " << d.message.body << "\n";
            return rmq::AckAction::Ack;
        });
        std::this_thread::sleep_for(300ms);   // give the subscriber time to declare the queue (see section 6, #5)

        // 3) Publish through the default exchange: routing key == queue name.
        rmq::Publisher pub(conn);
        rmq::Message msg;
        msg.body = "hello";
        msg.content_type = "text/plain";
        const rmq::Status st = pub.publish("app.events", std::move(msg));   // blocks until the broker confirmed
        if (!st) std::cerr << "publish failed: " << st.message << "\n";

        std::this_thread::sleep_for(300ms);
    } catch (const rmq::Error& e) {                                // bad options, double start, ...
        std::cerr << "rmq error (" << rmq::to_string(e.code()) << "): " << e.what() << "\n";
        return 1;
    }                                                              // destructors shut everything down cleanly
}
```

Running a throw-away broker for development:

```bash
docker run --rm -p 5672:5672 -p 15672:15672 rabbitmq:3-management    # UI: http://localhost:15672 (guest/guest)
```

More runnable samples live in `examples/`: `pubsub_demo.cpp` (topic exchange) and `rpc_demo.cpp` (server + client).

---

## 3. How it works (mental model)

```
 your threads                         rmq IO thread ("rmq-io", one per Connection)
 ------------                         ---------------------------------------------
 publish()/call()/start()  --post-->  task queue --> AMQP-CPP channels --> poll() loop --> TCP socket --> broker
 callbacks/handlers        <--------  confirms, deliveries, replies, state changes, timers, heartbeats
```

- Each `Connection` starts **one internal thread** that runs a `poll()` loop: socket I/O, the AMQP protocol,
  heartbeats, reconnect back-off and timers (RPC timeouts). AMQP-CPP objects are touched by that thread only,
  so there is no locking around them and **you never run an event loop yourself**.
- `Publisher`, `Subscriber`, `RpcClient` and `RpcServer` are *components* that borrow a `Connection`.
  Each owns one AMQP channel and keeps it alive: after a connection loss or a channel error the channel is
  re-created and its topology (exchange / queue / bindings / consumer) is re-declared automatically.
- All public methods are thread-safe. They enqueue work for the IO thread and return (or wait on a
  future/promise for the blocking variants).
- **Everything you hand to the library as a callback or handler runs on the IO thread.**
  That is the single most important rule - see [section 6](#6-things-to-be-careful-about).

Connection states:

```
Idle --start()--> Connecting --handshake ok--> Connected --lost--> Reconnecting --ok--> Connected
                      |                                                |
                      +-- (auto_reconnect=false: Failed) <-------------+
 any state --stop()--> Closing --> Closed          (Closed is final: a Connection cannot be restarted)
```

Reconnect back-off: `reconnect_min_delay` (500 ms), doubled after every failure up to `reconnect_max_delay`
(30 s), reset after a successful connect. There is no jitter.

---

## 4. API guide

### 4.1 Connection

```cpp
rmq::ConnectionOptions o;
o.host = "rabbit.local";   o.port = 5672;
o.username = "svc";        o.password = "secret";   o.vhost = "/";
o.heartbeat_seconds = 30;                     // 0 = accept the broker's proposal
o.connect_timeout   = std::chrono::seconds(5);   // TCP connect and AMQP handshake (each)
o.auto_reconnect    = true;
o.reconnect_min_delay = std::chrono::milliseconds(500);
o.reconnect_max_delay = std::chrono::seconds(30);
o.shutdown_timeout    = std::chrono::seconds(2);  // how long stop() waits for the close handshake

rmq::Connection conn(o);      // validates options -> throws rmq::Error(InvalidArgument)
```

| Member | What it does |
|---|---|
| `start()` | Spawns the IO thread and begins connecting. Non-blocking. Calling it twice is fine; after `stop()` it throws `Error(Closed)`. |
| `stop()` | Graceful close (sends AMQP close, waits up to `shutdown_timeout`), joins the IO thread. Idempotent. Also run by the destructor. If called *from the IO thread* (inside a callback) it only requests the shutdown and does not join. |
| `waitConnected(timeout)` | Blocks until `Connected` -> `true`. Returns `false` on timeout, or if the connection reached `Closing`, `Closed` or `Failed`. While it is `Connecting`/`Reconnecting` it keeps waiting. |
| `state()` / `isConnected()` | Current `ConnectionState`; thread-safe snapshot. |
| `setStateCallback(cb)` | `void(ConnectionState, const std::string& detail)` on every state change. |
| `setErrorCallback(cb)` | `void(const Status&)` for connection errors (connect failures, broker closes the connection, heartbeat timeout), **channel errors** of every component (e.g. `NOT_FOUND - no exchange ...`) and exceptions thrown by your handlers. |

Set both callbacks **before** `start()` so you do not miss the first events. They run on the IO thread.

### 4.2 Message and Delivery

```cpp
struct Message {
    std::string body;                              // opaque bytes
    std::string content_type, content_encoding;
    std::string correlation_id, reply_to, message_id;
    std::map<std::string, std::string> headers;    // string values only
    std::chrono::milliseconds expiration{0};       // per-message TTL, 0 = none
    bool persistent = false;                       // delivery-mode 2
};
struct Delivery {                                  // what a consumer receives
    Message message;
    std::string exchange, routing_key;
    std::uint64_t delivery_tag;
    bool redelivered;
};
```

- `persistent = true` only survives a broker restart if the **queue is durable** too.
- Received headers whose value is not a string (ints, tables, ...) are dropped when converting to `Message::headers`.
- In RPC, `correlation_id` and `reply_to` of the *request* are managed by the library (overwritten).

### 4.3 Publisher

```cpp
rmq::PublisherOptions po;
po.exchange          = "events";   // "" = default exchange (routing key = queue name)
po.exchange_type     = "topic";    // direct | fanout | topic | headers (used only when declaring)
po.declare_exchange  = true;       // declare it on every (re)connect
po.durable           = true;       // durability of the declared exchange
po.confirms          = true;       // publisher confirms (default)
po.max_pending       = 10000;      // back-pressure limit (queued + unconfirmed messages)

rmq::Publisher pub(conn, po);      // throws Error(InvalidArgument) on bad options

// blocking: returns after the broker confirmed, or after the timeout
rmq::Status st = pub.publish("sensor.temp", rmq::Message{"21.5"}, std::chrono::seconds(5));

// asynchronous: returns immediately, `done` reports the outcome
pub.publishAsync("sensor.temp", rmq::Message{"21.5"}, [](const rmq::Status& s) {
    if (!s) { /* log / retry */ }
});
```

How it behaves:

- **Confirms on** (default): the completion fires when the broker acked (`Ok`), nacked (`Channel`),
  or the connection/channel died before the confirm (`Io` / `Channel` / `Closed`).
- **Confirms off**: the completion fires as soon as the frame is handed to the socket buffer - no broker
  acknowledgement at all.
- While the connection is down, new messages are **queued** (up to `max_pending`) and sent after the reconnect.
- If the queue is full the completion reports `Overflow`; if the connection was stopped it reports `Closed`.
  In these two cases the completion runs **inline on the calling thread**, otherwise on the IO thread.
- `publish()` returns `Status{Timeout}` if no confirm arrives in time. The message may **still be delivered later**.
- `publish()` called from the IO thread (inside a handler/callback) returns `Status{Internal}` instead of deadlocking.

### 4.4 Subscriber

```cpp
rmq::SubscriberOptions so;
so.queue       = "orders";                       // empty = server-named queue
so.durable     = true;
so.exclusive   = false;
so.auto_delete = false;
so.bindings    = { {"events", "sensor.*"} };     // exchange + routing key; the exchange must already exist
so.prefetch    = 50;                             // max unacked messages in flight (0 = unlimited)
so.auto_ack    = false;                          // true = broker considers messages delivered immediately

rmq::Subscriber sub(conn, so);
sub.start([](const rmq::Delivery& d) {
    process(d.message.body);
    return rmq::AckAction::Ack;                  // or NackRequeue / NackDiscard
});
// ...
sub.stop();                                      // blocking; no handler call after it returns (also done by the destructor)
std::string q = sub.queueName();                 // useful for server-named queues
```

- The handler's return value is the ack decision, applied right after the handler returns.
  `NackRequeue` puts the message back on the queue, `NackDiscard` drops it (or dead-letters it if the queue has a DLX).
- A handler that **throws** is treated as `NackDiscard`, and the exception text is sent to your `ErrorCallback`.
- With `auto_ack = true` the return value is ignored and `prefetch` is not applied.
- `start()` throws `Error(InvalidArgument)` if the handler is empty or the subscriber is already started.
  After `stop()` you may `start()` again.
- On reconnect the queue, bindings and consumer are re-created. A **server-named** queue gets a **new name** each time.
- Messages that were delivered but not yet acked when the connection dropped are redelivered by the broker
  (`Delivery::redelivered == true`).

### 4.5 RpcServer

```cpp
rmq::RpcServerOptions so;
so.queue    = "rpc.upper";     // required
so.durable  = true;
so.prefetch = 1;               // 1 = strictly one request at a time

rmq::RpcServer server(conn, so);
server.start([](const rmq::Delivery& request) {
    rmq::Message reply;
    reply.body = to_upper(request.message.body);
    return reply;              // sent to request.message.reply_to with the same correlation_id
});
```

- If the handler **throws**, an empty reply with header `rmq-error: <what()>` is sent; the client's future then
  throws `rmq::Error` with `ErrorCode::Remote`. The request is acked either way (it is not retried).
- A request without `reply_to` is processed and acked, but nothing is sent back.
- The handler runs on the IO thread, so a slow handler delays *all* traffic of this connection, including heartbeats.

### 4.6 RpcClient

```cpp
rmq::RpcClientOptions co;
co.routing_key     = "rpc.upper";      // the server's queue (default exchange) ...
co.exchange        = "";               // ... or set an exchange + routing key
co.default_timeout = std::chrono::seconds(5);

rmq::RpcClient client(conn, co);       // throws if neither exchange nor routing_key is set

// (a) future style
try {
    rmq::Message reply = client.call(rmq::Message{"hello"}).get();            // timeout 0 => default_timeout
    rmq::Message r2    = client.call(rmq::Message{"hello"}, std::chrono::milliseconds(300)).get();
} catch (const rmq::Error& e) {
    // e.code(): Timeout, Remote (server threw), Io (connection lost), Closed, Channel
}

// (b) callback style - the callback runs on the IO thread
client.callAsync(rmq::Message{"hello"}, [](const rmq::Status& st, rmq::Message reply) {
    if (st) use(reply.body); else log(st.message);
});
```

- One client handles many concurrent calls (correlation ids); share it between threads.
- It creates one exclusive, auto-delete reply queue per client and re-creates it after a reconnect.
- Calls made before the reply queue is ready are held and sent as soon as it is.
- A request that was **already sent** when the connection dropped fails right away with `Io` (its reply queue is gone).
  The library never retries a request by itself - your RPC may or may not have been executed by the server.
- A reply that arrives after the timeout is ignored.
- Requests are published without the `mandatory` flag: if the target queue does not exist the call simply times out
  (no fast failure).

### 4.7 Errors

Two mechanisms are used: **exceptions** for programming/config mistakes and futures, **`Status`** for runtime outcomes.

| `ErrorCode` | Meaning | Typically seen from |
|---|---|---|
| `InvalidArgument` | bad option / empty handler / double `start()` | constructors, `start()` (exception) |
| `ConnectFailed` | DNS/TCP connect failed or timed out | `ErrorCallback` |
| `Io` | socket error, peer closed, connection lost before confirm/reply | callbacks, RPC futures |
| `Protocol` | AMQP error from the broker (auth failure, `CONNECTION_FORCED`, ...) | `ErrorCallback` |
| `Timeout` | handshake / heartbeat / confirm / RPC timeout | `publish()`, RPC futures, `ErrorCallback` |
| `Closed` | the object or connection was stopped | completions, RPC futures, `start()` after `stop()` |
| `Channel` | channel-level broker error (`NOT_FOUND`, `PRECONDITION_FAILED`, nack, ...) | completions, `ErrorCallback` |
| `Overflow` | `Publisher` queue full (`max_pending`) | publish completions |
| `Remote` | the RPC server's handler threw | RPC futures/callbacks |
| `Internal` | unexpected failure, handler threw, blocking call on the IO thread | various |

`rmq::to_string(code)` gives the name for logging. `Status` converts to `bool` (`true` = ok).

---

## 5. Delivery guarantees

| Situation | Behaviour |
|---|---|
| Publish while connected, confirms on | **At-least-once** towards the broker once the completion says `Ok`. |
| Publish while disconnected | Queued in memory (<= `max_pending`) and sent after reconnect. Lost if your process exits first. |
| Connection drops with unconfirmed publishes | Completion fails; the message is **not resent** (it may or may not have reached the broker). Resend in your code if you accept duplicates. |
| Consumer, manual ack | **At-least-once**: unacked messages are redelivered after a drop. Make handlers idempotent. |
| Consumer, `auto_ack` | **At-most-once**: messages in flight at a crash are lost. |
| Broker restart | Non-durable queues/exchanges vanish; they are re-declared on reconnect, but messages published in the gap are not routed anywhere. Use durable queues (+ `persistent` messages) if you need survival. |
| Message to a non-existent queue/exchange (default exchange) | **Silently dropped** by RabbitMQ (no `mandatory` flag is used). |
| RPC | Timeout/connection loss never tell you whether the server executed the request. Design requests to be idempotent. |

---

## 6. Things to be careful about

1. **Callbacks and handlers run on the IO thread.** This covers `StateCallback`, `ErrorCallback`, subscriber/RPC-server
   handlers, publish completions and `callAsync` callbacks. Do **not** block in them: no sleeps, no long computation,
   no blocking I/O. While they run, the connection cannot read, write or send heartbeats. If you block for longer than
   about twice the heartbeat interval, the broker drops the connection. Hand slow work to your own thread pool
   (copy the data, then return `Ack` immediately only if losing the message on a crash is acceptable; otherwise use
   `auto_ack = false` and keep the handler fast).
2. **Never wait on a future / blocking call from the IO thread.** `client.call(...).get()` inside a handler or callback
   deadlocks the connection (the reply can only be processed by the thread you just blocked). `Publisher::publish()`
   detects this and returns `Internal`; `RpcClient::call().get()` and `waitConnected()` do not.
   Use the async variants inside handlers.
3. **Do not call `Connection::stop()`/`waitConnected()` from a callback.** `stop()` is safe (request only), but
   `waitConnected()` would block the IO thread.
4. **A `Connection` is single-use.** After `stop()` it stays `Closed`; create a new one. Components (`Publisher`, ...) are
   bound to their connection and are dead once it is stopped.
5. **Topology is created asynchronously, and publishing does not create queues.** `Subscriber::start()` returns
   immediately; the queue exists a few milliseconds later. A message published to the default exchange before the
   queue exists is dropped. Start consumers first (or use durable queues that already exist), and wait for the
   subscriber (e.g. `queueName()` for server-named queues, or a short delay) before the first publish in tests.
   Declared-exchange and binding order matters too: the `Publisher` with `declare_exchange` must have declared the
   exchange before a `Subscriber` binds to it, otherwise the binding fails with `NOT_FOUND`
   (the channel is retried every second).
6. **Server-named queues + `durable = true` leak queues.** `SubscriberOptions` defaults to `durable = true`. With an empty
   `queue` name set `durable = false` and `exclusive = true` (or `auto_delete = true`), otherwise every reconnect
   leaves a new durable queue behind on the broker.
7. **Re-declaring with different arguments fails.** Declaring an existing queue/exchange with different
   durability/flags yields `PRECONDITION_FAILED` (reported via `ErrorCallback`, retried every second). Delete the old
   entity or keep the options identical.
8. **Channel errors are not fatal but loud.** A broker-side channel error (unknown exchange, access refused, ...) closes
   only that component's channel, fails its in-flight work, reports to `ErrorCallback` and retries after 1 s. Always set an
   `ErrorCallback`, otherwise these problems are invisible.
9. **`publish()` timing out is not "not sent".** The message might still be delivered afterwards. With
   confirms off there is no feedback at all.
10. **Handlers must be idempotent** (redeliveries, plus at-least-once semantics, see section 5).
11. **Back-pressure.** `Publisher::max_pending` bounds queued + unconfirmed messages per publisher (excess gets `Overflow`),
    but the socket output buffer is not bounded: do not publish unbounded streams of huge bodies faster than the
    network/broker can take them. Use confirms, check completions and slow down on `Overflow`.
12. **Heartbeats.** `heartbeat_seconds = 0` means "use the broker's value", not "disabled". Missing broker traffic for
    2 x the negotiated interval is treated as a dead connection and triggers a reconnect.
13. **Shutdown order.** Destroying components first and the `Connection` last is the cleanest order. Destructors block
    briefly (component close up to ~5 s worst case, `stop()` up to `shutdown_timeout`). Never destroy these objects from
    inside one of their own callbacks.
14. **Security.** There is no TLS yet: credentials and data travel in clear text. Use it on trusted networks, localhost
    or through a tunnel/VPN until TLS is added.
15. **Message size.** Bodies are copied (into the outgoing buffer and into `Delivery`). For multi-megabyte payloads keep
    `prefetch` low.
16. **Platform.** Linux/POSIX only (`pipe2`, `poll`, `MSG_NOSIGNAL`).

---

## 7. Troubleshooting

| Symptom | Likely cause |
|---|---|
| State stays `Connecting`, errors say `ConnectFailed ... Connection refused` | wrong host/port, broker not running, firewall. The library keeps retrying with back-off. |
| `Protocol: ACCESS_REFUSED` / handshake errors | wrong user/password or no permission on the vhost. |
| `Protocol: CONNECTION_FORCED ... shutdown` | broker was stopped/restarted - the library reconnects by itself. |
| `Timeout: heartbeat timeout` | a handler/callback blocked the IO thread, or the network is dead. |
| `Channel: NOT_FOUND - no exchange 'x'` | publishing to / binding to an exchange that was never declared (set `declare_exchange` or create it). |
| `Channel: PRECONDITION_FAILED` | queue/exchange already exists with different flags. |
| Published messages never arrive | no queue bound / queue not declared yet (section 6 #5); wrong routing key; `publish()` result not checked. |
| `call().get()` throws `Timeout` | server not running, wrong `routing_key`, or the server queue does not exist. |
| Process hangs on exit | a handler is blocked, or a callback waits on a future (section 6 #1, #2). |

Enable the callbacks first - they tell you almost everything:

```cpp
conn.setStateCallback([](rmq::ConnectionState s, const std::string& why) { log("state", rmq::to_string(s), why); });
conn.setErrorCallback([](const rmq::Status& st) { log("error", rmq::to_string(st.code), st.message); });
```

---

## 8. Testing

`tests/integration_test.cpp` runs against a real broker (pub/sub, topic exchange, RPC incl. remote error and timeout,
failure path, teardown):

```bash
docker run --rm -d --name rmq-test -p 5672:5672 rabbitmq:3
./build/rmq_integration_test 127.0.0.1 5672 basic
```

Reconnect test (publishes for 20 s - restart the broker connections meanwhile):

```bash
./build/rmq_integration_test 127.0.0.1 5672 reconnect &
sleep 5; docker restart rmq-test        # or: rabbitmqctl stop_app; sleep 3; rabbitmqctl start_app
wait
```

`tools/fake_amqp_broker.py` is a tiny handshake-only fake broker (no channels) used to test connect, heartbeats and
reconnect without RabbitMQ: `python3 tools/fake_amqp_broker.py <port> <heartbeat_s> <send_heartbeats 0|1> <lifetime_s>`.

The code was also run under AddressSanitizer + UBSan (clean) and ThreadSanitizer (only a known libstdc++
`exception_ptr` false positive when a failed RPC future is destroyed).

---

## 9. Limitations / roadmap

- No TLS (`amqps`), no connection URI parsing, no multiple-host failover.
- No explicit queue/exchange management API (`declareQueue`, `bind`, `delete`): topology is declared by `Subscriber`,
  `RpcServer`, `RpcClient` and `Publisher(declare_exchange)` only.
- No `mandatory` publishing / returned-message handling; no automatic resend of unconfirmed messages.
- Headers support string values only; no consumer-cancel notifications; one channel per component.
- Handlers run on the IO thread (an optional worker pool for handlers is a natural next step).
- No `install()` / `find_package()` rules yet - use `add_subdirectory` or `FetchContent`.
- Linux/POSIX only.

---

## 10. Project layout

```
rmq/
  CMakeLists.txt
  include/rmq/            public API: connection, publisher, subscriber, rpc_client, rpc_server, message, error, rmq.hpp
  src/
    connection.cpp        Connection facade, option validation
    publisher.cpp         confirms, pending queue, in-flight tracking
    subscriber.cpp  rpc_server.cpp  rpc_client.cpp  error.cpp
    detail/
      connection_core.*   IO thread, socket, heartbeats, reconnect, timers, task queue (AMQP::ConnectionHandler)
      managed_channel.*   one self-healing channel (re-create on reconnect / channel error)
      consumer.*          queue + bindings + consumer + acks, shared by Subscriber and RpcServer
      amqp_util.*         Message <-> AMQP envelope conversion
  examples/               connect_demo, pubsub_demo, rpc_demo
  tests/                  integration_test (needs a broker)
  tools/                  fake_amqp_broker.py
```
