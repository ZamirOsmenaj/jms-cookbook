# JMS Demo — Advanced Queue (Point-to-Point) with Apache ActiveMQ

This project builds on `03-queue-basic` and demonstrates a realistic set of advanced
JMS Queue features in a single runnable example. Everything runs in Docker — no local
Java, Maven, or ActiveMQ installation required.

> **Recommended order:** complete `03-queue-basic` first. This project assumes you
> already understand the basic Queue / Point-to-Point model.

---

## What this project adds

| Feature | What it demonstrates |
|---|---|
| **Competing consumers** | Two general workers share the load — each message goes to exactly one |
| **Message selectors** | Specialist consumers filter by `orderType` or `customerTier` (SQL-92) |
| **Message priority** | VIP orders (priority 9) are delivered before STANDARD orders (priority 4) |
| **Transacted sessions** | `session.commit()` / `session.rollback()` control acknowledgement atomically |
| **Redelivery + backoff** | Failed messages are retried with exponential backoff before going to DLQ |
| **Dead-Letter Queue** | After 3 failed redeliveries the broker moves the message to `DLQ.orders.queue` |
| **Message TTL / expiry** | FOOD orders expire after 30 s; expired messages also land in the DLQ |
| **Request / Reply pattern** | VIP consumers send a reply back to the producer's named reply queue |
| **Custom broker config** | `activemq.xml` sets per-queue DLQ policy, persistence, and memory limits |
| **Shared library module** | `common` is a reusable Maven module holding the domain model and helpers |

---

## Architecture

```
┌──────────────────────────────────────────────────────────────────────────┐
│                            Docker Network                                │
│                                                                          │
│  ┌──────────────┐  sends orders   ┌────────────────────────────────┐    │
│  │   Producer   │ ───────────────►│   ActiveMQ Broker              │    │
│  │              │◄─── replies ────│   Queue: orders.queue          │    │
│  └──────────────┘                 └──────────────┬─────────────────┘    │
│                                                  │ each order goes to   │
│                                                  │ EXACTLY ONE consumer │
│                           ┌──────────────────────┼──────────────────┐   │
│                           ▼                      ▼                  ▼   │
│              ┌─────────────────┐  ┌──────────────────┐  ┌──────────────┐│
│              │ general-worker-1│  │ general-worker-2 │  │  vip-handler ││
│              │  (no selector)  │  │  (no selector)   │  │(VIP selector)││
│              └─────────────────┘  └──────────────────┘  └──────────────┘│
│                                                                          │
│              ┌──────────────────────────────┐                           │
│              │    electronics-specialist    │                           │
│              │  (orderType='ELECTRONICS')   │                           │
│              └──────────────────────────────┘                           │
│                                                                          │
│  ┌───────────────────────────────────────────┐                          │
│  │  DLQ.orders.queue  (failed after 3x, or   │  ← dead-letter queue    │
│  │                     expired FOOD orders)  │                          │
│  └───────────────────────────────────────────┘                          │
└──────────────────────────────────────────────────────────────────────────┘
```

### The four consumers

| Name | Selector | Role |
|---|---|---|
| `general-worker-1` | none | Competing consumer — processes any order |
| `general-worker-2` | none | Competing consumer — shares load with worker-1 |
| `electronics-specialist` | `orderType = 'ELECTRONICS'` | Only picks up electronics orders |
| `vip-handler` | `customerTier = 'VIP'` | Only picks up VIP orders; also sends replies |

---

## Project structure

```
04-queue-advanced/
├── pom.xml                              ← Parent POM (multi-module, Java 17)
├── docker-compose.yml                   ← Orchestrates broker, producer, consumer
├── broker/
│   └── activemq.xml                    ← Per-queue DLQ policy, persistence config
│
├── common/                              ← Shared library (no main class)
│   └── src/main/java/com/example/jms/common/
│       ├── OrderRequest.java            ← E-commerce order domain model
│       ├── OrderReply.java              ← Reply model for request/reply pattern
│       ├── BrokerConfig.java            ← Connection factory + redelivery policy
│       └── JsonUtils.java               ← JSON serialisation helper
│
├── producer/
│   ├── Dockerfile
│   └── src/main/java/com/example/jms/producer/
│       ├── OrderProducer.java           ← Priority, TTL, JMSReplyTo, selectors
│       └── ProducerApp.java             ← Sends 12 sample orders on a loop
│
└── consumer/
    ├── Dockerfile
    └── src/main/java/com/example/jms/consumer/
        ├── OrderConsumer.java           ← Transacted session, rollback, reply
        └── ConsumerApp.java             ← Launches 4 competing/selective consumers
```

---

## Quick start

### Prerequisites

- Docker + Docker Compose v2
- (Optional, for local runs) JDK 17, Maven 3.6+

### Run everything

```bash
# From the 04-queue-advanced directory
docker compose up --build
```

The first run takes a few minutes — Maven downloads dependencies inside the containers.
Subsequent runs are fast because Docker caches the build layers.

### Watch the output

```bash
# Consumer output — load balancing, replies, failures, stats
docker compose logs -f consumer

# Producer output — sends, VIP reply received
docker compose logs -f producer
```

### Stop everything

```bash
docker compose down

# Full clean — remove containers, images, cached build layers, and persisted data
docker compose down --rmi all -v
```

---

## Expected output

### Producer (sends 12 sample orders per round, loops forever by default)

```
[Producer] SEND [VIP/ReplyTo=orders.reply] priority=9 → ORD-A1B2C3D4 | customer=CUST-001 [VIP] | type=ELECTRONICS  | $1299.99 | MacBook Pro 16-inch
[Producer] SEND [STANDARD] priority=4 ttl=30000ms    → ORD-E5F6G7H8 | customer=CUST-003 [STANDARD] | type=FOOD       |   $34.50 | Weekly grocery box
[Producer] REPLY RECEIVED ← ORD-A1B2C3D4 → COMPLETED by vip-handler: Order fulfilled in vip-handler
```

### Consumer (four concurrent listeners)

```
[general-worker-1]       RECEIVED → ORD-E5F6G7H8 | customer=CUST-003 [STANDARD] | type=FOOD       |   $34.50 | ...  | redelivered=false
[vip-handler]            RECEIVED → ORD-A1B2C3D4 | customer=CUST-001 [VIP]      | type=ELECTRONICS | $1299.99 | ...  | redelivered=false
[vip-handler]            PROCESSED #1 in 742ms → ORD-A1B2C3D4
[vip-handler]            REPLY SENT → correlationId=ORD-A1B2C3D4
[electronics-specialist] RECEIVED → ORD-B9C0D1E2 | customer=CUST-004 [STANDARD] | type=ELECTRONICS |  $499.00 | ...
```

Key things to observe:
- `general-worker-1` and `general-worker-2` alternate orders — load balancing in action.
- `electronics-specialist` only appears for `ELECTRONICS` orders.
- `vip-handler` only appears for `VIP` orders, and always sends a reply back.
- If a simulated failure occurs, you will see `FAILED` → `RECEIVED redelivered=true` until the message lands in the DLQ after 3 attempts.
- Every 15 seconds the consumer prints a stats summary with processed/failed counts.

---

## Web dashboard

While containers are running, open:

**http://localhost:8161** — username: `admin`, password: `admin`

Useful views:
- **Queues → `orders.queue`** — queue depth, number of active consumers, throughput
- **Queues → `DLQ.orders.queue`** — failed and expired messages (appears after ~30 s)
- **Queues → `orders.reply`** — VIP order replies

---

## JMS concepts covered

### 1. Queue vs Topic — the key difference

| | Queue (Point-to-Point) | Topic (Pub/Sub) |
|---|---|---|
| Who receives a message | **Exactly one** consumer | Every active subscriber |
| Load balancing | Yes — competing consumers | No — fan-out |
| Missed messages | Stay in queue until consumed | Lost (unless durable subscriber) |
| Use cases | Orders, tasks, emails, jobs | Events, notifications, broadcasts |

### 2. Competing consumers (load balancing)

`general-worker-1` and `general-worker-2` both listen on `orders.queue` with no selector.
The broker delivers each message to whichever worker is free first — automatic load balancing.

```
Order A ──► general-worker-1  (not to both!)
Order B ──► general-worker-2
Order C ──► general-worker-1  (worker-2 was busy)
```

Scale horizontally with:
```bash
docker compose up --build --scale consumer=3
```

### 3. Message priority

VIP orders are sent with JMS priority 9; STANDARD orders with priority 4.
The broker delivers higher-priority messages first, even if they arrived later.

```java
// DeliveryMode.PERSISTENT, priority, ttl
producer.send(ordersQueue, message, DeliveryMode.PERSISTENT, 9, 0);
```

JMS priorities range from 0 (lowest) to 9 (highest). Default is 4.

### 4. Message selectors

Consumers filter using SQL-92 expressions evaluated **broker-side** against JMS message properties.
Unmatched messages stay in the queue for other consumers to pick up.

```java
// Only VIP orders
session.createConsumer(queue, "customerTier = 'VIP'");

// Only electronics orders
session.createConsumer(queue, "orderType = 'ELECTRONICS'");
```

The producer sets the properties on each message before sending:

```java
message.setStringProperty("customerTier", order.getCustomerTier());
message.setStringProperty("orderType",    order.getOrderType());
```

### 5. Transacted sessions and rollback

```java
// Transacted session — acknowledgement is manual
Session session = connection.createSession(true, Session.SESSION_TRANSACTED);

// Success path: message permanently removed from the queue
session.commit();

// Failure path: message redelivered after backoff delay
session.rollback();
```

Using a transacted session means the message is only acknowledged once your business
logic succeeds. A crash or rollback puts the message back in the queue.

### 6. Redelivery policy and Dead-Letter Queue

Configured in `BrokerConfig.java` (client-side) and `activemq.xml` (broker-side):

```java
policy.setMaximumRedeliveries(3);
policy.setInitialRedeliveryDelay(2000);   // 2 s
policy.setBackOffMultiplier(2);
policy.setUseExponentialBackOff(true);    // 2s → 4s → 8s → DLQ
```

After 3 failed redeliveries the broker moves the message to `DLQ.orders.queue`
instead of discarding it. The `activemq.xml` uses `individualDeadLetterStrategy`
so each queue gets its own DLQ (e.g., `DLQ.orders.queue`) rather than a single
global `ActiveMQ.DLQ`.

### 7. Message TTL (expiry)

FOOD orders expire after 30 seconds if not consumed:

```java
long ttl = "FOOD".equals(order.getOrderType()) ? 30_000 : 0;  // 0 = never expires
producer.send(ordersQueue, message, DeliveryMode.PERSISTENT, priority, ttl);
```

Expired messages also land in `DLQ.orders.queue` because `processExpired=true`
is set in the broker's `individualDeadLetterStrategy`.

### 8. Request / Reply pattern

```
Producer                                        Consumer
────────                                        ────────
message.setJMSReplyTo(replyQueue)
message.setJMSCorrelationID(orderId)
producer.send(ordersQueue, message) ──────────► onMessage(message)
                                                // process order...
                                                replyMsg.setJMSCorrelationID(correlationId)
onReply(reply) ◄──────────────────────────────  replyProducer.send(JMSReplyTo, replyMsg)
```

This project uses a permanent named reply queue (`orders.reply`) rather than a
temporary queue, so replies survive a producer restart.

### 9. TextMessage + JSON

Rather than `ObjectMessage` (which requires Java serialisation on both ends), this project
sends `TextMessage` with a JSON payload. This is the most interoperable approach in practice —
any language or framework that can parse JSON can consume the message.

### 10. JMS object hierarchy

```
ConnectionFactory  (thread-safe — share across threads)
  └─ Connection    (thread-safe — represents a TCP connection)
       └─ Session  (NOT thread-safe — one per thread)
            ├─ MessageProducer   (sends to a Destination)
            └─ MessageConsumer   (receives from a Destination)
```

---

## Experiments

### See load balancing in action

```bash
docker compose logs -f consumer | grep "PROCESSED"
# Orders alternate between general-worker-1 and general-worker-2
```

### Trigger the DLQ

Wait ~60 seconds — 10% of orders fail and after 3 redeliveries go to the DLQ.
Then open the Web Console:

```
http://localhost:8161 → Queues → DLQ.orders.queue
```

### Watch a FOOD order expire

FOOD orders have a 30-second TTL. If `general-worker-1` or `general-worker-2` are busy,
a FOOD order can expire before being consumed and will appear in `DLQ.orders.queue`.

### Scale consumers

```bash
docker compose up --build --scale consumer=3
# 3 consumer containers, each with 4 internal workers = 12 total workers
```

### Slow down the producer and watch the queue depth grow

```bash
PRODUCE_INTERVAL_MS=500 docker compose up --build
# Queue depth increases as producer outpaces consumers, then catches up
```

### Stop the consumer and let messages accumulate

```bash
docker compose stop consumer
# Let the producer run for a few rounds, then restart:
docker compose start consumer
# Watch the backlog drain — queued messages are not lost
```

---

## Running locally (no Docker)

```bash
# Terminal 1 – start ActiveMQ (Docker just for the broker)
docker run -p 61616:61616 -p 8161:8161 apache/activemq-classic:5.18.3

# Terminal 2 – build all modules
cd 04-queue-advanced
mvn package -DskipTests

# Terminal 3 – start consumer first
export BROKER_URL=tcp://localhost:61616
java -jar consumer/target/jms-queue-consumer-1.0.0-SNAPSHOT.jar

# Terminal 4 – producer
export BROKER_URL=tcp://localhost:61616
java -jar producer/target/jms-queue-producer-1.0.0-SNAPSHOT.jar
```

---

## Environment variables

| Variable | Default | Description |
|---|---|---|
| `BROKER_URL` | `tcp://localhost:61616` | ActiveMQ broker address |
| `PRODUCE_INTERVAL_MS` | `2000` | Milliseconds between produced messages |
| `MAX_ROUNDS` | `-1` | Rounds through the sample order list (`-1` = loop forever) |

---

## Ports exposed

| Port | Purpose |
|---|---|
| `61616` | JMS / OpenWire — Java clients connect here |
| `8161` | ActiveMQ Web Console |
| `5672` | AMQP 1.0 |
| `61613` | STOMP |
