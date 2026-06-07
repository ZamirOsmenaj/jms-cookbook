# JMS Demo — Advanced Pub/Sub with Apache ActiveMQ

This project builds on `01-pubsub-basic` and demonstrates more realistic Pub/Sub
features: **durable subscriptions**, **message selectors**, and **concurrent subscribers**
running side-by-side. Everything runs in Docker — no local Java, Maven, or ActiveMQ
installation required.

> **Recommended order:** complete `01-pubsub-basic` first. This project assumes you
> already understand the basic Topic / Pub/Sub model.

---

## What this project adds

| Feature | What it demonstrates |
|---|---|
| **Non-durable subscription** | Standard behaviour — messages are lost if the subscriber is offline |
| **Durable subscription** | Broker queues messages while the subscriber is offline; delivered on reconnect |
| **Message selectors** | SQL-92 filter evaluated broker-side; subscriber only receives matching messages |
| **Concurrent subscribers** | Four subscribers run in the same JVM, each with a different strategy |
| **Shared library module** | `jms-common` is a reusable Maven module holding the domain model and helpers |

---

## Architecture

```
┌─────────────────────────────────────────────────────────────────┐
│                         Docker Network                          │
│                                                                 │
│  ┌─────────────┐   publishes    ┌──────────────────────────┐    │
│  │  Publisher  │ ─────────────► │   ActiveMQ Broker        │    │
│  │  (Java 17)  │   TextMessage  │   Topic: news.events     │    │
│  └─────────────┘   (JSON)       └────────────┬─────────────┘    │
│                                              │                  │
│                          delivers to each subscriber            │
│                          ▼           ▼           ▼          ▼   │
│               ┌──────────────┐ ┌──────────────┐ ┌──────────────┐│
│               │  all-news    │ │durable-      │ │sports-       ││
│               │ (non-durable)│ │all-news      │ │breaking      ││
│               │ no filter    │ │(durable)     │ │(filtered)    ││
│               └──────────────┘ └──────────────┘ └──────────────┘│
│                                                 ┌──────────────┐│
│                                                 │breaking-news ││
│                                                 │(filtered)    ││
│                                                 └──────────────┘│
└─────────────────────────────────────────────────────────────────┘
```

### The four subscribers

| Name | Type | Selector | Receives |
|---|---|---|---|
| `all-news` | Non-durable | none | Every message |
| `durable-all-news` | Durable | none | Every message; catches up after going offline |
| `sports-breaking` | Filtered | `category = 'SPORTS' AND priority = 'BREAKING'` | Only breaking sports news |
| `breaking-news` | Filtered | `priority = 'BREAKING'` | All breaking news, any category |

---

## Project structure

```
02-pubsub-advanced/
├── pom.xml                          ← Parent POM (multi-module)
├── docker-compose.yml               ← Orchestrates all services
│
├── jms-common/                      ← Shared library (no main class)
│   └── src/main/java/com/example/jms/common/
│       ├── NewsEvent.java           ← Domain model / message payload
│       ├── JmsConnectionFactory.java  ← Connection helper
│       └── JsonUtils.java           ← JSON serialisation helper
│
├── jms-publisher/
│   ├── Dockerfile
│   └── src/main/java/com/example/jms/publisher/
│       ├── NewsPublisher.java       ← Core publish logic
│       └── PublisherApp.java        ← Entry point; sends sample events
│
└── jms-subscriber/
    ├── Dockerfile
    └── src/main/java/com/example/jms/subscriber/
        ├── NewsSubscriber.java      ← Core subscribe logic
        └── SubscriberApp.java       ← Launches 4 concurrent subscribers
```

---

## Quick start

### Prerequisites

- Docker + Docker Compose v2
- (Optional, for local runs) JDK 17, Maven 3.6+

### Run everything

```bash
# From the 02-pubsub-advanced directory
docker compose up --build
```

The first run takes a few minutes — Maven downloads dependencies inside the containers.
Subsequent runs are fast because Docker caches the build layers.

### Watch the output

```bash
# Subscriber output (most interesting)
docker compose logs -f subscriber

# Publisher output
docker compose logs -f publisher
```

### Stop everything

```bash
docker compose down

# Full clean — remove containers, images, and cached build layers
docker compose down --rmi all
```

---

## Expected output

### Publisher (sends one round of 12 events, then exits)

```
[Publisher] Published: [SPORTS/BREAKING]  Champions League Final Tonight
[Publisher] Published: [TECH/HIGH]        New AI Model Breaks Benchmarks
[Publisher] Published: [WEATHER/HIGH]     Storm Warning Issued for Coastal Areas
[Publisher] Published: [SPORTS/BREAKING]  World Record Broken in 100m Sprint
...
```

### Subscriber (four concurrent listeners)

```
[all-news]         ← RECEIVED | [SPORTS/BREAKING]  Champions League Final Tonight
[durable-all-news] ← RECEIVED | [SPORTS/BREAKING]  Champions League Final Tonight
[sports-breaking]  ← RECEIVED | [SPORTS/BREAKING]  Champions League Final Tonight
[breaking-news]    ← RECEIVED | [SPORTS/BREAKING]  Champions League Final Tonight

[all-news]         ← RECEIVED | [TECH/HIGH]        New AI Model Breaks Benchmarks
[durable-all-news] ← RECEIVED | [TECH/HIGH]        New AI Model Breaks Benchmarks
                                ↑ sports-breaking and breaking-news are silent here
                                  because this event is not BREAKING and not SPORTS
```

Key things to observe:
- `all-news` and `durable-all-news` log every single event.
- `sports-breaking` only logs events where category is `SPORTS` **and** priority is `BREAKING`.
- `breaking-news` logs all `BREAKING` events regardless of category.

> **Note on console ordering:** you may occasionally see a subscriber "RECEIVED" line appear
> before the corresponding publisher "Published →" line. This is a **display artifact** — the
> publisher and subscriber run in separate containers and Docker Compose interleaves their stdout
> streams with no ordering guarantee. The broker always receives the message before delivering it;
> it's only the log lines that appear out of order on screen. Check the timestamps in the log
> output (`HH:mm:ss.SSS`) to confirm the real sequence.

---

## Web dashboard

While containers are running, open:

**http://localhost:8161** — username: `admin`, password: `admin`

Navigate to **Topics** → `news.events` to see live enqueue / dequeue counts and the list
of active subscribers. Click **Subscribers** to inspect the state of durable subscriptions.

---

## JMS concepts covered

### 1. Durable vs non-durable subscriptions

| | Non-durable | Durable |
|---|---|---|
| Misses messages when offline? | **Yes** — lost forever | **No** — broker queues them |
| Connection requires `clientID`? | No | **Yes** |
| Named subscription? | No | **Yes** — broker identifies it by name |
| Use case | Live feeds, best-effort | Guaranteed delivery |

```java
// Non-durable — simple, no clientID needed
session.createConsumer(topic);

// Durable — connection must have a unique clientID set
connection.setClientID("my-app-instance");
session.createDurableSubscriber(topic, "durable-all-news");
```

### 2. Message selectors

Selectors are SQL-92 expressions evaluated **broker-side** against JMS message properties.
The subscriber only receives messages that match — unmatched messages are never delivered.

```java
// Only BREAKING sports news
session.createConsumer(topic, "category = 'SPORTS' AND priority = 'BREAKING'");

// All breaking news, any category
session.createConsumer(topic, "priority = 'BREAKING'");

// High or breaking finance news
session.createConsumer(topic, "category = 'FINANCE' AND priority IN ('HIGH','BREAKING')");
```

The publisher sets the properties on each message:

```java
message.setStringProperty("category", event.getCategory());
message.setStringProperty("priority", event.getPriority());
```

### 3. Acknowledge modes

| Mode | Behaviour |
|---|---|
| `AUTO_ACKNOWLEDGE` | Broker marks delivered once `onMessage()` returns normally |
| `CLIENT_ACKNOWLEDGE` | Consumer calls `message.acknowledge()` manually |
| `DUPS_OK_ACKNOWLEDGE` | Lazy; may redeliver; higher throughput |
| `SESSION_TRANSACTED` | Commit / rollback a batch of operations atomically |

### 4. Delivery modes

```java
producer.setDeliveryMode(DeliveryMode.PERSISTENT);     // survives broker restart (written to disk)
producer.setDeliveryMode(DeliveryMode.NON_PERSISTENT); // faster, lost on crash
```

### 5. JMS object hierarchy

```
ConnectionFactory  (thread-safe — share across threads)
  └─ Connection    (thread-safe — represents a TCP connection)
       └─ Session  (NOT thread-safe — one per thread)
            ├─ MessageProducer   (sends to a Destination)
            └─ MessageConsumer   (receives from a Destination)
```

### 6. TextMessage + JSON

Rather than `ObjectMessage` (which requires Java serialisation on both ends), this project
sends `TextMessage` with a JSON payload. This is the most interoperable approach in practice —
any language or framework that can parse JSON can consume the message.

```java
TextMessage msg = session.createTextMessage(JsonUtils.toJson(event));
msg.setStringProperty("category", event.getCategory());
msg.setStringProperty("priority",  event.getPriority());
producer.send(msg);
```

---

## Experiment ideas

### Try durable subscriptions in action

1. Stop the subscriber: `docker compose stop subscriber`
2. Let the publisher run for a bit (edit `MAX_ROUNDS` in `docker-compose.yml` to a higher value, e.g. `5`).
3. Restart: `docker compose start subscriber`
4. Watch `durable-all-news` catch up with all missed messages.
   `all-news` (non-durable) will **not** receive them.

### Change a message selector

Edit `SubscriberApp.java`, modify the selector string, then rebuild:

```bash
docker compose up --build subscriber
```

### Publish forever

In `docker-compose.yml`, set `MAX_ROUNDS: "-1"` on the publisher service, then:

```bash
docker compose up --build
```

### Scale publishers

```bash
docker compose up --build --scale publisher=3
```

Three publisher instances will all publish to the same topic simultaneously.

---

## Running locally (no Docker)

```bash
# Terminal 1 – start ActiveMQ (Docker just for the broker)
docker run -p 61616:61616 -p 8161:8161 apache/activemq-classic:5.18.3

# Terminal 2 – build all modules
cd 02-pubsub-advanced
mvn package -DskipTests

# Terminal 3 – start subscriber first (so the durable subscription is
#              registered before any messages arrive)
export BROKER_URL=tcp://localhost:61616
java -jar jms-subscriber/target/jms-subscriber-1.0.0-SNAPSHOT.jar

# Terminal 4 – publisher
export BROKER_URL=tcp://localhost:61616
java -jar jms-publisher/target/jms-publisher-1.0.0-SNAPSHOT.jar
```

---

## Environment variables

| Variable | Default | Description |
|---|---|---|
| `BROKER_URL` | `tcp://localhost:61616` | ActiveMQ broker address |
| `PUBLISH_INTERVAL_MS` | `3000` | Milliseconds between published events |
| `MAX_ROUNDS` | `1` | Rounds through the sample event list (`-1` = loop forever) |

---

## Ports exposed

| Port | Purpose |
|---|---|
| `61616` | JMS / OpenWire — Java clients connect here |
| `8161` | ActiveMQ Web Console |
