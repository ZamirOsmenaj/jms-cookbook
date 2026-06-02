# JMS Demo — Pub/Sub with Apache ActiveMQ

This project demonstrates the **Publish-Subscribe (Pub/Sub)** messaging pattern
using **JMS** (Java Message Service) and **Apache ActiveMQ** as the broker.
Everything runs in Docker — no local Java, Maven, or ActiveMQ installation required.

---

## What is JMS?

**JMS (Java Message Service)** is a standard Java API (part of Jakarta EE) that lets applications 
create, send, receive, and read messages asynchronously through a messaging system (a "message broker"). 
Think of it as a contract: JMS defines *how* your code talks to a messaging system, while the actual
broker (ActiveMQ, IBM MQ, RabbitMQ, etc.) is a plug-in implementation underneath.

Because your code targets the JMS API rather than any specific broker, you could swap
ActiveMQ for another broker with minimal code changes.

### Purpose

JMS decouples applications from each other. Instead of calling each other directly, 
systems communicate by exchanging messages through a broker. This means:

- **Sender and receiver don't need to be running at the same time**
- **Neither needs to know the other's location or implementation**
- Messages are delivered reliably, even if a component is temporarily down

### When is it used?

JMS is a good fit when you need:

- **Asynchronous processing** — e.g., submitting an order and processing it in the background
- **Loose coupling** — microservices or systems that shouldn't depend directly on each other
- **Load balancing** — distributing tasks among multiple consumers
- **Reliability** — guaranteed message delivery even during failures
- **Event-driven architecture** — triggering workflows based on events

Common real-world examples: order processing, email notifications, payment handling, log aggregation.

### JMS defines two messaging models:

| Model | Destination type | Who receives the message? |
|---|---|---|
| **Point-to-Point** | Queue | Exactly one consumer |
| **Publish-Subscribe** | Topic | Every subscriber gets a copy |

This demo uses **Topics (Pub/Sub)**. The Producer sends a message once; both
Consumer A and Consumer B each receive their own independent copy.

### Key Takeaway
Think of JMS like a post office: the sender drops a letter (message) in a mailbox (queue/topic), 
and the receiver picks it up when ready — neither needs to be at the door at the same time. Popular JMS brokers include ActiveMQ, IBM MQ, and RabbitMQ (with a JMS adapter).

---

## What is Apache ActiveMQ?

**Apache ActiveMQ Classic** is one of the most widely used open-source message brokers.
It implements the JMS specification and acts as the middleman between producers and
consumers — it receives messages, stores them (if configured to do so), and forwards
them to all matching subscribers.

In this project ActiveMQ runs as a Docker container. It exposes two ports:

- **61616** — the JMS transport port (TCP). Java clients connect here.
- **8161** — the web administration console. Open it in your browser to inspect
  queues, topics, and live message traffic.

---

## How this project is structured

```
01-pubsub-basic/
├── docker-compose.yml          # Orchestrates all 4 containers
├── producer/
│   ├── Dockerfile
│   ├── pom.xml
│   └── src/main/java/com/example/Producer.java
├── consumer-a/
│   ├── Dockerfile
│   ├── pom.xml
│   └── src/main/java/com/example/ConsumerA.java
└── consumer-b/
    ├── Dockerfile
    ├── pom.xml
    └── src/main/java/com/example/ConsumerB.java
```

Each of the three Java services has its own `Dockerfile` that uses a Maven base image
to compile the source and package it into a fat JAR. `docker-compose.yml` starts all
four containers (broker + 3 Java services) in the right order and wires them together
on a shared internal network.

---

## Run the demo

```bash
docker compose up --build
```

The first run takes a few minutes — Maven downloads dependencies inside the containers.
Subsequent runs are fast because Docker caches the build layers.

---

## Expected output

```
activemq    | INFO: ActiveMQ started
consumer-a  | [Consumer A] Connected!
consumer-a  | [Consumer A] Subscribed and waiting for messages...
consumer-b  | [Consumer B] Connected!
consumer-b  | [Consumer B] Subscribed and waiting for messages...
producer    | [Producer] Sent: Breaking news #1 - Hello from Producer!
consumer-a  | [Consumer A] >>> Received: Breaking news #1 - Hello from Producer!
consumer-b  | [Consumer B] >>> Received: Breaking news #1 - Hello from Producer!
producer    | [Producer] Sent: Breaking news #2 - Hello from Producer!
consumer-a  | [Consumer A] >>> Received: Breaking news #2 - Hello from Producer!
consumer-b  | [Consumer B] >>> Received: Breaking news #2 - Hello from Producer!
...
```

Both consumers receive every message independently — that is the Pub/Sub guarantee.

---

## Web dashboard

While the containers are running, open:

**http://localhost:8161** — username: `admin`, password: `admin`

Navigate to **Topics** to see `news.topic`, the number of subscribers, and a running
count of messages enqueued and dispatched in real time.

---

## Useful commands

```bash
# Run in the background
docker compose up --build -d

# Follow the logs of a specific container
docker logs -f consumer-a
docker logs -f consumer-b
docker logs -f producer

# Stop everything
docker compose down

# Full clean — remove containers, images, and cached build layers
docker compose down --rmi all
```
