# JMS Demo — Point-to-Point with Apache ActiveMQ (Queue)

This project demonstrates the **Point-to-Point (P2P)** messaging pattern using
**JMS** and **Apache ActiveMQ**. It is the companion to the Pub/Sub (Topic) demo,
and the contrast between the two is the main learning goal.

---

## Queue vs Topic — the core difference

| | Topic (Pub/Sub) | Queue (Point-to-Point) |
|---|---|---|
| **Message delivery** | Every subscriber gets a copy | Exactly one consumer gets each message |
| **Consumers** | Independent — don't affect each other | Competing — share the workload |
| **Use case** | Broadcast / notifications | Work distribution / load balancing |
| **clientID required?** | Yes (for durable subscriptions) | No |
| **Messages held when no consumer?** | Only for durable subscribers | Always — until a consumer picks them up |

In the **Topic demo** you ran before, both Consumer A and Consumer B received
every message. In **this Queue demo**, the 10 messages are split between them —
Consumer A will process roughly 5 and Consumer B will process the other 5.
No message is ever processed twice.

---

## How this project works

The Producer sends 10 "order" messages to a Queue named `orders.queue`, one per
second. Consumer A and Consumer B both listen on the same queue. The broker
distributes the messages between them in round-robin order:

```
Producer  →  [Order #1, Order #2, ... Order #10]  →  orders.queue (broker)
                                                           ↓           ↓
                                                      Consumer A   Consumer B
                                                      (Order #1)   (Order #2)
                                                      (Order #3)   (Order #4)
                                                          ...          ...
```

---

## Project structure

```
jms-queue-demo/
├── docker-compose.yml
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

---

## Run the demo

```bash
docker compose up --build
```

---

## Expected output

The key thing to observe is that each order number appears in **only one** consumer,
never in both:

```
consumer-a  | [Consumer A] Listening on orders.queue — waiting for messages...
consumer-b  | [Consumer B] Listening on orders.queue — waiting for messages...
producer    | [Producer] Sent: Order #1 — please process me!
producer    | [Producer] Sent: Order #2 — please process me!
consumer-a  | [Consumer A] >>> Processing: Order #1 — please process me!
consumer-b  | [Consumer B] >>> Processing: Order #2 — please process me!
consumer-a  | [Consumer A] >>> Done.
consumer-b  | [Consumer B] >>> Done.
producer    | [Producer] Sent: Order #3 — please process me!
consumer-a  | [Consumer A] >>> Processing: Order #3 — please process me!
...
```

Compare this to the Topic demo where both consumers printed the same message number.

---

## Web dashboard

Open **http://localhost:8161** (username: `admin`, password: `admin`) while the
containers are running. Navigate to **Queues** to see `orders.queue`, the number
of pending messages, and how many have been consumed in total.

---

## Useful commands

```bash
# Run in the background
docker compose up --build -d

# Follow logs per container
docker logs -f consumer-a
docker logs -f consumer-b
docker logs -f producer

# Stop everything
docker compose down

# Full clean — remove containers and images
docker compose down --rmi all
```
