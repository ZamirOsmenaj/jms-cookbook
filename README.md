# JMS Demos

A collection of small JMS (Java Message Service) projects created for learning, experimentation, and future reference.

The repository demonstrates common messaging patterns and JMS features using Apache ActiveMQ and Docker.

## Repository Structure

| Project           | Description                                                                                                                                                             |
|-------------------| ----------------------------------------------------------------------------------------------------------------------------------------------------------------------- |
| 01-topic-basic    | Basic Publish/Subscribe example with one producer and multiple consumers                                                                                                |
| 02-topic-advanced | Advanced Publish/Subscribe demonstrating durable subscriptions, message selectors, and concurrent consumers                                                             |
| 03-queue-basic    | Basic Point-to-Point messaging using queues                                                                                                                             |
| 04-queue-advanced | Advanced queue features: competing consumers, message priority, selectors, transacted sessions, redelivery with backoff, Dead-Letter Queue, TTL/expiry, and Request/Reply |

## Technologies

* Java
* JMS
* Apache ActiveMQ
* Docker
* Docker Compose

## Purpose

This repository is intended as:

* A personal learning resource.
* A reference implementation of common JMS patterns.
* A collection of runnable examples demonstrating messaging concepts.

## Running an Example

Navigate to the desired project directory:

```bash
cd 01-topic-basic
docker compose up --build
```

Follow the project-specific README for details about the demonstrated JMS concepts and expected behavior.

## Learning Progression

Recommended order:

1. Topic Basic
2. Topic Advanced
3. Queue Basic
4. Queue Advanced

Each project builds upon concepts introduced in the previous examples.
