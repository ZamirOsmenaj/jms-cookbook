package com.example;

import org.apache.activemq.ActiveMQConnectionFactory;

import javax.jms.*;

/**
 * ConsumerB — a second independent subscriber on the same Topic.
 *
 * This class is intentionally almost identical to ConsumerA to demonstrate the
 * core Pub/Sub guarantee: both consumers subscribe to the *same* Topic, yet each
 * receives its own full copy of every message. They are completely independent —
 * ConsumerB receiving a message has no effect whatsoever on ConsumerA, and vice versa.
 *
 * The only things that differ from ConsumerA are the clientID ("ConsumerB") and
 * the durable subscription name ("subB"). Both must be unique across all consumers
 * connected to the broker at the same time.
 */
public class ConsumerB {
    public static void main(String[] args) throws Exception {

        // Read the broker URL from the environment variable injected by Docker Compose.
        // Falls back to localhost when running outside Docker.
        String brokerUrl = System.getenv("BROKER_URL") != null
                ? System.getenv("BROKER_URL")
                : "tcp://localhost:61616";

        System.out.println("[Consumer B] Connecting to broker at: " + brokerUrl);

        ConnectionFactory factory = new ActiveMQConnectionFactory(brokerUrl);
        Connection connection = null;

        // Retry loop — same reasoning as ConsumerA: the broker might not be fully
        // ready yet even though Docker Compose has already reported it as healthy.
        for (int attempt = 1; attempt <= 10; attempt++) {
            try {
                connection = factory.createConnection();

                // Must be unique across all active connections on this broker.
                // Using the same clientID as ConsumerA would cause a conflict and
                // one of the two connections would be rejected by the broker.
                connection.setClientID("ConsumerB");

                connection.start();
                System.out.println("[Consumer B] Connected!");
                break;
            } catch (Exception e) {
                System.out.println("[Consumer B] Attempt " + attempt + " failed, retrying in 3s...");
                Thread.sleep(3000);
            }
        }

        if (connection == null) {
            System.err.println("[Consumer B] Could not connect. Exiting.");
            System.exit(1);
        }

        // Non-transactional session with automatic acknowledgement.
        // The broker removes a message from this consumer's backlog as soon as
        // onMessage() completes successfully.
        Session session = connection.createSession(false, Session.AUTO_ACKNOWLEDGE);

        // Must match the exact Topic name used by the Producer and ConsumerA.
        Topic topic = session.createTopic("news.topic");

        // "subB" is the durable subscription name — different from ConsumerA's "subA".
        // The broker maintains a separate message backlog for each named subscription,
        // so ConsumerA and ConsumerB never interfere with each other's message delivery.
        TopicSubscriber subscriber = session.createDurableSubscriber(topic, "subB");

        System.out.println("[Consumer B] Subscribed and waiting for messages...");

        // Asynchronous message listener — the broker pushes messages to this callback
        // as they arrive, without ConsumerB needing to poll or call receive().
        subscriber.setMessageListener(msg -> {
            if (msg instanceof TextMessage) {
                try {
                    System.out.println("[Consumer B] >>> Received: " + ((TextMessage) msg).getText());
                } catch (JMSException e) {
                    e.printStackTrace();
                }
            }
        });

        // Keep the main thread alive so the listener keeps running.
        Thread.sleep(60000);
        connection.close();
        System.out.println("[Consumer B] Done.");
    }
}
