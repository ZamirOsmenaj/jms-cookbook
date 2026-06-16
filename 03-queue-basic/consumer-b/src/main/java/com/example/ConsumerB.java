package com.example;

import org.apache.activemq.ActiveMQConnectionFactory;

import javax.jms.*;

/**
 * ConsumerB — the second competing consumer on the same Queue.
 *
 * ConsumerA and ConsumerB are intentionally almost identical. This is the point:
 * in the Queue model, all consumers on the same queue are interchangeable workers.
 * The broker does not care which consumer processes which message — it just ensures
 * that each message goes to exactly one of them.
 *
 * You can prove this by watching the logs: you will NOT see the same order number
 * in both Consumer A and Consumer B output. Each order is processed once, by one worker.
 *
 * Real-world analogy: think of a call centre queue. Calls (messages) wait in line,
 * and the next available agent (consumer) picks up the next call. No call is handled
 * by two agents at once, and no agent handles the same call twice.
 */
public class ConsumerB {
    public static void main(String[] args) throws Exception {

        // Read broker URL from the environment variable set by Docker Compose.
        // Falls back to localhost when running outside Docker.
        String brokerUrl = System.getenv("BROKER_URL") != null
                ? System.getenv("BROKER_URL")
                : "tcp://localhost:61616";

        System.out.println("[Consumer B] Connecting to broker at: " + brokerUrl);

        ConnectionFactory factory = new ActiveMQConnectionFactory(brokerUrl);
        Connection connection = null;

        // Retry loop: handles the startup race condition between containers.
        for (int attempt = 1; attempt <= 10; attempt++) {
            try {
                connection = factory.createConnection();
                // No clientID needed — queue consumers do not use durable subscriptions.
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
        // The broker removes the message from the queue as soon as onMessage() returns.
        Session session = connection.createSession(false, Session.AUTO_ACKNOWLEDGE);

        // The queue name must be identical to what the Producer and ConsumerA use.
        // All three share the same logical queue on the broker.
        Queue queue = session.createQueue("orders.queue");

        // Plain MessageConsumer — no TopicSubscriber, no durable subscription name,
        // no clientID. Queues are simpler to set up than durable topic subscriptions.
        MessageConsumer consumer = session.createConsumer(queue);

        System.out.println("[Consumer B] Listening on orders.queue — waiting for messages...");

        consumer.setMessageListener(msg -> {
            if (msg instanceof TextMessage) {
                try {
                    System.out.println("[Consumer B] >>> Processing: " + ((TextMessage) msg).getText());
                    Thread.sleep(500); // simulate work
                    System.out.println("[Consumer B] >>> Done.");
                } catch (Exception e) {
                    e.printStackTrace();
                }
            }
        });

        // Keep alive long enough to receive all messages from the Producer.
        Thread.sleep(60000);
        connection.close();
        System.out.println("[Consumer B] Shutting down.");
    }
}
