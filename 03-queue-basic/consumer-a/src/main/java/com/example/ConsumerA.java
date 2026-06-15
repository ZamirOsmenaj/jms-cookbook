package com.example;

import org.apache.activemq.ActiveMQConnectionFactory;

import javax.jms.*;

/**
 * ConsumerA — one of two competing consumers on the same Queue.
 *
 * In the Point-to-Point (Queue) model, multiple consumers can listen on the
 * same queue simultaneously. The broker distributes messages between them
 * using round-robin: Consumer A gets message #1, Consumer B gets message #2,
 * Consumer A gets message #3, and so on.
 *
 * This is fundamentally different from the Topic/Pub-Sub demo where BOTH
 * consumers received EVERY message. Here, each message is processed by
 * exactly one consumer — the two consumers are competing, not duplicating.
 *
 * Notice what is NOT needed compared to the Topic demo:
 *   - No clientID on the connection  (clientIDs are only required for durable topic subscriptions)
 *   - No TopicSubscriber             (a plain MessageConsumer is sufficient for queues)
 *   - No durable subscription name  (queues are inherently persistent — messages
 *                                    wait in the queue until a consumer picks them up,
 *                                    regardless of whether any consumer is currently connected)
 */
public class ConsumerA {
    public static void main(String[] args) throws Exception {

        // Read broker URL from the environment variable set by Docker Compose.
        // Falls back to localhost when running outside Docker.
        String brokerUrl = System.getenv("BROKER_URL") != null
                ? System.getenv("BROKER_URL")
                : "tcp://localhost:61616";

        System.out.println("[Consumer A] Connecting to broker at: " + brokerUrl);

        ConnectionFactory factory = new ActiveMQConnectionFactory(brokerUrl);
        Connection connection = null;

        // Retry loop: the broker might still be starting even after Docker Compose
        // reports it healthy. We retry rather than crash immediately.
        for (int attempt = 1; attempt <= 10; attempt++) {
            try {
                connection = factory.createConnection();
                // No setClientID() needed here — that is only required for durable
                // topic subscriptions, not for queue consumers.
                connection.start();
                System.out.println("[Consumer A] Connected!");
                break;
            } catch (Exception e) {
                System.out.println("[Consumer A] Attempt " + attempt + " failed, retrying in 3s...");
                Thread.sleep(3000);
            }
        }

        if (connection == null) {
            System.err.println("[Consumer A] Could not connect. Exiting.");
            System.exit(1);
        }

        // Non-transactional session with automatic acknowledgement.
        // With AUTO_ACKNOWLEDGE the broker removes a message from the queue as soon
        // as this consumer's onMessage() method returns successfully.
        // If you need to guarantee processing (e.g. write to a DB before acking),
        // use Session.CLIENT_ACKNOWLEDGE and call message.acknowledge() yourself,
        // or use a transacted session so the broker only removes the message when
        // you commit the transaction.
        Session session = connection.createSession(false, Session.AUTO_ACKNOWLEDGE);

        // Connect to the same queue the Producer is sending to.
        // The name must match exactly — "orders.queue" here and in Producer.java.
        Queue queue = session.createQueue("orders.queue");

        // A standard MessageConsumer is all we need for a queue.
        // Both ConsumerA and ConsumerB create a MessageConsumer on the same queue;
        // the broker automatically load-balances messages between them.
        MessageConsumer consumer = session.createConsumer(queue);

        System.out.println("[Consumer A] Listening on orders.queue — waiting for messages...");

        // Asynchronous listener: the broker pushes messages to this callback as they
        // arrive. We do not need to poll or call receive() in a loop.
        // Simulate some processing time with a short sleep so the round-robin
        // distribution between A and B is easy to observe in the logs.
        consumer.setMessageListener(msg -> {
            if (msg instanceof TextMessage) {
                try {
                    System.out.println("[Consumer A] >>> Processing: " + ((TextMessage) msg).getText());
                    Thread.sleep(500); // simulate work (e.g. saving to a database)
                    System.out.println("[Consumer A] >>> Done.");
                } catch (Exception e) {
                    e.printStackTrace();
                }
            }
        });

        // Keep the main thread alive so the listener thread keeps running.
        // 60 seconds is more than enough for the Producer to finish sending 10 messages.
        Thread.sleep(60000);
        connection.close();
        System.out.println("[Consumer A] Shutting down.");
    }
}
