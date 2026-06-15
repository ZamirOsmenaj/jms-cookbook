package com.example;

import org.apache.activemq.ActiveMQConnectionFactory;

import javax.jms.*;

/**
 * Producer — sends messages to a JMS Queue.
 *
 * This is the Point-to-Point (P2P) messaging model.
 * The key difference from the Topic/Pub-Sub demo is:
 *
 *   Topic  → every subscriber gets a copy of every message (broadcast)
 *   Queue  → each message is delivered to exactly ONE consumer (round-robin)
 *
 * In practice this means that if Consumer A and Consumer B are both listening
 * on the same queue, the broker will share the messages between them — message #1
 * goes to A, message #2 goes to B, message #3 goes to A, and so on.
 * No consumer ever receives the same message as another consumer.
 *
 * This pattern is ideal for work distribution / load balancing: imagine 10 orders
 * arriving per second and 3 worker processes consuming from the same queue —
 * each order is processed by exactly one worker, and the load is spread evenly.
 */
public class Producer {
    public static void main(String[] args) throws Exception {

        // Read the broker URL from the environment variable set by Docker Compose.
        // Falls back to localhost when running outside Docker.
        String brokerUrl = System.getenv("BROKER_URL") != null
                ? System.getenv("BROKER_URL")
                : "tcp://localhost:61616";

        System.out.println("[Producer] Connecting to broker at: " + brokerUrl);

        // ActiveMQConnectionFactory is the ActiveMQ implementation of the standard
        // JMS ConnectionFactory. It manages the TCP connection to the broker.
        ConnectionFactory factory = new ActiveMQConnectionFactory(brokerUrl);
        Connection connection = null;

        // Retry loop: the broker container may still be initialising when this
        // container starts, even after Docker Compose's healthcheck passes.
        for (int attempt = 1; attempt <= 10; attempt++) {
            try {
                connection = factory.createConnection();
                connection.start(); // activates message delivery on this connection
                System.out.println("[Producer] Connected!");
                break;
            } catch (Exception e) {
                System.out.println("[Producer] Attempt " + attempt + " failed, retrying in 3s...");
                Thread.sleep(3000);
            }
        }

        if (connection == null) {
            System.err.println("[Producer] Could not connect after 10 attempts. Exiting.");
            System.exit(1);
        }

        // Create a non-transactional session with automatic acknowledgement.
        // AUTO_ACKNOWLEDGE: the broker marks a message as consumed as soon as the
        // consumer's onMessage() returns successfully — no manual ack needed.
        Session session = connection.createSession(false, Session.AUTO_ACKNOWLEDGE);

        // *** THE KEY DIFFERENCE FROM THE TOPIC DEMO ***
        // Here we create a Queue instead of a Topic.
        // A Queue holds messages until exactly one consumer picks each one up.
        // The broker decides which consumer gets each message (round-robin by default).
        // No clientID or durable subscription is needed — queues are inherently persistent
        // per message: if no consumer is available the message simply waits in the queue.
        Queue queue = session.createQueue("orders.queue");

        // Bind the producer to our queue. Every message sent through this producer
        // will land in "orders.queue" on the broker.
        MessageProducer producer = session.createProducer(queue);

        // PERSISTENT delivery: the broker writes each message to disk before
        // acknowledging receipt. If the broker restarts, no messages are lost.
        // Use NON_PERSISTENT only when losing messages on restart is acceptable
        // and you need maximum throughput.
        producer.setDeliveryMode(DeliveryMode.PERSISTENT);

        // Brief pause so both consumers have time to connect and start listening
        // before the first message arrives.
        System.out.println("[Producer] Waiting 5s for consumers to be ready...");
        Thread.sleep(5000);

        // Send 10 messages. Watch the output — you will see them split between
        // Consumer A and Consumer B, not duplicated to both.
        for (int i = 1; i <= 10; i++) {
            TextMessage message = session.createTextMessage("Order #" + i + " — please process me!");
            producer.send(message);
            System.out.println("[Producer] Sent: " + message.getText());
            Thread.sleep(1000); // 1 second gap so the distribution is easy to follow
        }

        System.out.println("[Producer] All messages sent. Closing.");
        connection.close();
    }
}
