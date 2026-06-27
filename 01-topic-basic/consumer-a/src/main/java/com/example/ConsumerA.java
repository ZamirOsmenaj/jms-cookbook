package com.example;

import org.apache.activemq.ActiveMQConnectionFactory;

import javax.jms.*;

/**
 * ConsumerA — subscribes to a JMS Topic and receives every published message.
 *
 * In the Pub/Sub model, any number of consumers can subscribe to the same Topic.
 * Each subscriber gets its own independent copy of every message — unlike a Queue
 * where a message is consumed by exactly one receiver.
 *
 * This consumer uses a *durable* subscription. A durable subscription tells the
 * broker "remember me by name — if I disconnect temporarily, hold any messages
 * that arrive while I'm gone and deliver them when I reconnect." A non-durable
 * subscription would miss all messages sent while the consumer was offline.
 */
public class ConsumerA {
    public static void main(String[] args) throws Exception {

        // Read the broker URL from the environment variable injected by Docker Compose.
        // Falls back to localhost when running outside Docker.
        String brokerUrl = System.getenv("BROKER_URL") != null
                ? System.getenv("BROKER_URL")
                : "tcp://localhost:61616";

        System.out.println("[Consumer A] Connecting to broker at: " + brokerUrl);

        ConnectionFactory factory = new ActiveMQConnectionFactory(brokerUrl);
        Connection connection = null;

        // Retry loop — the broker container might not be fully ready yet even after
        // Docker Compose's depends_on condition passes. Retrying avoids a hard crash.
        for (int attempt = 1; attempt <= 10; attempt++) {
            try {
                connection = factory.createConnection();

                // A clientID uniquely identifies this connection on the broker.
                // It is REQUIRED for durable subscriptions — the broker uses it to
                // track which messages to hold when this consumer is offline.
                // Every consumer in the system must have a different clientID.
                connection.setClientID("ConsumerA");

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
        // AUTO_ACKNOWLEDGE means the broker considers a message delivered as soon as
        // this consumer's onMessage() method returns without throwing an exception.
        Session session = connection.createSession(false, Session.AUTO_ACKNOWLEDGE);

        // Look up (or create) the same Topic the Producer is sending to.
        // The name "news.topic" must match exactly what the Producer uses.
        Topic topic = session.createTopic("news.topic");

        // createDurableSubscriber registers a named, persistent subscription ("subA").
        // The broker will retain undelivered messages for this subscription even when
        // ConsumerA is not running. To cancel the subscription entirely you would call
        // session.unsubscribe("subA").
        TopicSubscriber subscriber = session.createDurableSubscriber(topic, "subA");

        System.out.println("[Consumer A] Subscribed and waiting for messages...");

        // MessageListener is the async (push) way to receive messages in JMS.
        // The broker calls onMessage() on a separate thread each time a message arrives.
        // The alternative is the synchronous (pull) approach: subscriber.receive().
        subscriber.setMessageListener(msg -> {
            if (msg instanceof TextMessage) {
                try {
                    System.out.println("[Consumer A] >>> Received: " + ((TextMessage) msg).getText());
                } catch (JMSException e) {
                    e.printStackTrace();
                }
            }
        });

        // Keep the main thread alive so the listener thread keeps running.
        // 60 seconds is enough to receive all 10 messages from the Producer.
        Thread.sleep(60000);
        connection.close();
        System.out.println("[Consumer A] Done.");
    }
}
