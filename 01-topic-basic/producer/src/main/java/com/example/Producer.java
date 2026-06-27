package com.example;

import org.apache.activemq.ActiveMQConnectionFactory;

import javax.jms.*;

/**
 * Producer — publishes messages to a JMS Topic.
 *
 * In the Publish-Subscribe (Pub/Sub) model, a Producer does not send messages
 * directly to any specific consumer. Instead, it sends them to a named Topic
 * on the broker. Every consumer that is subscribed to that Topic will receive
 * a copy of every message — the Producer doesn't know (or care) how many
 * consumers are listening.
 *
 * This is the key difference from a Queue (Point-to-Point), where a message
 * is consumed by exactly one consumer.
 */
public class Producer {
    public static void main(String[] args) throws Exception {

        // Read the broker URL from the environment variable injected by Docker Compose.
        // If not set (e.g. running locally outside Docker), fall back to localhost.
        String brokerUrl = System.getenv("BROKER_URL") != null
                ? System.getenv("BROKER_URL")
                : "tcp://localhost:61616";

        System.out.println("Producer connecting to broker at: " + brokerUrl);

        // ActiveMQConnectionFactory is the ActiveMQ-specific implementation of the
        // standard JMS ConnectionFactory interface. It knows how to open a TCP
        // connection to the broker at the given URL.
        ConnectionFactory factory = new ActiveMQConnectionFactory(brokerUrl);
        Connection connection = null;

        // The broker container may still be starting up when this container launches,
        // even with depends_on in docker-compose. We retry a few times to handle that
        // race condition gracefully instead of crashing immediately.
        for (int attempt = 1; attempt <= 10; attempt++) {
            try {
                connection = factory.createConnection();
                connection.start(); // start() activates message delivery on this connection
                System.out.println("Producer connected to broker!");
                break;
            } catch (Exception e) {
                System.out.println("Attempt " + attempt + " failed, retrying in 3s...");
                Thread.sleep(3000);
            }
        }

        if (connection == null) {
            System.err.println("Could not connect to broker after 10 attempts. Exiting.");
            System.exit(1);
        }

        // A Session is a single-threaded context for producing and consuming messages.
        // First argument  (false)              → non-transactional session
        // Second argument (AUTO_ACKNOWLEDGE)   → the broker marks a message as delivered
        //                                        automatically once the consumer receives it,
        //                                        no manual acknowledgement needed
        Session session = connection.createSession(false, Session.AUTO_ACKNOWLEDGE);

        // Create (or look up) a Topic named "news.topic" on the broker.
        // A Topic is the Pub/Sub destination: one message in → every subscriber gets a copy.
        // If you wanted Point-to-Point (one message → one consumer) you would use
        // session.createQueue("news.queue") instead.
        Topic topic = session.createTopic("news.topic");

        // A MessageProducer is bound to a destination (our Topic).
        // Every message sent through this producer will go to "news.topic".
        MessageProducer producer = session.createProducer(topic);

        // NON_PERSISTENT means messages are kept in memory only.
        // If the broker restarts, undelivered messages are lost.
        // Use DeliveryMode.PERSISTENT if you need guaranteed delivery across broker restarts.
        producer.setDeliveryMode(DeliveryMode.NON_PERSISTENT);

        // Give the consumers a moment to finish subscribing before we start sending.
        // Without this pause the first few messages might be sent before the consumers
        // have registered their subscriptions on the broker.
        System.out.println("Waiting 5s for consumers to be ready...");
        Thread.sleep(5000);

        // Send 10 messages, one per second, so you can watch them arrive in real time.
        for (int i = 1; i <= 10; i++) {
            // TextMessage is the simplest JMS message type — it carries a plain String.
            // Other types include BytesMessage, MapMessage, ObjectMessage, StreamMessage.
            TextMessage message = session.createTextMessage("Breaking news #" + i + " - Hello from Producer!");
            producer.send(message);
            System.out.println("[Producer] Sent: " + message.getText());
            Thread.sleep(1000);
        }

        System.out.println("[Producer] All messages sent. Closing.");
        // Always close the connection to release broker-side resources.
        connection.close();
    }
}
