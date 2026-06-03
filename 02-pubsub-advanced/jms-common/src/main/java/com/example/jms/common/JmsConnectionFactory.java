package com.example.jms.common;

import org.apache.activemq.ActiveMQConnectionFactory;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import javax.jms.Connection;
import javax.jms.JMSException;

/**
 * Central place to create JMS {@link Connection} objects.
 *
 * <h3>JMS Connection Hierarchy (important to understand!)</h3>
 * <pre>
 *  ConnectionFactory
 *       └─ Connection          (TCP connection to the broker)
 *            └─ Session        (single-threaded context for sending/receiving)
 *                 ├─ MessageProducer  (sends messages to a Destination)
 *                 └─ MessageConsumer  (receives messages from a Destination)
 * </pre>
 *
 * <p><b>Thread-safety rules:</b>
 * <ul>
 *   <li>ConnectionFactory – thread-safe, share it.</li>
 *   <li>Connection        – thread-safe, share it.</li>
 *   <li>Session           – NOT thread-safe; one per thread.</li>
 *   <li>Producer/Consumer – NOT thread-safe; created per session.</li>
 * </ul>
 */
public class JmsConnectionFactory {

    private static final Logger log = LoggerFactory.getLogger(JmsConnectionFactory.class);

    /**
     * Default broker URL.  Can be overridden via the {@code BROKER_URL}
     * environment variable so Docker containers can point to each other.
     *
     * <p>ActiveMQ URL format: {@code tcp://host:port}
     * The failover transport retries automatically:
     * {@code failover:(tcp://host:61616)?maxReconnectAttempts=5}
     */
    public static final String DEFAULT_BROKER_URL =
            System.getenv().getOrDefault("BROKER_URL", "tcp://localhost:61616");

    /** The JMS topic name all modules share */
    public static final String TOPIC_NAME = "news.events";

    private JmsConnectionFactory() {}

    /**
     * Creates and starts a JMS {@link Connection}.
     *
     * <p>{@code connection.start()} must be called before any consumers
     * can receive messages. Producers can send without starting.
     */
    public static Connection createConnection(String clientId) throws JMSException {
        log.info("Connecting to broker at {} (clientId={})", DEFAULT_BROKER_URL, clientId);

        // ActiveMQConnectionFactory is the ActiveMQ-specific implementation
        // of the JMS-standard ConnectionFactory interface.
        ActiveMQConnectionFactory factory = new ActiveMQConnectionFactory(DEFAULT_BROKER_URL);

        // Trust all packages for ObjectMessage deserialization (dev only!)
        factory.setTrustAllPackages(true);

        Connection connection = factory.createConnection();

        // ClientID is required for durable subscriptions.
        // Must be unique across all active connections.
        connection.setClientID(clientId);

        // Start the connection so message delivery can begin.
        connection.start();

        log.info("Connected successfully (clientId={})", clientId);
        return connection;
    }
}
