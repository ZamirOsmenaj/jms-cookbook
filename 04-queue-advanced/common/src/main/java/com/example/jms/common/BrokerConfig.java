package com.example.jms.common;

import org.apache.activemq.ActiveMQConnectionFactory;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import javax.jms.Connection;
import javax.jms.JMSException;

/**
 * Shared constants and connection factory for all modules.
 *
 * <h3>Queue naming conventions</h3>
 * <pre>
 *   orders.queue        – main order processing queue (all consumers)
 *   orders.reply        – replies sent back to producers (request/reply)
 *   orders.dlq          – dead-letter queue (failed after max redeliveries)
 * </pre>
 *
 * <h3>Key Queue behaviours</h3>
 * <ul>
 *   <li><b>Competing consumers:</b> multiple consumers on the same queue
 *       share the load — each message goes to exactly ONE consumer.</li>
 *   <li><b>Persistence:</b> PERSISTENT messages survive a broker restart.</li>
 *   <li><b>Redelivery:</b> if a consumer fails (session rollback or crash),
 *       the broker redelivers after a backoff delay.</li>
 *   <li><b>Dead-letter queue:</b> after N failed redeliveries the broker
 *       moves the message to a DLQ instead of discarding it.</li>
 *   <li><b>Priority:</b> higher-priority messages are delivered first
 *       (0 = lowest, 9 = highest, default = 4).</li>
 * </ul>
 */
public class BrokerConfig {

    private static final Logger log = LoggerFactory.getLogger(BrokerConfig.class);

    public static final String BROKER_URL =
            System.getenv().getOrDefault("BROKER_URL", "tcp://localhost:61616");

    /** Main order processing queue */
    public static final String ORDERS_QUEUE = "orders.queue";

    /** Reply-to queue for the request/reply demo */
    public static final String REPLY_QUEUE  = "orders.reply";

    private BrokerConfig() {}

    public static Connection createConnection(String clientId) throws JMSException {
        log.info("Connecting to broker at {} (clientId={})", BROKER_URL, clientId);
        ActiveMQConnectionFactory factory = new ActiveMQConnectionFactory(BROKER_URL);
        factory.setTrustAllPackages(true);

        // Redelivery policy: retry up to 3 times with 2-second backoff,
        // then send to DLQ.  Configured here so all clients share the policy.
        org.apache.activemq.RedeliveryPolicy policy = new org.apache.activemq.RedeliveryPolicy();
        policy.setMaximumRedeliveries(3);
        policy.setInitialRedeliveryDelay(2000);
        policy.setBackOffMultiplier(2);
        policy.setUseExponentialBackOff(true);
        factory.setRedeliveryPolicy(policy);

        Connection connection = factory.createConnection();
        if (clientId != null && !clientId.isEmpty()) {
            connection.setClientID(clientId);
        }
        connection.start();
        log.info("Connected (clientId={})", clientId);
        return connection;
    }
}
