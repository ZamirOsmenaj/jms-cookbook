package com.example.jms.publisher;

import com.example.jms.common.JmsConnectionFactory;
import com.example.jms.common.JsonUtils;
import com.example.jms.common.NewsEvent;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import javax.jms.*;

/**
 * NewsPublisher demonstrates how to publish messages to a JMS Topic.
 *
 * <h3>Topic vs Queue – the core distinction</h3>
 * <pre>
 *  QUEUE  (Point-to-Point / P2P)
 *    - One producer  →  one consumer
 *    - Messages are load-balanced across competing consumers
 *    - Message is removed once consumed
 *
 *  TOPIC  (Publish-Subscribe)
 *    - One producer  →  MANY consumers
 *    - Every active subscriber receives a copy
 *    - Durable subscribers receive messages even when offline
 * </pre>
 *
 * <h3>JMS Message Properties (used for Selectors)</h3>
 * <p>We attach {@code category} and {@code priority} as JMS properties
 * so subscribers can filter without deserializing the JSON body.
 * Example selector: {@code category = 'SPORTS' AND priority = 'BREAKING'}
 */
public class NewsPublisher implements AutoCloseable {

    private static final Logger log = LoggerFactory.getLogger(NewsPublisher.class);

    private final Connection connection;
    private final Session session;
    private final MessageProducer producer;
    private final Topic topic;

    public NewsPublisher() throws JMSException {
        // Step 1 – Obtain a Connection
        this.connection = JmsConnectionFactory.createConnection("publisher-1");

        // Step 2 – Create a Session
        // Parameters:
        //   transacted  = false  (no transactions; each send is immediately committed)
        //   acknowledgeMode = AUTO_ACKNOWLEDGE (broker removes msg once delivered)
        //
        // Other acknowledge modes:
        //   CLIENT_ACKNOWLEDGE – consumer calls message.acknowledge() manually
        //   DUPS_OK_ACKNOWLEDGE – lazy ack, may redeliver; better throughput
        this.session = connection.createSession(false, Session.AUTO_ACKNOWLEDGE);

        // Step 3 – Look up / create the Destination (a Topic)
        // In production you'd look this up via JNDI; here we create it directly.
        this.topic = session.createTopic(JmsConnectionFactory.TOPIC_NAME);

        // Step 4 – Create a MessageProducer bound to the topic
        this.producer = session.createProducer(topic);

        // DeliveryMode.PERSISTENT  → broker saves msg to disk; survives restart
        // DeliveryMode.NON_PERSISTENT → faster but lost on broker crash
        this.producer.setDeliveryMode(DeliveryMode.PERSISTENT);

        log.info("NewsPublisher ready. Publishing to topic: {}", JmsConnectionFactory.TOPIC_NAME);
    }

    /**
     * Publishes a {@link NewsEvent} as a JMS {@link TextMessage}.
     *
     * <p>We also set JMS message properties so subscribers can use
     * <em>Message Selectors</em> to filter without reading the JSON body.
     */
    public void publish(NewsEvent event) throws JMSException {
        // Serialize the domain object to JSON
        String json = JsonUtils.toJson(event);

        // Create a TextMessage (most portable JMS message type)
        TextMessage message = session.createTextMessage(json);

        // --- JMS Message Headers (set automatically by the broker/API) ---
        // JMSMessageID    – unique ID assigned by broker
        // JMSTimestamp    – time of send
        // JMSDestination  – where it's going
        // JMSExpiration   – TTL (0 = never expire)
        // JMSPriority     – 0 (lowest) to 9 (highest); default = 4

        // --- Custom JMS Properties (used for Message Selectors) ---
        // These are indexed by the broker and queryable by consumers.
        message.setStringProperty("category", event.getCategory());
        message.setStringProperty("priority", event.getPriority());
        message.setStringProperty("eventId", event.getId());

        // Set JMS Priority (0-9).  Map our domain priorities to JMS priorities.
        int jmsPriority = mapPriority(event.getPriority());
        producer.setPriority(jmsPriority);

        // Send!
        producer.send(message);

        log.info("Published → {} | msgId={}", event, message.getJMSMessageID());
    }

    private int mapPriority(String priority) {
        switch (priority.toUpperCase()) {
            case "BREAKING": return 9;
            case "HIGH":     return 7;
            case "MEDIUM":   return 4;
            default:         return 1;
        }
    }

    @Override
    public void close() {
        try {
            producer.close();
            session.close();
            connection.close();
            log.info("NewsPublisher closed cleanly.");
        } catch (JMSException e) {
            log.warn("Error closing publisher: {}", e.getMessage());
        }
    }
}
