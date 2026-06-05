package com.example.jms.subscriber;

import com.example.jms.common.JmsConnectionFactory;
import com.example.jms.common.JsonUtils;
import com.example.jms.common.NewsEvent;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import javax.jms.*;

/**
 * NewsSubscriber demonstrates three subscription styles:
 *
 * <ol>
 *   <li><b>Non-durable</b> – receives messages only while connected.
 *       Missed messages are gone. Good for real-time dashboards.</li>
 *   <li><b>Durable</b> – the broker stores messages while the subscriber
 *       is offline. On reconnect, queued messages are delivered.
 *       Required: unique clientID on Connection + unique subscriptionName.</li>
 *   <li><b>Message Selector</b> – SQL-92 style filter evaluated by the broker
 *       against JMS properties. Only matching messages are delivered.
 *       Example: {@code category = 'SPORTS' AND priority = 'BREAKING'}</li>
 * </ol>
 *
 * <h3>Acknowledgement modes recap</h3>
 * <pre>
 *   AUTO_ACKNOWLEDGE   – broker marks delivered once onMessage() returns normally
 *   CLIENT_ACKNOWLEDGE – consumer calls msg.acknowledge() when ready
 *   DUPS_OK_ACKNOWLEDGE– lazy; broker may redeliver; higher throughput
 *   SESSION_TRANSACTED  – part of a transaction; commit/rollback together
 * </pre>
 */
public class NewsSubscriber implements AutoCloseable {

    private static final Logger log = LoggerFactory.getLogger(NewsSubscriber.class);

    public enum SubscriptionType { NON_DURABLE, DURABLE, FILTERED }

    private final String name;
    private final Connection connection;
    private final Session session;
    private final MessageConsumer consumer;

    /**
     * Creates and starts a subscriber.
     *
     * @param name             human-readable name (also used as durable sub name)
     * @param type             subscription style
     * @param messageSelector  SQL-92 filter string, or null for no filter
     */
    public NewsSubscriber(String name, SubscriptionType type,
                          String messageSelector) throws JMSException {
        this.name = name;

        // Each durable subscriber needs a unique clientID on the Connection.
        // Non-durable subscribers can share a clientID (or omit it).
        String clientId = (type == SubscriptionType.DURABLE)
                ? "durable-client-" + name
                : "nondurable-client-" + name;

        this.connection = JmsConnectionFactory.createConnection(clientId);

        // CLIENT_ACKNOWLEDGE so we control when each message is "done"
        this.session = connection.createSession(false, Session.CLIENT_ACKNOWLEDGE);

        Topic topic = session.createTopic(JmsConnectionFactory.TOPIC_NAME);

        switch (type) {
            case DURABLE:
                // createDurableSubscriber registers a named subscription with
                // the broker. Messages accumulate while we are offline.
                // To cancel a durable subscription: session.unsubscribe(name)
                this.consumer = session.createDurableSubscriber(topic, name);
                log.info("[{}] Created DURABLE subscription (no filter)", name);
                break;

            case FILTERED:
                // Message selector uses JMS property names set by the publisher.
                // Selector is evaluated broker-side – the message body is never
                // transferred if the selector does not match.
                String selector = (messageSelector != null)
                        ? messageSelector
                        : "category = 'SPORTS'";
                this.consumer = session.createConsumer(topic, selector);
                log.info("[{}] Created FILTERED subscription | selector: {}", name, selector);
                break;

            case NON_DURABLE:
            default:
                // Plain subscriber – active only while connected.
                this.consumer = session.createConsumer(topic);
                log.info("[{}] Created NON-DURABLE subscription (no filter)", name);
                break;
        }

        // Register an async MessageListener.
        // The JMS provider calls onMessage() on a provider-managed thread.
        this.consumer.setMessageListener(this::onMessage);
    }

    /**
     * Async callback – called by the JMS provider for each delivered message.
     *
     * <p>IMPORTANT: do not call blocking operations (like session.createProducer)
     * inside this callback – the JMS session is single-threaded.
     */
    private void onMessage(Message message) {
        try {
            if (message instanceof TextMessage) {
                String json = ((TextMessage) message).getText();
                NewsEvent event = JsonUtils.fromJson(json, NewsEvent.class);

                // Log JMS headers alongside our domain data
                log.info("[{}] ← RECEIVED | jmsId={} | priority={} | {}",
                        name,
                        message.getJMSMessageID(),
                        message.getJMSPriority(),
                        event);

                // Manually acknowledge after successful processing.
                // If we crash before this, the broker redelivers (with CLIENT_ACKNOWLEDGE).
                message.acknowledge();

            } else {
                log.warn("[{}] Unexpected message type: {}", name, message.getClass().getSimpleName());
            }
        } catch (JMSException e) {
            log.error("[{}] Error processing message: {}", name, e.getMessage(), e);
        }
    }

    @Override
    public void close() {
        try {
            consumer.close();
            session.close();
            connection.close();
            log.info("[{}] Subscriber closed cleanly.", name);
        } catch (JMSException e) {
            log.warn("[{}] Error closing subscriber: {}", name, e.getMessage());
        }
    }
}
