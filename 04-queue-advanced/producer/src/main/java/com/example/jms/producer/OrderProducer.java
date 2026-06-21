package com.example.jms.producer;

import com.example.jms.common.BrokerConfig;
import com.example.jms.common.JsonUtils;
import com.example.jms.common.OrderReply;
import com.example.jms.common.OrderRequest;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import javax.jms.*;
import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;

/**
 * OrderProducer sends {@link OrderRequest} messages to the orders queue.
 *
 * <h3>Concepts demonstrated</h3>
 *
 * <b>1. Message Priority</b><br>
 * JMS supports priorities 0–9 (default 4). The broker delivers higher-priority
 * messages first within the same queue. VIP customers get priority 9.
 *
 * <b>2. JMS Message Properties (for Selectors)</b><br>
 * We attach {@code customerTier} and {@code orderType} as JMS properties so
 * specialised consumers can filter using SQL-92 selectors, e.g.:
 * <pre>  customerTier = 'VIP'</pre>
 * <pre>  orderType = 'ELECTRONICS' OR orderType = 'FURNITURE'</pre>
 *
 * <b>3. Request / Reply Pattern</b><br>
 * For VIP orders we demonstrate the JMS request/reply pattern:
 * <ul>
 *   <li>Set {@code JMSReplyTo} to our reply queue</li>
 *   <li>Set {@code JMSCorrelationID} to the orderId</li>
 *   <li>Consumer sends a reply back to {@code JMSReplyTo}</li>
 *   <li>Producer matches reply via {@code JMSCorrelationID}</li>
 * </ul>
 *
 * <b>4. Message Expiry (TTL)</b><br>
 * Time-sensitive orders expire after a configurable TTL. Expired messages
 * go to the broker's expiry destination rather than being processed.
 */
public class OrderProducer implements AutoCloseable {

    private static final Logger log = LoggerFactory.getLogger(OrderProducer.class);

    /** TTL for FOOD orders – 30 seconds (they must be processed quickly) */
    private static final long FOOD_TTL_MS = 30_000;

    private final Connection connection;
    private final Session session;
    private final MessageProducer producer;
    private final Queue ordersQueue;
    private final Queue replyQueue;
    private final MessageConsumer replyConsumer;

    /** Tracks pending request/reply correlations: orderId → sent message */
    private final Map<String, OrderRequest> pendingReplies = new ConcurrentHashMap<>();

    public OrderProducer() throws JMSException {
        this.connection  = BrokerConfig.createConnection("order-producer");
        // transacted=false, AUTO_ACKNOWLEDGE for the producer session
        this.session     = connection.createSession(false, Session.AUTO_ACKNOWLEDGE);
        this.ordersQueue = session.createQueue(BrokerConfig.ORDERS_QUEUE);

        // Named reply queue — permanent so replies survive a producer restart.
        // Alternative: session.createTemporaryQueue() for single-connection lifetime.
        this.replyQueue  = session.createQueue(BrokerConfig.REPLY_QUEUE);

        // Unbound producer (no fixed destination) — we set destination per-send
        // so we can send to different queues from one producer if needed.
        this.producer = session.createProducer(null);
        this.producer.setDeliveryMode(DeliveryMode.PERSISTENT);

        // Listen for replies on the reply queue
        this.replyConsumer = session.createConsumer(replyQueue);
        this.replyConsumer.setMessageListener(this::onReply);

        log.info("OrderProducer ready → queue: {}", BrokerConfig.ORDERS_QUEUE);
    }

    /**
     * Sends an order to the queue, applying priority and TTL automatically.
     */
    public void sendOrder(OrderRequest order) throws JMSException {
        TextMessage message = session.createTextMessage(JsonUtils.toJson(order));

        // ── JMS Properties (broker-indexed, used for selectors) ──────────
        message.setStringProperty("customerTier", order.getCustomerTier());
        message.setStringProperty("orderType",    order.getOrderType());
        message.setStringProperty("orderId",      order.getOrderId());

        // ── JMS Priority ─────────────────────────────────────────────────
        // VIP → 9 (highest), STANDARD → 4 (default)
        int priority = "VIP".equals(order.getCustomerTier()) ? 9 : 4;

        // ── TTL (Message Expiry) ─────────────────────────────────────────
        // FOOD orders must be consumed within 30 s or they expire to DLQ.
        long ttl = "FOOD".equals(order.getOrderType()) ? FOOD_TTL_MS : 0; // 0 = never

        // ── Request/Reply: set JMSReplyTo for VIP orders ─────────────────
        if ("VIP".equals(order.getCustomerTier())) {
            message.setJMSReplyTo(replyQueue);
            // JMSCorrelationID links reply back to this request
            message.setJMSCorrelationID(order.getOrderId());
            pendingReplies.put(order.getOrderId(), order);
            log.info("SEND [VIP/ReplyTo={}] priority={} → {}",
                    BrokerConfig.REPLY_QUEUE, priority, order);
        } else {
            log.info("SEND [{}] priority={} ttl={}ms → {}",
                    order.getCustomerTier(), priority, ttl, order);
        }

        // Send to the orders queue with explicit priority and TTL
        producer.send(ordersQueue, message, DeliveryMode.PERSISTENT, priority, ttl);
    }

    /**
     * Async callback when a reply arrives on the reply queue.
     * Matches the reply to the original request via JMSCorrelationID.
     */
    private void onReply(Message message) {
        try {
            String correlationId = message.getJMSCorrelationID();
            String json = ((TextMessage) message).getText();
            OrderReply reply = JsonUtils.fromJson(json, OrderReply.class);

            OrderRequest original = pendingReplies.remove(correlationId);
            if (original != null) {
                log.info("REPLY RECEIVED ← {} (matched to: {})", reply, original.getOrderId());
            } else {
                log.warn("REPLY for unknown correlationId: {}", correlationId);
            }
        } catch (JMSException e) {
            log.error("Error processing reply: {}", e.getMessage(), e);
        }
    }

    @Override
    public void close() {
        try {
            replyConsumer.close();
            producer.close();
            session.close();
            connection.close();
            log.info("OrderProducer closed.");
        } catch (JMSException e) {
            log.warn("Error closing producer: {}", e.getMessage());
        }
    }
}
