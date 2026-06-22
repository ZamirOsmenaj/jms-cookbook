package com.example.jms.consumer;

import com.example.jms.common.BrokerConfig;
import com.example.jms.common.JsonUtils;
import com.example.jms.common.OrderReply;
import com.example.jms.common.OrderRequest;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import javax.jms.*;
import java.util.Random;
import java.util.concurrent.atomic.AtomicInteger;

/**
 * OrderConsumer pulls messages from the orders queue and processes them.
 *
 * <h3>Competing Consumers (load balancing)</h3>
 * Multiple instances of this class connect to the SAME queue.
 * The broker delivers each message to exactly ONE consumer — whichever
 * is free first. This is fundamentally different from Topics where every
 * subscriber gets every message.
 *
 * <h3>Transacted Session</h3>
 * We use {@code transacted=true} so that message acknowledgement and
 * any business logic are atomic:
 * <pre>
 *   session.commit()   → message removed from queue (success path)
 *   session.rollback() → message redelivered to queue (failure path)
 * </pre>
 * After N rollbacks the broker moves the message to the Dead-Letter Queue.
 *
 * <h3>Message Selector</h3>
 * An optional selector lets this consumer specialise (e.g. only ELECTRONICS).
 * Unmatched messages stay in the queue for other consumers.
 *
 * <h3>Request / Reply</h3>
 * If the message has a {@code JMSReplyTo} header, we send an
 * {@link OrderReply} back to that destination.
 */
public class OrderConsumer implements AutoCloseable {

    private static final Logger log = LoggerFactory.getLogger(OrderConsumer.class);

    private static final Random RANDOM = new Random();

    /** Simulated processing time range (ms) */
    private static final int PROCESS_MIN_MS = 500;
    private static final int PROCESS_MAX_MS = 1500;

    private final String name;
    private final Connection connection;

    /**
     * Transacted session — commit/rollback control acknowledgement.
     * One session = one thread (JMS rule).
     */
    private final Session session;
    private final MessageConsumer consumer;
    private final AtomicInteger processedCount = new AtomicInteger(0);
    private final AtomicInteger failedCount    = new AtomicInteger(0);

    /**
     * @param name     consumer identifier shown in logs
     * @param selector SQL-92 message selector, or {@code null} to receive all orders
     */
    public OrderConsumer(String name, String selector) throws JMSException {
        this.name = name;

        this.connection = BrokerConfig.createConnection(null); // no clientId needed for queues

        // transacted=true — we manually commit() or rollback() each message.
        // The second parameter is ignored when transacted=true.
        this.session = connection.createSession(true, Session.SESSION_TRANSACTED);

        Queue queue = session.createQueue(BrokerConfig.ORDERS_QUEUE);

        if (selector != null && !selector.isEmpty()) {
            this.consumer = session.createConsumer(queue, selector);
            log.info("[{}] Listening on queue '{}' | selector: {}",
                    name, BrokerConfig.ORDERS_QUEUE, selector);
        } else {
            this.consumer = session.createConsumer(queue);
            log.info("[{}] Listening on queue '{}' | no selector (all orders)",
                    name, BrokerConfig.ORDERS_QUEUE);
        }

        this.consumer.setMessageListener(this::onMessage);
    }

    private void onMessage(Message message) {
        String orderId = "UNKNOWN";
        try {
            orderId = message.getStringProperty("orderId");
            String json = ((TextMessage) message).getText();
            OrderRequest order = JsonUtils.fromJson(json, OrderRequest.class);

            log.info("[{}] RECEIVED → {} | redelivered={}",
                    name, order, message.getJMSRedelivered());

            // Simulate processing work
            int workMs = PROCESS_MIN_MS + RANDOM.nextInt(PROCESS_MAX_MS - PROCESS_MIN_MS);
            Thread.sleep(workMs);

            // Simulate occasional failures (10% chance) — triggers redelivery
            if (RANDOM.nextInt(10) == 0) {
                throw new RuntimeException("Simulated processing error for order " + order.getOrderId());
            }

            // ── Success path ────────────────────────────────────────────
            int count = processedCount.incrementAndGet();
            log.info("[{}] PROCESSED #{} in {}ms → {}", name, count, workMs, order.getOrderId());

            // Send reply if producer requested one (request/reply pattern)
            sendReplyIfRequested(message, order);

            // Commit the transaction → message permanently removed from queue
            session.commit();

        } catch (InterruptedException e) {
            Thread.currentThread().interrupt();
        } catch (Exception e) {
            failedCount.incrementAndGet();
            log.warn("[{}] FAILED to process order {} — rolling back for redelivery: {}",
                    name, orderId, e.getMessage());
            try {
                // Rollback → broker will redeliver after backoff delay.
                // After maxRedeliveries the broker sends to DLQ.
                session.rollback();
            } catch (JMSException rollbackEx) {
                log.error("[{}] Rollback failed: {}", name, rollbackEx.getMessage());
            }
        }
    }

    /**
     * Sends an {@link OrderReply} back to the {@code JMSReplyTo} destination.
     * Only called when the producer set the {@code JMSReplyTo} header.
     */
    private void sendReplyIfRequested(Message original, OrderRequest order)
            throws JMSException {
        Destination replyTo = original.getJMSReplyTo();
        if (replyTo == null) return;

        OrderReply reply = OrderReply.success(
                order.getOrderId(), name,
                "Order fulfilled in " + name);

        // Use a separate non-transacted session for the reply so it is sent
        // immediately and not held back until the main session commits.
        // (In production you'd manage this more carefully or use one session.)
        Session replySession = connection.createSession(false, Session.AUTO_ACKNOWLEDGE);
        try {
            MessageProducer replyProducer = replySession.createProducer(replyTo);
            TextMessage replyMsg = replySession.createTextMessage(JsonUtils.toJson(reply));
            // Echo the correlationID back so the producer can match it
            replyMsg.setJMSCorrelationID(original.getJMSCorrelationID());
            replyProducer.send(replyMsg);
            replyProducer.close();
            log.info("[{}] REPLY SENT → correlationId={}", name, original.getJMSCorrelationID());
        } finally {
            replySession.close();
        }
    }

    public int getProcessedCount() { return processedCount.get(); }
    public int getFailedCount()    { return failedCount.get(); }

    @Override
    public void close() {
        try {
            consumer.close();
            session.close();
            connection.close();
            log.info("[{}] Closed. processed={} failed={}",
                    name, processedCount.get(), failedCount.get());
        } catch (JMSException e) {
            log.warn("[{}] Error closing: {}", name, e.getMessage());
        }
    }
}
