package com.example.jms.producer;

import com.example.jms.common.OrderRequest;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.concurrent.TimeUnit;

/**
 * Continuously sends a mix of {@link OrderRequest} messages to demonstrate:
 * <ul>
 *   <li>Priority ordering (VIP vs STANDARD)</li>
 *   <li>Message selectors (orderType property)</li>
 *   <li>TTL / expiry (FOOD orders expire after 30 s)</li>
 *   <li>Request/reply (VIP orders set JMSReplyTo)</li>
 * </ul>
 *
 * <p>Watch the consumer logs to see that:
 * <ol>
 *   <li>VIP orders are always consumed before STANDARD orders queued at the same time</li>
 *   <li>Each order is consumed by exactly ONE consumer (load balancing)</li>
 *   <li>Replies flow back to the producer for VIP orders</li>
 * </ol>
 */
public class ProducerApp {

    private static final Logger log = LoggerFactory.getLogger(ProducerApp.class);

    private static final OrderRequest[] SAMPLE_ORDERS = {
        // VIP customers — priority 9, request/reply enabled
        OrderRequest.create("CUST-001", "VIP",      "ELECTRONICS", 1299.99, "MacBook Pro 16-inch"),
        OrderRequest.create("CUST-002", "VIP",      "CLOTHING",     249.99, "Designer winter coat"),
        OrderRequest.create("CUST-003", "STANDARD", "FOOD",          34.50, "Weekly grocery box"),
        OrderRequest.create("CUST-004", "STANDARD", "ELECTRONICS",  499.00, "Gaming headset + controller"),
        OrderRequest.create("CUST-005", "VIP",      "FURNITURE",   1850.00, "Ergonomic office chair"),
        OrderRequest.create("CUST-006", "STANDARD", "FOOD",          12.99, "Pizza x3 delivery"),
        OrderRequest.create("CUST-007", "STANDARD", "CLOTHING",      89.00, "Running shoes"),
        OrderRequest.create("CUST-008", "VIP",      "ELECTRONICS",  329.00, "Noise-cancelling headphones"),
        OrderRequest.create("CUST-009", "STANDARD", "FURNITURE",    620.00, "Bookshelf (self-assembly)"),
        OrderRequest.create("CUST-010", "STANDARD", "FOOD",          22.75, "Sushi platter"),
        OrderRequest.create("CUST-011", "VIP",      "CLOTHING",     599.00, "Tailored suit"),
        OrderRequest.create("CUST-012", "STANDARD", "ELECTRONICS",  149.00, "Bluetooth speaker"),
    };

    public static void main(String[] args) throws Exception {
        log.info("=== JMS Order Producer Starting ===");

        long intervalMs = Long.parseLong(
                System.getenv().getOrDefault("PRODUCE_INTERVAL_MS", "2000"));
        int maxRounds = Integer.parseInt(
                System.getenv().getOrDefault("MAX_ROUNDS", "-1"));

        Runtime.getRuntime().addShutdownHook(new Thread(
                () -> log.info("Producer shutting down...")));

        try (OrderProducer producer = new OrderProducer()) {
            int round = 0;
            while (maxRounds == -1 || round < maxRounds) {
                log.info("--- Round {} ---", round + 1);
                for (OrderRequest order : SAMPLE_ORDERS) {
                    producer.sendOrder(order);
                    TimeUnit.MILLISECONDS.sleep(intervalMs);
                }
                round++;
            }
        }

        log.info("=== Producer finished ===");
    }
}
