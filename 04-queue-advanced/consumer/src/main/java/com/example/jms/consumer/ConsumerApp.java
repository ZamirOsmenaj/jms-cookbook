package com.example.jms.consumer;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.Executors;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.TimeUnit;

/**
 * Launches multiple concurrent consumers to demonstrate the key
 * Queue behaviours that make it fundamentally different from a Topic.
 *
 * <pre>
 *  Consumer              | Selector                              | Role
 *  ----------------------|---------------------------------------|----------------------------
 *  general-worker-1      | none                                  | Competing consumer (all orders)
 *  general-worker-2      | none                                  | Competing consumer (all orders)
 *  electronics-specialist| orderType = 'ELECTRONICS'             | Selective consumer
 *  vip-handler           | customerTier = 'VIP'                  | VIP-only (sends replies)
 * </pre>
 *
 * <h3>What to observe</h3>
 * <ul>
 *   <li>Each order is processed by EXACTLY ONE consumer — never duplicated</li>
 *   <li>general-worker-1 and general-worker-2 share the load between them</li>
 *   <li>electronics-specialist only sees ELECTRONICS orders; others stay in queue</li>
 *   <li>vip-handler processes VIP orders first (priority 9) and sends replies</li>
 *   <li>Simulated failures trigger rollback → redelivery → eventually DLQ</li>
 * </ul>
 */
public class ConsumerApp {

    private static final Logger log = LoggerFactory.getLogger(ConsumerApp.class);

    public static void main(String[] args) throws Exception {
        log.info("=== JMS Order Consumer Starting ===");

        List<OrderConsumer> consumers = new ArrayList<>();

        // 1 & 2. Two general workers — competing consumers, no selector.
        //        The broker load-balances across them automatically.
        consumers.add(new OrderConsumer("general-worker-1", null));
        consumers.add(new OrderConsumer("general-worker-2", null));

        // 3. Specialist: only picks up ELECTRONICS orders.
        //    Other order types remain in the queue for general workers.
        consumers.add(new OrderConsumer("electronics-specialist",
                "orderType = 'ELECTRONICS'"));

        // 4. VIP handler: only picks up VIP customer orders.
        //    Because VIP messages have priority=9 they jump the queue,
        //    so this consumer always has work ready before STANDARD orders.
        consumers.add(new OrderConsumer("vip-handler",
                "customerTier = 'VIP'"));

        log.info("All {} consumers active. Waiting for orders...", consumers.size());
        log.info("(Press Ctrl+C to stop)\n");

        // Print a stats summary every 15 seconds
        ScheduledExecutorService scheduler = Executors.newSingleThreadScheduledExecutor();
        scheduler.scheduleAtFixedRate(() -> {
            log.info("── Stats ──────────────────────────────────────────────");
            consumers.forEach(c ->
                    log.info("  {} | processed={} failed={}",
                            c.getClass().getSimpleName(),
                            c.getProcessedCount(),
                            c.getFailedCount()));
            log.info("───────────────────────────────────────────────────────");
        }, 15, 15, TimeUnit.SECONDS);

        Runtime.getRuntime().addShutdownHook(new Thread(() -> {
            log.info("Shutdown signal — closing consumers...");
            scheduler.shutdownNow();
            consumers.forEach(OrderConsumer::close);
        }));

        Thread.currentThread().join();
    }
}
