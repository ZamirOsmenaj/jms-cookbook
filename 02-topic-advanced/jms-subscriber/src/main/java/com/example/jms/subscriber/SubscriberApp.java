package com.example.jms.subscriber;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.ArrayList;
import java.util.List;

/**
 * Launches three concurrent subscribers to demonstrate the different
 * JMS subscription models side-by-side.
 *
 * <pre>
 *  Subscriber         | Type        | Filter
 *  -------------------|-------------|------------------------------
 *  all-news           | NON_DURABLE | none  (receives everything)
 *  durable-all-news   | DURABLE     | none  (survives disconnects)
 *  sports-breaking    | FILTERED    | category='SPORTS' AND priority='BREAKING'
 *  breaking-news      | FILTERED    | priority='BREAKING'
 * </pre>
 *
 * <p>Watch the logs: "all-news" and "durable-all-news" receive every event;
 * "sports-breaking" only gets sports breaking news;
 * "breaking-news" gets breaking news from any category.
 *
 * <h3>How to run (without Docker)</h3>
 * <pre>
 *   export BROKER_URL=tcp://localhost:61616
 *   java -jar subscriber/target/subscriber.jar
 * </pre>
 */
public class SubscriberApp {

    private static final Logger log = LoggerFactory.getLogger(SubscriberApp.class);

    public static void main(String[] args) throws Exception {
        log.info("=== JMS News Subscriber Starting ===");

        List<NewsSubscriber> subscribers = new ArrayList<>();

        // 1. Non-durable – receives all messages while connected
        subscribers.add(new NewsSubscriber(
                "all-news",
                NewsSubscriber.SubscriptionType.NON_DURABLE,
                null));

        // 2. Durable – receives all messages; broker queues while offline
        subscribers.add(new NewsSubscriber(
                "durable-all-news",
                NewsSubscriber.SubscriptionType.DURABLE,
                null));

        // 3. Filtered – only BREAKING sports news
        subscribers.add(new NewsSubscriber(
                "sports-breaking",
                NewsSubscriber.SubscriptionType.FILTERED,
                "category = 'SPORTS' AND priority = 'BREAKING'"));

        // 4. Filtered – all breaking news regardless of category
        subscribers.add(new NewsSubscriber(
                "breaking-news",
                NewsSubscriber.SubscriptionType.FILTERED,
                "priority = 'BREAKING'"));

        log.info("All {} subscribers active. Waiting for messages...", subscribers.size());
        log.info("(Press Ctrl+C to stop)");

        // Clean shutdown on Ctrl+C / Docker stop
        Runtime.getRuntime().addShutdownHook(new Thread(() -> {
            log.info("Shutdown signal received – closing subscribers...");
            subscribers.forEach(NewsSubscriber::close);
        }));

        // Keep the main thread alive; message delivery happens on JMS threads
        Thread.currentThread().join();
    }
}
