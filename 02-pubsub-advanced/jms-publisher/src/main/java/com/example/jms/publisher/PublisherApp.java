package com.example.jms.publisher;

import com.example.jms.common.NewsEvent;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.concurrent.TimeUnit;

/**
 * Entry point for the Publisher service.
 *
 * <p>Continuously publishes {@link NewsEvent} messages across different
 * categories and priorities so subscribers can observe filtering behaviour.
 *
 * <h3>How to run (without Docker)</h3>
 * <pre>
 *   export BROKER_URL=tcp://localhost:61616
 *   java -jar publisher/target/publisher.jar
 * </pre>
 */
public class PublisherApp {

    private static final Logger log = LoggerFactory.getLogger(PublisherApp.class);

    // Sample events covering multiple categories and priorities
    private static final NewsEvent[] SAMPLE_EVENTS = {
        NewsEvent.create("SPORTS",    "Champions League Final Tonight",          "Full preview of the match.",                                "BREAKING"),
        NewsEvent.create("TECH",      "New AI Model Breaks Benchmarks",          "A major lab released a frontier model.",                    "HIGH"),
        NewsEvent.create("WEATHER",   "Storm Warning Issued for Coastal Areas",  "Residents advised to stay indoors.",                        "HIGH"),
        NewsEvent.create("SPORTS",    "World Record Broken in 100m Sprint",      "Athlete smashes 20-year record.",                           "BREAKING"),
        NewsEvent.create("POLITICS",  "Parliament Votes on Climate Bill",        "Outcome expected late tonight.",                            "MEDIUM"),
        NewsEvent.create("TECH",      "Open-Source Framework Hits 1M Stars",     "Community celebrates milestone.",                           "LOW"),
        NewsEvent.create("WEATHER",   "Weekend Forecast: Sunny and Warm",        "Great conditions expected across the country.",             "LOW"),
        NewsEvent.create("FINANCE",   "Central Bank Raises Interest Rates",      "Decision surprises markets.",                               "HIGH"),
        NewsEvent.create("SPORTS",    "Transfer Window Opens",                   "Clubs prepare record bids.",                                "MEDIUM"),
        NewsEvent.create("TECH",      "Major Security Vulnerability Disclosed",  "Patch available; update immediately.",                      "BREAKING"),
        NewsEvent.create("POLITICS",  "Election Results: Unexpected Swing",      "Analysts review what went wrong with polls.",               "BREAKING"),
        NewsEvent.create("FINANCE",   "Tech Stocks Rally 5% on Strong Earnings", "Nasdaq closes at all-time high.",                           "HIGH"),
    };

    public static void main(String[] args) throws Exception {
        log.info("=== JMS News Publisher Starting ===");

        // How long to wait between publishes (default: 3 seconds)
        long intervalMs = Long.parseLong(
                System.getenv().getOrDefault("PUBLISH_INTERVAL_MS", "3000"));

        // How many rounds to loop (default: loop forever = -1)
        int maxRounds = Integer.parseInt(
                System.getenv().getOrDefault("MAX_ROUNDS", "-1"));

        // Register shutdown hook for clean close
        Runtime.getRuntime().addShutdownHook(new Thread(() ->
                log.info("Publisher shutting down...")));

        try (NewsPublisher publisher = new NewsPublisher()) {
            int round = 0;
            while (maxRounds == -1 || round < maxRounds) {
                log.info("--- Round {} ---", round + 1);
                for (NewsEvent event : SAMPLE_EVENTS) {
                    publisher.publish(event);
                    TimeUnit.MILLISECONDS.sleep(intervalMs);
                }
                round++;
            }

            // Give the broker a moment to confirm delivery and let Logback flush
            // all buffered log lines before the JVM exits. Without this pause,
            // the last "Published →" line can be dropped because the JVM shuts
            // down before the log appender drains its internal buffer.
            TimeUnit.MILLISECONDS.sleep(500);
        }

        log.info("=== Publisher finished ===");
    }
}
