package com.example.jms.common;

import com.fasterxml.jackson.annotation.JsonCreator;
import com.fasterxml.jackson.annotation.JsonProperty;

import java.io.Serializable;
import java.time.Instant;
import java.util.UUID;

/**
 * NewsEvent is the domain model published to the JMS Topic.
 *
 * <p>JMS supports several message types:
 * <ul>
 *   <li>{@code TextMessage}   – plain string (we use JSON here)</li>
 *   <li>{@code ObjectMessage} – serialized Java object</li>
 *   <li>{@code MapMessage}    – key/value pairs</li>
 *   <li>{@code BytesMessage}  – raw bytes</li>
 *   <li>{@code StreamMessage} – primitive types in a stream</li>
 * </ul>
 *
 * <p>We serialize this class to JSON and send it as a {@code TextMessage} –
 * the most interoperable approach in practice.
 */
public class NewsEvent implements Serializable {

    private static final long serialVersionUID = 1L;

    /** Unique event identifier */
    private final String id;

    /**
     * Category used as a JMS Message Selector property.
     * Subscribers can filter: {@code category = 'SPORTS'} etc.
     */
    private final String category;

    /** Human-readable headline */
    private final String headline;

    /** Full story body */
    private final String body;

    /** ISO-8601 timestamp of when the event was created */
    private final String timestamp;

    /** Severity: LOW, MEDIUM, HIGH, BREAKING */
    private final String priority;

    @JsonCreator
    public NewsEvent(
            @JsonProperty("id")        String id,
            @JsonProperty("category")  String category,
            @JsonProperty("headline")  String headline,
            @JsonProperty("body")      String body,
            @JsonProperty("timestamp") String timestamp,
            @JsonProperty("priority")  String priority) {
        this.id        = id;
        this.category  = category;
        this.headline  = headline;
        this.body      = body;
        this.timestamp = timestamp;
        this.priority  = priority;
    }

    /** Factory method for convenience */
    public static NewsEvent create(String category, String headline,
                                   String body, String priority) {
        return new NewsEvent(
                UUID.randomUUID().toString(),
                category,
                headline,
                body,
                Instant.now().toString(),
                priority
        );
    }

    public String getId()        { return id; }
    public String getCategory()  { return category; }
    public String getHeadline()  { return headline; }
    public String getBody()      { return body; }
    public String getTimestamp() { return timestamp; }
    public String getPriority()  { return priority; }

    @Override
    public String toString() {
        return String.format("[%s] (%s/%s) %s", id.substring(0,8), category, priority, headline);
    }
}
