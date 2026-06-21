package com.example.jms.common;

import com.fasterxml.jackson.annotation.JsonCreator;
import com.fasterxml.jackson.annotation.JsonProperty;

import java.io.Serializable;
import java.time.Instant;

/**
 * OrderReply is sent back by a consumer to the producer's reply queue.
 *
 * <h3>JMS Request / Reply Pattern</h3>
 * <pre>
 *   Producer                             Consumer
 *   ────────                             ────────
 *   1. Create a temporary reply queue    
 *   2. Set JMSReplyTo = reply queue      
 *   3. Set JMSCorrelationID = requestId  
 *   4. Send to orders.queue  ──────────► 5. Receive order
 *                                        6. Process it
 *                            ◄────────── 7. Send reply to JMSReplyTo queue
 *   8. Receive from reply queue
 *   9. Match via JMSCorrelationID
 * </pre>
 *
 * <p>The reply queue can be a permanent named queue or a temporary queue
 * (created with {@code session.createTemporaryQueue()}) that lives only
 * for the duration of the connection.
 */
public class OrderReply implements Serializable {

    private static final long serialVersionUID = 1L;

    private final String orderId;
    private final String status;       // COMPLETED, FAILED
    private final String message;      // human-readable outcome
    private final String processedBy;  // which consumer handled it
    private final String processedAt;

    @JsonCreator
    public OrderReply(
            @JsonProperty("orderId")     String orderId,
            @JsonProperty("status")      String status,
            @JsonProperty("message")     String message,
            @JsonProperty("processedBy") String processedBy,
            @JsonProperty("processedAt") String processedAt) {
        this.orderId     = orderId;
        this.status      = status;
        this.message     = message;
        this.processedBy = processedBy;
        this.processedAt = processedAt;
    }

    public static OrderReply success(String orderId, String processedBy, String detail) {
        return new OrderReply(orderId, "COMPLETED", detail, processedBy, Instant.now().toString());
    }

    public static OrderReply failure(String orderId, String processedBy, String reason) {
        return new OrderReply(orderId, "FAILED", reason, processedBy, Instant.now().toString());
    }

    public String getOrderId()     { return orderId; }
    public String getStatus()      { return status; }
    public String getMessage()     { return message; }
    public String getProcessedBy() { return processedBy; }
    public String getProcessedAt() { return processedAt; }

    @Override
    public String toString() {
        return String.format("%s → %s by %s: %s", orderId, status, processedBy, message);
    }
}
