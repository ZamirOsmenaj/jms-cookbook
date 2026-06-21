package com.example.jms.common;

import com.fasterxml.jackson.annotation.JsonCreator;
import com.fasterxml.jackson.annotation.JsonProperty;

import java.io.Serializable;
import java.time.Instant;
import java.util.UUID;

/**
 * OrderRequest represents an e-commerce order sent to a processing queue.
 *
 * <p>This is a great Queue use case because:
 * <ul>
 *   <li>Each order must be processed <b>exactly once</b> (not broadcast to all consumers)</li>
 *   <li>Multiple consumer instances can share the load (competing consumers pattern)</li>
 *   <li>Orders must survive a consumer crash and be redelivered (persistence)</li>
 *   <li>VIP orders should jump the queue (message priority)</li>
 * </ul>
 *
 * <h3>Queue vs Topic – when to use which</h3>
 * <pre>
 *   QUEUE  → "someone must do this work"   (orders, emails, tasks, jobs)
 *   TOPIC  → "everyone should know this"   (events, notifications, feeds)
 * </pre>
 */
public class OrderRequest implements Serializable {

    private static final long serialVersionUID = 1L;

    /** Unique order identifier */
    private final String orderId;

    /** Customer identifier */
    private final String customerId;

    /**
     * Customer tier — used as a JMS property for priority routing.
     * VIP orders get JMS priority 9; STANDARD get priority 4.
     */
    private final String customerTier;  // VIP, STANDARD

    /** Type of order — used as a Message Selector by specialised consumers */
    private final String orderType;     // ELECTRONICS, CLOTHING, FOOD, FURNITURE

    /** Total order value in USD */
    private final double amount;

    /** Human-readable description */
    private final String description;

    /** ISO-8601 creation timestamp */
    private final String createdAt;

    /** Processing status (set by consumer after handling) */
    private String status;  // PENDING, PROCESSING, COMPLETED, FAILED

    @JsonCreator
    public OrderRequest(
            @JsonProperty("orderId")      String orderId,
            @JsonProperty("customerId")   String customerId,
            @JsonProperty("customerTier") String customerTier,
            @JsonProperty("orderType")    String orderType,
            @JsonProperty("amount")       double amount,
            @JsonProperty("description")  String description,
            @JsonProperty("createdAt")    String createdAt,
            @JsonProperty("status")       String status) {
        this.orderId      = orderId;
        this.customerId   = customerId;
        this.customerTier = customerTier;
        this.orderType    = orderType;
        this.amount       = amount;
        this.description  = description;
        this.createdAt    = createdAt;
        this.status       = status;
    }

    public static OrderRequest create(String customerId, String customerTier,
                                      String orderType, double amount,
                                      String description) {
        return new OrderRequest(
                "ORD-" + UUID.randomUUID().toString().substring(0, 8).toUpperCase(),
                customerId,
                customerTier,
                orderType,
                amount,
                description,
                Instant.now().toString(),
                "PENDING"
        );
    }

    public String getOrderId()      { return orderId; }
    public String getCustomerId()   { return customerId; }
    public String getCustomerTier() { return customerTier; }
    public String getOrderType()    { return orderType; }
    public double getAmount()       { return amount; }
    public String getDescription()  { return description; }
    public String getCreatedAt()    { return createdAt; }
    public String getStatus()       { return status; }
    public void setStatus(String s) { this.status = s; }

    @Override
    public String toString() {
        return String.format("%s | customer=%s [%s] | type=%-11s | $%7.2f | %s",
                orderId, customerId, customerTier, orderType, amount, description);
    }
}
