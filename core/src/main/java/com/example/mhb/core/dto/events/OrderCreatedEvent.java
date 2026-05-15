package com.example.mhb.core.dto.events;

import java.util.UUID;

public class OrderCreatedEvent {
    private UUID orderId;
    private UUID productId;
    private UUID customerId;
    private Integer productQuantity;

    public OrderCreatedEvent() {
    }

    public OrderCreatedEvent(UUID orderId, UUID productId, UUID customerId, Integer productQuantity) {
        this.orderId = orderId;
        this.productId = productId;
        this.customerId = customerId;
        this.productQuantity = productQuantity;
    }

    public UUID getOrderId() {
        return orderId;
    }

    public void setOrderId(UUID orderId) {
        this.orderId = orderId;
    }

    public UUID getProductId() {
        return productId;
    }

    public void setProductId(UUID productId) {
        this.productId = productId;
    }

    public UUID getCustomerId() {
        return customerId;
    }

    public void setCustomerId(UUID customerId) {
        this.customerId = customerId;
    }

    public Integer getProductQuantity() {
        return productQuantity;
    }

    public void setProductQuantity(Integer productQuantity) {
        this.productQuantity = productQuantity;
    }
}
