package com.example.mhb.core.dto.events;

import java.util.UUID;

public class PaymentProcessedEvent {
    private UUID orderId;
    private UUID productId;

    public PaymentProcessedEvent() {
    }

    public PaymentProcessedEvent(UUID orderId, UUID productId) {
        this.orderId = orderId;
        this.productId = productId;
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
}
