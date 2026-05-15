package com.example.mhb.core.dto.commands;

import java.math.BigDecimal;
import java.util.UUID;

public class ProcessPaymentCommand {
    private UUID productId;
    private UUID orderId;
    private BigDecimal productPrice;
    private Integer productQuantity;

    public ProcessPaymentCommand() {
    }

    public ProcessPaymentCommand(UUID productId, UUID orderId, BigDecimal productPrice, Integer productQuantity) {
        this.productId = productId;
        this.orderId = orderId;
        this.productPrice = productPrice;
        this.productQuantity = productQuantity;
    }

    public UUID getProductId() {
        return productId;
    }

    public void setProductId(UUID productId) {
        this.productId = productId;
    }

    public UUID getOrderId() {
        return orderId;
    }

    public void setOrderId(UUID orderId) {
        this.orderId = orderId;
    }

    public BigDecimal getProductPrice() {
        return productPrice;
    }

    public void setProductPrice(BigDecimal productPrice) {
        this.productPrice = productPrice;
    }

    public Integer getProductQuantity() {
        return productQuantity;
    }

    public void setProductQuantity(Integer productQuantity) {
        this.productQuantity = productQuantity;
    }
}
