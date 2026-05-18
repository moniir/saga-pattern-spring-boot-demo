package com.example.mhb.orders.service;

import com.example.mhb.core.dto.Order;

import java.util.UUID;

public interface OrderService {
    Order placeOrder(Order order);
    void approveOrder(UUID orderId);
}
