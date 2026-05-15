package com.example.mhb.orders.service;

import com.example.mhb.core.types.OrderStatus;
import com.example.mhb.orders.dto.OrderHistory;

import java.util.List;
import java.util.UUID;

public interface OrderHistoryService {
    void add(UUID orderId, OrderStatus orderStatus);

    List<OrderHistory> findByOrderId(UUID orderId);
}
