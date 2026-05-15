package com.example.mhb.orders.service;

import com.example.mhb.core.dto.Order;
import com.example.mhb.core.dto.events.OrderCreatedEvent;
import com.example.mhb.core.types.OrderStatus;
import com.example.mhb.orders.dao.jpa.entity.OrderEntity;
import com.example.mhb.orders.dao.jpa.repository.OrderRepository;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.core.env.Environment;
import org.springframework.kafka.core.KafkaTemplate;
import org.springframework.stereotype.Service;

@Service
public class OrderServiceImpl implements OrderService {
    private final OrderRepository orderRepository;
    private final KafkaTemplate<String, Object> kafkaTemplate;
    private final String ordersEventsTopicName;

    public OrderServiceImpl(OrderRepository orderRepository, KafkaTemplate<String, Object> kafkaTemplate,
                            KafkaTemplate<String, Object> kafkaTemplate1,
                            @Value("${order.events.topic.name}") String ordersEventsTopicName) {
        this.orderRepository = orderRepository;
        this.kafkaTemplate = kafkaTemplate1;
        this.ordersEventsTopicName = ordersEventsTopicName;
    }

    @Override
    public Order placeOrder(Order order) {
        OrderEntity entity = new OrderEntity();
        entity.setCustomerId(order.getCustomerId());
        entity.setProductId(order.getProductId());
        entity.setProductQuantity(order.getProductQuantity());
        entity.setStatus(OrderStatus.CREATED);
        orderRepository.save(entity);
        OrderCreatedEvent placedOrder = new OrderCreatedEvent(entity.getId(), entity.getCustomerId(),
                entity.getProductId(), entity.getProductQuantity());
        kafkaTemplate.send(ordersEventsTopicName, placedOrder);

        return new Order(
                entity.getId(),
                entity.getCustomerId(),
                entity.getProductId(),
                entity.getProductQuantity(),
                entity.getStatus());
    }

}
