package com.example.mhb.orders.saga;

import com.example.mhb.core.dto.commands.ReserveProductCommand;
import com.example.mhb.core.dto.events.OrderCreatedEvent;
import com.example.mhb.core.dto.events.ProductReservedEvent;
import com.example.mhb.core.types.OrderStatus;
import com.example.mhb.orders.service.OrderHistoryService;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.kafka.annotation.KafkaHandler;
import org.springframework.kafka.annotation.KafkaListener;
import org.springframework.kafka.core.KafkaTemplate;
import org.springframework.messaging.handler.annotation.Payload;
import org.springframework.stereotype.Component;
import com.example.mhb.core.dto.commands.*;

@Component
@KafkaListener(topics = {"${order.events.topic.name}","${product.events.topic.name}","${payments.events.topic.name}"})
public class OrderSaga {

    private final KafkaTemplate<String,Object> kafkaTemplate;
    private final String productsCommandsTopicName;
    private final OrderHistoryService orderHistoryService;
    private final String paymentsCommandsTopicName;
    private final String paymentsEventssTopicName;

    public OrderSaga(KafkaTemplate<String, Object> kafkaTemplate,
                     @Value("${products.commands.topic.name}") String productsCommandsTopicName,
                     OrderHistoryService orderHistoryService,
                     @Value("${payments.commands.topic.name}") String paymentsCommandsTopicName,
                     @Value("${payments.events.topic.name}") String paymentsEventssTopicName) {
        this.kafkaTemplate = kafkaTemplate;
        this.productsCommandsTopicName = productsCommandsTopicName;
        this.orderHistoryService = orderHistoryService;
        this.paymentsCommandsTopicName = paymentsCommandsTopicName;
        this.paymentsEventssTopicName = paymentsEventssTopicName;
    }

    @KafkaHandler
    public void handleEvent(@Payload OrderCreatedEvent event) {
        ReserveProductCommand command = new ReserveProductCommand(event.getProductId(),
                event.getProductQuantity(),
                event.getOrderId()
        );
        kafkaTemplate.send(productsCommandsTopicName, command);
        orderHistoryService.add(event.getOrderId(), OrderStatus.CREATED);
    }

    @KafkaHandler
    public void handleEvent(@Payload ProductReservedEvent event) {
        ProcessPaymentCommand command = new ProcessPaymentCommand(event.getProductId(),event.getOrderId(),event.getProductPrice(),event.getProductQuantity());
        kafkaTemplate.send(paymentsCommandsTopicName,command);
    }
}
