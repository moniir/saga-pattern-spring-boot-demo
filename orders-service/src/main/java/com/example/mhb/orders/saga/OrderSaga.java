package com.example.mhb.orders.saga;

import com.example.mhb.core.dto.commands.ReserveProductCommand;
import com.example.mhb.core.dto.events.OrderApprovedEvent;
import com.example.mhb.core.dto.events.OrderCreatedEvent;
import com.example.mhb.core.dto.events.PaymentFailedEvent;
import com.example.mhb.core.dto.events.PaymentProcessedEvent;
import com.example.mhb.core.dto.events.ProductReservationFailedEvent;
import com.example.mhb.core.dto.events.ProductReservedEvent;
import com.example.mhb.core.dto.events.ProductReservationCancelledEvent;
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
@KafkaListener(topics = {
        "${order.events.topic.name}",
        "${products.events.topic.name}",
        "${payments.events.topic.name}"}
)
public class OrderSaga {

    private final KafkaTemplate<String,Object> kafkaTemplate;
    private final String productsCommandsTopicName;
    private final OrderHistoryService orderHistoryService;
    private final String paymentsCommandsTopicName;
    private final String ordersCommandsTopicName;

    public OrderSaga(KafkaTemplate<String, Object> kafkaTemplate,
                     @Value("${products.commands.topic.name}") String productsCommandsTopicName,
                     OrderHistoryService orderHistoryService,
                     @Value("${payments.commands.topic.name}") String paymentsCommandsTopicName,
                     @Value("${orders.commands.topic.name}") String ordersCommandsTopicName) {
        this.kafkaTemplate = kafkaTemplate;
        this.productsCommandsTopicName = productsCommandsTopicName;
        this.orderHistoryService = orderHistoryService;
        this.paymentsCommandsTopicName = paymentsCommandsTopicName;
        this.ordersCommandsTopicName = ordersCommandsTopicName;
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

    @KafkaHandler
    public void handleEvent(@Payload PaymentProcessedEvent event) {
        ApproveOrderCommand approveOrderCommand = new ApproveOrderCommand(event.getOrderId());
        kafkaTemplate.send(ordersCommandsTopicName,approveOrderCommand);
    }

    @KafkaHandler
    public void handleEvent(@Payload OrderApprovedEvent event) {
        orderHistoryService.add(event.getOrderId(),OrderStatus.APPROVED);
    }

    @KafkaHandler
    public void handleEvent(@Payload ProductReservationFailedEvent event) {
        orderHistoryService.add(event.getOrderId(), OrderStatus.REJECTED);
    }

    @KafkaHandler
    public void handleEvent(@Payload PaymentFailedEvent event) {
        orderHistoryService.add(event.getOrderId(), OrderStatus.REJECTED);
        CancelProductReservationCommand command = new CancelProductReservationCommand(event.getProductId(),
                event.getOrderId(), event.getProductQuantity());
        kafkaTemplate.send(productsCommandsTopicName, command);

    }

    @KafkaHandler
    public void handleEvent(@Payload ProductReservationCancelledEvent event) {
        RejectOrderCommand command = new RejectOrderCommand(event.getOrderId());
        kafkaTemplate.send(ordersCommandsTopicName, command);
        orderHistoryService.add(event.getOrderId(), OrderStatus.REJECTED);

    }
}
