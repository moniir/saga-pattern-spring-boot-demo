package com.example.mhb.orders.service.handler;

import com.example.mhb.core.dto.commands.ApproveOrderCommand;
import com.example.mhb.core.dto.commands.RejectOrderCommand;
import com.example.mhb.orders.service.OrderService;
import org.springframework.kafka.annotation.KafkaHandler;
import org.springframework.kafka.annotation.KafkaListener;
import org.springframework.messaging.handler.annotation.Payload;
import org.springframework.stereotype.Component;

@Component
@KafkaListener(topics = "${orders.commands.topic.name}")
public class OrderCommandHandler {

    private final OrderService orderService;

    public OrderCommandHandler(OrderService orderService) {
        this.orderService = orderService;
    }

    @KafkaHandler
    public void handleCommand(@Payload ApproveOrderCommand approveOrderCommand) {
        orderService.approveOrder(approveOrderCommand.getOrderId());
    }

    @KafkaHandler
    public void handleCommand(@Payload RejectOrderCommand command){
        orderService.rejectOrder(command.getOrderId());
    }
}
