package com.example.mhb.products.service.handler;

import com.example.mhb.core.dto.Product;
import com.example.mhb.core.dto.commands.CancelProductReservationCommand;
import com.example.mhb.core.dto.events.ProductReservationCancelledEvent;
import com.example.mhb.core.dto.commands.ReserveProductCommand;
import com.example.mhb.core.dto.events.ProductReservedEvent;
import com.example.mhb.core.exceptions.ProductInsufficientQuantityException;
import com.example.mhb.products.service.ProductService;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.kafka.annotation.KafkaHandler;
import org.springframework.kafka.annotation.KafkaListener;
import org.springframework.kafka.core.KafkaTemplate;
import org.springframework.messaging.handler.annotation.Payload;
import org.springframework.stereotype.Component;
import com.example.mhb.core.dto.events.*;


@Component
@KafkaListener(topics = "${products.commands.topic.name}")
public class ProductCommandsHandler {

    private final ProductService productService;
    private final Logger logger = LoggerFactory.getLogger(this.getClass());
    private final KafkaTemplate<String,Object> kafkaTemplate;
    private final String productEventsTopicName;

    public ProductCommandsHandler(ProductService productService, KafkaTemplate<String, Object> kafkaTemplate,
                                  @Value("${products.events.topic.name}") String productEventsTopicName) {
        this.productService = productService;
        this.kafkaTemplate = kafkaTemplate;
        this.productEventsTopicName = productEventsTopicName;
    }

    @KafkaHandler
    public void handleCommand(@Payload ReserveProductCommand command){
        try {
            Product desiredProduct = new Product(command.getProductId(), command.getProductQuantity());
            Product reservedProduct = productService.reserve(desiredProduct,command.getOrderId());
            ProductReservedEvent productReservedEvent = new ProductReservedEvent(command.getOrderId(),
                    command.getProductId(), reservedProduct.getPrice(), command.getProductQuantity());
            kafkaTemplate.send(productEventsTopicName,productReservedEvent);
        } catch (IllegalArgumentException | ProductInsufficientQuantityException e) {
            // Business validation errors - don't retry, publish failure event immediately
            logger.warn("Product reservation failed for order {}: {}", command.getOrderId(), e.getMessage());
            ProductReservationFailedEvent productReservationFailedEvent = new ProductReservationFailedEvent(command.getProductId(),
                    command.getProductQuantity(), command.getOrderId());
            kafkaTemplate.send(productEventsTopicName, productReservationFailedEvent);
        } catch (Exception e) {
            // Technical errors - rethrow to trigger Kafka retry mechanism
            logger.error("Technical error processing product reservation for order {}", command.getOrderId(), e);
            throw e;
        }

    }
    @KafkaHandler
    public void handleCommand(@Payload CancelProductReservationCommand command){
        Product product = new Product(command.getProductId(),command.getProductQuantity());
        productService.cancelReservation(product,command.getOrderId());
        ProductReservationCancelledEvent productReservationCancelledEvent =
                new ProductReservationCancelledEvent(command.getProductId(), command.getOrderId());
        kafkaTemplate.send(productEventsTopicName, productReservationCancelledEvent);
    }
}
