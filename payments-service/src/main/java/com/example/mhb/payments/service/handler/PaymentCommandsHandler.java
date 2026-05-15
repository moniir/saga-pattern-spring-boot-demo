package com.example.mhb.payments.service.handler;

import com.example.mhb.core.dto.Payment;
import com.example.mhb.core.dto.commands.ProcessPaymentCommand;
import com.example.mhb.payments.service.PaymentService;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.kafka.annotation.KafkaHandler;
import org.springframework.kafka.annotation.KafkaListener;
import org.springframework.kafka.core.KafkaTemplate;
import org.springframework.messaging.handler.annotation.Payload;
import org.springframework.stereotype.Component;
import com.example.mhb.core.exceptions.CreditCardProcessorUnavailableException;
import com.example.mhb.core.dto.events.PaymentProcessedEvent;
import com.example.mhb.core.dto.events.PaymentFailedEvent;

@Component
@KafkaListener(topics = "${payments.commands.topic.name}")
public class PaymentCommandsHandler {

    private final PaymentService paymentService;
    private final Logger logger = LoggerFactory.getLogger(this.getClass());
    private final KafkaTemplate<String,Object> kafkaTemplate;
    private final String paymentEventsTopicName;

    public PaymentCommandsHandler(PaymentService paymentService, KafkaTemplate<String, Object> kafkaTemplate,
                                  @Value("${payments.events.topic.name}") String paymentEventsTopicName) {
        this.paymentService = paymentService;
        this.kafkaTemplate = kafkaTemplate;
        this.paymentEventsTopicName = paymentEventsTopicName;
    }

    @KafkaHandler
    public void handleCommand(@Payload ProcessPaymentCommand command){
        try {
            Payment payment = new Payment(command.getOrderId(),command.getProductId(),command.getProductPrice(),command.getProductQuantity());
            Payment processedPayment = paymentService.process(payment);
            PaymentProcessedEvent processedEvent = new PaymentProcessedEvent(processedPayment.getOrderId(),processedPayment.getId());
            kafkaTemplate.send(paymentEventsTopicName,processedEvent);
        } catch (CreditCardProcessorUnavailableException e) {
            logger.error(e.getLocalizedMessage(),e);
            PaymentFailedEvent paymentFailedEvent = new PaymentFailedEvent(command.getOrderId(),command.getProductId(),command.getProductQuantity());
            kafkaTemplate.send(paymentEventsTopicName,paymentFailedEvent);
        }

    }
}
