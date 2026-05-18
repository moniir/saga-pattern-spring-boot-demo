package com.example.mhb.orders.config;

import org.apache.kafka.clients.admin.NewTopic;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;
import org.springframework.kafka.config.TopicBuilder;
import org.springframework.kafka.core.KafkaTemplate;
import org.springframework.kafka.core.ProducerFactory;

@Configuration
public class KafkaConfig {

    @Value("${order.events.topic.name}")
    private String ordersEventsTopicName;
    @Value("${products.commands.topic.name}")
    private String productsCommandsTopicName;
    @Value("${payments.commands.topic.name}")
    private String paymentsCommandsTopicName;
    @Value("${orders.commands.topic.name}")
    private String ordersCommandsTopicName;

    private final static Integer TOTAL_REPLICATION_FACTOR=3;
    private final static Integer TOTAL_TOPIC_PARTITION=3;


    @Bean
    KafkaTemplate<String, Object> kafkaTemplate(ProducerFactory<String, Object> producerFactory) {
        return new KafkaTemplate<>(producerFactory);
    }

    @Bean
    NewTopic createOrdersEventsTopic(){
        return TopicBuilder.name(ordersEventsTopicName)
                .partitions(TOTAL_TOPIC_PARTITION)
                .replicas(TOTAL_REPLICATION_FACTOR)
                .build();
    }

    @Bean
    NewTopic createProductsCommandsTopic() {
        return TopicBuilder.name(productsCommandsTopicName)
                .partitions(TOTAL_TOPIC_PARTITION)
                .replicas(TOTAL_REPLICATION_FACTOR)
                .build();
    }
    @Bean
    NewTopic createPaymentsCommandsTopic() {
        return TopicBuilder.name(paymentsCommandsTopicName)
                .partitions(TOTAL_TOPIC_PARTITION)
                .replicas(TOTAL_REPLICATION_FACTOR)
                .build();
    }
    @Bean
    NewTopic createOrdersCommandsTopic(){
        return TopicBuilder.name(ordersCommandsTopicName)
                .partitions(TOTAL_TOPIC_PARTITION)
                .replicas(TOTAL_REPLICATION_FACTOR)
                .build();
    }
}
