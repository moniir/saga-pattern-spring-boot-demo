package com.example.mhb.payments.config;

import org.apache.kafka.clients.admin.NewTopic;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;
import org.springframework.kafka.config.TopicBuilder;
import org.springframework.kafka.core.KafkaTemplate;
import org.springframework.kafka.core.ProducerFactory;

@Configuration
public class KafkaConfig {

    @Value("${payments.events.topic.name}")
    private String paymentsEventsTopicName;
    private final static Integer TOTAL_REPLICATION_FACTOR=3;
    private final static Integer TOTAL_TOPIC_PARTITION=3;

    @Bean
    KafkaTemplate<String, Object> kafkaTemplate(ProducerFactory<String, Object> producerFactory) {
        return new KafkaTemplate<>(producerFactory);
    }
    @Bean
    NewTopic createPaymentsEventsTopic() {
        return TopicBuilder.name(paymentsEventsTopicName)
                .partitions(TOTAL_TOPIC_PARTITION)
                .replicas(TOTAL_REPLICATION_FACTOR)
                .build();
    }
}
