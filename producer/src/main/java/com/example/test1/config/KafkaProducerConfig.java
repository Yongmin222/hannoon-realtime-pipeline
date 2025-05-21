package com.example.test1.config;

import com.example.test1.entity.ReceiptData;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;
import org.springframework.kafka.core.DefaultKafkaProducerFactory;
import org.springframework.kafka.core.KafkaTemplate;
import org.springframework.kafka.core.ProducerFactory;
import org.springframework.boot.autoconfigure.kafka.KafkaProperties;

@Configuration
public class KafkaProducerConfig {

    @Value("${kafka.schema-registry-url}")
    private String schemaRegistryUrl;
    
    private final KafkaProperties kafkaProperties;
    
    public KafkaProducerConfig(KafkaProperties kafkaProperties) {
        this.kafkaProperties = kafkaProperties;
    }

    @Bean
    public ProducerFactory<String, ReceiptData> producerFactory() {
        var props = kafkaProperties.buildProducerProperties();
        props.put("schema.registry.url", schemaRegistryUrl);
        return new DefaultKafkaProducerFactory<>(props);
    }

    @Bean
    public KafkaTemplate<String, ReceiptData> kafkaTemplate() {
        return new KafkaTemplate<>(producerFactory());
    }
}
