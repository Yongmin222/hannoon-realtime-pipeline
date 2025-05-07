package com.example.test1.producer;

import lombok.RequiredArgsConstructor;
import org.springframework.kafka.core.KafkaTemplate;
import org.springframework.stereotype.Service;
import com.example.test1.entity.ReceiptData;



@Service
@RequiredArgsConstructor
public class ReceiptDataProducer {

    private final KafkaTemplate<String, ReceiptData> kafkaTemplate;

    private static final String TOPIC_NAME = "test"; // 원하는 토픽명 고정

    public void sendReceiptData(ReceiptData data) {
        kafkaTemplate.send(TOPIC_NAME, data);
    }
}
