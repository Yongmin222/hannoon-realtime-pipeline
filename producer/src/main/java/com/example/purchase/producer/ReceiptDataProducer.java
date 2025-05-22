package com.example.purchase.producer;

import com.example.test1.entity.ReceiptData;
import lombok.RequiredArgsConstructor;
import lombok.extern.slf4j.Slf4j;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.kafka.core.KafkaTemplate;
import org.springframework.kafka.support.SendResult;
import org.springframework.stereotype.Service;

import java.util.concurrent.CompletableFuture;

@Service
@RequiredArgsConstructor
@Slf4j
public class ReceiptDataProducer {

    private final KafkaTemplate<String, ReceiptData> kafkaTemplate;
    
    @Value("${kafka.topic-name}")
    private String topicName;

    public void sendReceiptData(ReceiptData data) {
        CompletableFuture<SendResult<String, ReceiptData>> future = kafkaTemplate.send(topicName, data);
        
        // CompletableFuture를 사용한 콜백 처리
        future.whenComplete((result, ex) -> {
            if (ex == null) {
                // 성공 처리
                log.info("메시지 전송 성공: topic={}, partition={}, offset={}",
                        result.getRecordMetadata().topic(),
                        result.getRecordMetadata().partition(),
                        result.getRecordMetadata().offset());
            } else {
                // 실패 처리
                log.error("메시지 전송 실패: {}", ex.getMessage());
            }
        });
    }
}
