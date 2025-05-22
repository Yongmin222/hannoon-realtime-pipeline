package com.kafka.duplicatedetector.utils;

import com.kafka.duplicatedetector.model.DuplicateAlert;
import com.fasterxml.jackson.databind.ObjectMapper;
import org.apache.flink.streaming.connectors.kafka.KafkaSerializationSchema;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import javax.annotation.Nullable;

/**
 * =======================================================
 * DuplicateAlert JSON 직렬화 스키마
 * =======================================================
 * 
 * 📋 기능:
 * - DuplicateAlert 객체를 JSON byte[]로 변환
 * - Kafka Producer에서 사용하는 직렬화 스키마
 * - 에러 처리 및 로깅 포함
 * 
 * 🔧 변환 과정:
 * DuplicateAlert Object → JSON String → ProducerRecord
 * 
 * 💡 사용법:
 * FlinkKafkaProducer에서 직렬화 스키마로 사용
 */
public class DuplicateAlertJsonSerializationSchema implements KafkaSerializationSchema<DuplicateAlert> {
    
    private static final Logger LOG = LoggerFactory.getLogger(DuplicateAlertJsonSerializationSchema.class);
    private final ObjectMapper objectMapper = new ObjectMapper();
    private final String topicName;
    
    /**
     * =======================================================
     * 생성자
     * =======================================================
     */
    public DuplicateAlertJsonSerializationSchema(String topicName) {
        this.topicName = topicName;
    }
    
    /**
     * =======================================================
     * DuplicateAlert 객체를 Kafka ProducerRecord로 직렬화
     * =======================================================
     * 
     * @param element 직렬화할 DuplicateAlert 객체
     * @param context Kafka 컨텍스트 (null 가능)
     * @param timestamp 타임스탬프 (null 가능)
     * @return Kafka ProducerRecord
     */
    @Override
    public ProducerRecord<byte[], byte[]> serialize(DuplicateAlert element, @Nullable Long timestamp) {
        try {
            byte[] value = objectMapper.writeValueAsBytes(element);
            
            if (LOG.isDebugEnabled()) {
                LOG.debug("Successfully serialized DuplicateAlert for user {} to {} bytes", 
                         element.getUserId(), value.length);
            }
            
            return new ProducerRecord<>(topicName, value);
            
        } catch (Exception e) {
            LOG.error("Failed to serialize DuplicateAlert for user {}: {}", 
                     element != null ? element.getUserId() : "null", element, e);
            throw new RuntimeException("Failed to serialize DuplicateAlert", e);
        }
    }
}
