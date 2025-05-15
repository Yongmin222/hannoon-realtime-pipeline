package com.kafka.detector.utils;

import com.kafka.detector.model.DuplicateAlert;
import com.fasterxml.jackson.databind.ObjectMapper;
import org.apache.flink.api.common.serialization.SerializationSchema;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * JSON Serialization schema for DuplicateAlert
 */
public class DuplicateAlertJsonSerializationSchema implements SerializationSchema<DuplicateAlert> {
    
    private static final Logger logger = LoggerFactory.getLogger(DuplicateAlertJsonSerializationSchema.class);
    private final ObjectMapper objectMapper = new ObjectMapper();
    
    @Override
    public byte[] serialize(DuplicateAlert element) {
        try {
            byte[] result = objectMapper.writeValueAsBytes(element);
            logger.debug("Serialized DuplicateAlert for user {} to {} bytes", 
                        element.getUserId(), result.length);
            return result;
        } catch (Exception e) {
            logger.error("Failed to serialize DuplicateAlert: {}", element, e);
            throw new RuntimeException("Failed to serialize DuplicateAlert", e);
        }
    }
}
