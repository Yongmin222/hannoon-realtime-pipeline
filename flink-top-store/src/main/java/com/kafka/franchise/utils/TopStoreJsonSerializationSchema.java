package com.kafka.franchise.utils;

import com.fasterxml.jackson.databind.ObjectMapper;
import com.fasterxml.jackson.databind.node.ArrayNode;
import com.fasterxml.jackson.databind.node.ObjectNode;
import com.kafka.franchise.model.TopStoreRankingData;
import org.apache.flink.connector.kafka.sink.KafkaRecordSerializationSchema;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * =======================================================
 * 상위 매장 랭킹 데이터 JSON 직렬화 스키마 클래스
 * =======================================================
 * 
 * 📋 역할:
 * - TopStoreRankingData 객체를 JSON 형식으로 변환
 * - Kafka 메시지로 전송하기 위한 ProducerRecord 생성
 * - 프랜차이즈 ID를 메시지 키로 사용하여 파티셔닝
 * 
 * 🔧 JSON 구조:
 * {
 *   "franchise_id": 123,
 *   "timestamp": "2025-05-22 14:30:00",
 *   "top_stores": [
 *     {
 *       "rank": 1,
 *       "store_id": 456,
 *       "store_name": "강남점",
 *       "store_brand": "맥도날드",
 *       "store_address": "서울시 강남구...",
 *       "total_sales": 1500000
 *     }
 *   ]
 * }
 */
public class TopStoreJsonSerializationSchema implements KafkaRecordSerializationSchema<TopStoreRankingData> {
    private static final Logger LOG = LoggerFactory.getLogger(TopStoreJsonSerializationSchema.class);
    private static final long serialVersionUID = 1L;

    // 출력 카프카 토픽 이름
    private final String topic;
    // JSON 변환을 위한 Jackson ObjectMapper (직렬화 불가능하므로 transient 선언)
    private transient ObjectMapper mapper;

    /**
     * 생성자
     * 
     * @param topic 출력 카프카 토픽 이름
     */
    public TopStoreJsonSerializationSchema(String topic) {
        this.topic = topic;
    }

    /**
     * TopStoreRankingData 객체를 카프카 레코드로 직렬화
     * 
     * @param element 직렬화할 TopStoreRankingData 객체
     * @param context 카프카 싱크 컨텍스트
     * @param timestamp 메시지 타임스탬프
     * @return 카프카 프로듀서 레코드
     */
    @Override
    public ProducerRecord<byte[], byte[]> serialize(TopStoreRankingData element, KafkaSinkContext context, Long timestamp) {
        // ObjectMapper가 null이면 초기화
        if (mapper == null) {
            mapper = new ObjectMapper();
        }

        try {
            // 키 생성 (프랜차이즈 ID를 키로 사용)
            String key = String.valueOf(element.getFranchise_id());
            byte[] keyBytes = key.getBytes();
            
            // ObjectNode(JSON 객체) 생성
            ObjectNode jsonNode = mapper.createObjectNode();
            jsonNode.put("franchise_id", element.getFranchise_id());
            jsonNode.put("timestamp", element.getTimestamp());
            
            // top_stores 배열 생성
            ArrayNode topStoresArray = jsonNode.putArray("top_stores");
            
            // 각 매장 정보를 배열에 추가
            for (TopStoreRankingData.StoreRankingInfo store : element.getTop_stores()) {
                ObjectNode storeNode = topStoresArray.addObject();
                storeNode.put("rank", store.getRank());
                storeNode.put("store_id", store.getStore_id());
                storeNode.put("store_name", store.getStore_name());
                storeNode.put("store_brand", store.getStore_brand());
                storeNode.put("store_address", store.getStore_address());
                storeNode.put("total_sales", store.getTotal_sales());
            }
            
            // JSON 데이터 직렬화 (바이트 배열로 변환)
            byte[] valueBytes = mapper.writeValueAsBytes(jsonNode);
            
            // 로그 출력 (첫 번째 매장 정보만)
            if (!element.getTop_stores().isEmpty()) {
                TopStoreRankingData.StoreRankingInfo topStore = element.getTop_stores().get(0);
                LOG.info("Serializing Top Stores for franchise_id={}, #1 store: {}, total_sales={}",
                        element.getFranchise_id(), topStore.getStore_name(), topStore.getTotal_sales());
            }
            
            // 카프카 프로듀서 레코드 생성
            // null 파티션을 사용하면 카프카가 키 기반으로 파티션을 결정
            return new ProducerRecord<>(topic, null, timestamp, keyBytes, valueBytes, null);
        } catch (Exception e) {
            LOG.error("Failed to serialize TopStoreRankingData to JSON: {}", e.getMessage(), e);
            return null;
        }
    }
}
