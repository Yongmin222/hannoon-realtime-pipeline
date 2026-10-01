package com.kafka.duplicatedetector.utils;

import com.kafka.duplicatedetector.model.ReceiptData;
import org.apache.avro.Schema;
import org.apache.avro.generic.GenericDatumReader;
import org.apache.avro.generic.GenericRecord;
import org.apache.avro.io.DatumReader;
import org.apache.avro.io.Decoder;
import org.apache.avro.io.DecoderFactory;
import org.apache.flink.api.common.serialization.DeserializationSchema;
import org.apache.flink.api.common.typeinfo.TypeInformation;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.nio.ByteBuffer;
import java.util.ArrayList;
import java.util.List;

/**
 * =======================================================
 * 단순 Avro 역직렬화 스키마 (Schema Registry 지원)
 * =======================================================
 * 
 * 📋 기능:
 * - Confluent Schema Registry 형식의 Avro 메시지를 ReceiptData로 변환
 * - Magic byte와 Schema ID 처리
 * - 영수증 데이터의 모든 필드 매핑
 * 
 * 🔧 메시지 형식:
 * [Magic Byte(1)] + [Schema ID(4)] + [Avro Data(N)]
 * 
 * 💡 변환 과정:
 * Kafka Message → Skip Header(5 bytes) → Avro Record → ReceiptData
 */
public class SimpleAvroDeserializationSchema implements DeserializationSchema<ReceiptData> {
    
    private static final Logger LOG = LoggerFactory.getLogger(SimpleAvroDeserializationSchema.class);
    
    // Confluent Schema Registry 메시지 형식 상수
    private static final byte MAGIC_BYTE = 0x0;
    private static final int SCHEMA_ID_SIZE = 4;
    private static final int HEADER_SIZE = 1 + SCHEMA_ID_SIZE; // Magic byte + Schema ID
    
    /**
     * Avro 스키마 정의 (ReceiptData 구조)
     */
    private static final String SCHEMA_STRING = """
        {
          "type": "record",
          "name": "ReceiptData",
          "namespace": "com.example.test1.entity",
          "fields": [
            { "name": "franchise_id", "type": "int" },
            { "name": "store_brand", "type": "string" },
            { "name": "store_id", "type": "int" },
            { "name": "store_name", "type": "string" },
            { "name": "region", "type": "string" },
            { "name": "store_address", "type": "string" },
            {
              "name": "menu_items",
              "type": {
                "type": "array",
                "items": {
                  "type": "record",
                  "name": "MenuItem",
                  "fields": [
                    { "name": "menu_id", "type": "int" },
                    { "name": "menu_name", "type": "string" },
                    { "name": "unit_price", "type": "int" },
                    { "name": "quantity", "type": "int" }
                  ]
                }
              }
            },
            { "name": "total_price", "type": "int" },
            { "name": "user_id", "type": "int" },
            { "name": "time", "type": "string" },
            { "name": "user_name", "type": "string" },
            { "name": "user_gender", "type": "string" },
            { "name": "user_age", "type": "int" }
          ]
        }
        """;
    
    /**
     * =======================================================
     * Kafka 메시지를 ReceiptData 객체로 역직렬화
     * =======================================================
     * 
     * @param message Kafka에서 받은 바이트 배열 메시지
     * @return 변환된 ReceiptData 객체 (실패 시 null)
     */
    @Override
    public ReceiptData deserialize(byte[] message) {
        if (message == null || message.length == 0) {
            LOG.warn("Received null or empty message");
            return null;
        }
        
        try {
            // 📝 Confluent 메시지 형식 검증
            if (message.length < HEADER_SIZE || message[0] != MAGIC_BYTE) {
                LOG.error("Invalid Confluent Avro message format. Expected magic byte: {}, got: {}", 
                         MAGIC_BYTE, message.length > 0 ? message[0] : "empty");
                return null;
            }
            
            // 📊 Schema ID 추출 (디버깅용)
            ByteBuffer buffer = ByteBuffer.wrap(message, 1, SCHEMA_ID_SIZE);
            int schemaId = buffer.getInt();
            if (LOG.isDebugEnabled()) {
                LOG.debug("Processing message with schema ID: {}, total size: {} bytes", 
                         schemaId, message.length);
            }
            
            // 🔄 Avro 데이터 디코딩 (헤더 5바이트 이후)
            Schema schema = new Schema.Parser().parse(SCHEMA_STRING);
            DatumReader<GenericRecord> reader = new GenericDatumReader<>(schema);
            Decoder decoder = DecoderFactory.get().binaryDecoder(
                message, HEADER_SIZE, message.length - HEADER_SIZE, null);
            GenericRecord record = reader.read(null, decoder);
            
            // 🏗️ ReceiptData 객체로 변환
            return convertToReceiptData(record);
            
        } catch (Exception e) {
            LOG.error("Failed to deserialize Avro message: {}", e.getMessage(), e);
            return null;
        }
    }
    
    /**
     * =======================================================
     * GenericRecord를 ReceiptData 객체로 변환
     * =======================================================
     * 
     * @param record Avro GenericRecord
     * @return 변환된 ReceiptData 객체
     */
    private ReceiptData convertToReceiptData(GenericRecord record) {
        ReceiptData receiptData = new ReceiptData();
        
        // 🏪 매장 정보 매핑
        receiptData.setFranchiseId((Integer) record.get("franchise_id"));
        receiptData.setStoreBrand(record.get("store_brand").toString());
        receiptData.setStoreId((Integer) record.get("store_id"));
        receiptData.setStoreName(record.get("store_name").toString());
        receiptData.setRegion(record.get("region").toString());
        receiptData.setStoreAddress(record.get("store_address").toString());
        
        // 🍔 메뉴 아이템 리스트 처리
        @SuppressWarnings("unchecked")
        List<GenericRecord> menuItemRecords = (List<GenericRecord>) record.get("menu_items");
        List<ReceiptData.MenuItem> menuItems = new ArrayList<>();
        
        if (menuItemRecords != null) {
            for (GenericRecord menuItemRecord : menuItemRecords) {
                ReceiptData.MenuItem menuItem = new ReceiptData.MenuItem();
                menuItem.setMenuId((Integer) menuItemRecord.get("menu_id"));
                menuItem.setMenuName(menuItemRecord.get("menu_name").toString());
                menuItem.setUnitPrice((Integer) menuItemRecord.get("unit_price"));
                menuItem.setQuantity((Integer) menuItemRecord.get("quantity"));
                menuItems.add(menuItem);
            }
        }
        
        receiptData.setMenuItems(menuItems);
        
        // 💰 결제 및 사용자 정보 매핑
        receiptData.setTotalPrice((Integer) record.get("total_price"));
        receiptData.setUserId((Integer) record.get("user_id"));
        receiptData.setTime(record.get("time").toString());
        receiptData.setUserName(record.get("user_name").toString());
        receiptData.setUserGender(record.get("user_gender").toString());
        receiptData.setUserAge((Integer) record.get("user_age"));
        
        if (LOG.isDebugEnabled()) {
            LOG.debug("Successfully converted receipt for user {} (franchise: {})", 
                     receiptData.getUserId(), receiptData.getFranchiseId());
        }
        
        return receiptData;
    }
    
    /**
     * =======================================================
     * 스트림 종료 여부 확인
     * =======================================================
     * 
     * @param nextElement 다음 요소
     * @return 항상 false (무한 스트림)
     */
    @Override
    public boolean isEndOfStream(ReceiptData nextElement) {
        return false;
    }
    
    /**
     * =======================================================
     * 생성되는 타입 정보 반환
     * =======================================================
     * 
     * @return ReceiptData 타입 정보
     */
    @Override
    public TypeInformation<ReceiptData> getProducedType() {
        return TypeInformation.of(ReceiptData.class);
    }
}
