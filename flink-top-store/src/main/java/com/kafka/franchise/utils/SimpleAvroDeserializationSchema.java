package com.kafka.franchise.utils;

import com.kafka.franchise.model.ReceiptData;
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

import java.io.ByteArrayInputStream;
import java.io.IOException;
import java.util.ArrayList;
import java.util.List;

/**
 * =======================================================
 * Avro 형식 데이터 역직렬화 스키마 클래스
 * =======================================================
 * 
 * 📋 역할:
 * - Kafka에서 수신한 Avro 이진 데이터를 Java 객체로 변환
 * - Confluent Schema Registry 와이어 포맷 지원
 * - 스키마 레지스트리 접근 불가 시에도 내장 스키마로 처리
 * 
 * 🔧 지원 기능:
 * - 매직 바이트 및 스키마 ID 처리
 * - GenericRecord → ReceiptData 변환
 * - 에러 처리 및 로깅
 * 
 * @param <T> 변환할 대상 객체 타입 (주로 ReceiptData)
 */
public class SimpleAvroDeserializationSchema<T> implements DeserializationSchema<T> {
    private static final Logger LOG = LoggerFactory.getLogger(SimpleAvroDeserializationSchema.class);
    
    // 변환할 대상 클래스
    private final Class<T> targetType;
    // Avro 스키마 객체 (직렬화 불가능하므로 transient 선언)
    private transient Schema schema;
    // Avro 데이터 리더 (직렬화 불가능하므로 transient 선언)
    private transient DatumReader<GenericRecord> datumReader;
    
    // Confluent Schema Registry 와이어 포맷의 매직 바이트 (0x00)
    private static final byte MAGIC_BYTE = 0x00;
    // 스키마 ID 길이 (4바이트)
    private static final int SCHEMA_ID_LENGTH = 4;
    
    /**
     * 수동으로 정의한 Avro 스키마 (JSON 문자열)
     * 이 스키마는 receipt.avsc 파일과 동일한 구조를 가집니다.
     * 스키마 레지스트리에 접근할 수 없는 경우에도 역직렬화가 가능하도록 
     * 스키마를 직접 포함하고 있습니다.
     */
    private static final String SCHEMA_JSON = "{\n" +
        "  \"type\": \"record\",\n" +
        "  \"name\": \"ReceiptData\",\n" +
        "  \"namespace\": \"com.kafka.sales.avro\",\n" +
        "  \"fields\": [\n" +
        "    { \"name\": \"franchise_id\", \"type\": \"int\" },\n" +
        "    { \"name\": \"store_brand\", \"type\": \"string\" },\n" +
        "    { \"name\": \"store_id\", \"type\": \"int\" },\n" +
        "    { \"name\": \"store_name\", \"type\": \"string\" },\n" +
        "    { \"name\": \"region\", \"type\": \"string\" },\n" +
        "    { \"name\": \"store_address\", \"type\": \"string\" },\n" +
        "    {\n" +
        "      \"name\": \"menu_items\",\n" +
        "      \"type\": {\n" +
        "        \"type\": \"array\",\n" +
        "        \"items\": {\n" +
        "          \"type\": \"record\",\n" +
        "          \"name\": \"MenuItem\",\n" +
        "          \"fields\": [\n" +
        "            { \"name\": \"menu_id\", \"type\": \"int\" },\n" +
        "            { \"name\": \"menu_name\", \"type\": \"string\" },\n" +
        "            { \"name\": \"unit_price\", \"type\": \"int\" },\n" +
        "            { \"name\": \"quantity\", \"type\": \"int\" }\n" +
        "          ]\n" +
        "        }\n" +
        "      }\n" +
        "    },\n" +
        "    { \"name\": \"total_price\", \"type\": \"int\" },\n" +
        "    { \"name\": \"user_id\", \"type\": \"int\" },\n" +
        "    { \"name\": \"time\", \"type\": \"string\" },\n" +
        "    { \"name\": \"user_name\", \"type\": \"string\" },\n" +
        "    { \"name\": \"user_gender\", \"type\": \"string\" },\n" +
        "    { \"name\": \"user_age\", \"type\": \"int\" }\n" +
        "  ]\n" +
        "}";

    /**
     * 생성자
     * 
     * @param targetType 변환할 대상 클래스 (주로 ReceiptData.class)
     */
    public SimpleAvroDeserializationSchema(Class<T> targetType) {
        this.targetType = targetType;
    }

    /**
     * 스키마 초기화 메소드
     * Flink에 의해 작업 시작 전에 호출됩니다.
     * 
     * @param context 초기화 컨텍스트
     * @throws Exception 초기화 중 발생할 수 있는 예외
     */
    @Override
    public void open(InitializationContext context) throws Exception {
        // Avro 스키마 파싱
        this.schema = new Schema.Parser().parse(SCHEMA_JSON);
        // Avro 데이터 리더 생성
        this.datumReader = new GenericDatumReader<>(schema);
    }

    /**
     * 바이트 배열을 Java 객체로 역직렬화
     * 
     * @param bytes 역직렬화할 바이트 배열
     * @return 변환된 Java 객체 (ReceiptData)
     * @throws IOException 역직렬화 중 발생할 수 있는 예외
     */
    @Override
    public T deserialize(byte[] bytes) throws IOException {
        if (bytes == null || bytes.length == 0) {
            return null;
        }

        try {
            ByteArrayInputStream inputStream = new ByteArrayInputStream(bytes);
            
            // Confluent Schema Registry 와이어 포맷 확인
            // 매직 바이트(0x00)로 시작하는지 확인
            if (bytes.length > 5 && bytes[0] == MAGIC_BYTE) {
                // 매직 바이트와 스키마 ID 건너뛰기 (총 5바이트)
                inputStream.skip(5);
            }
            
            // Avro 디코더 생성
            Decoder decoder = DecoderFactory.get().binaryDecoder(inputStream, null);
            // Avro 레코드 읽기
            GenericRecord record = datumReader.read(null, decoder);
            
            // GenericRecord를 ReceiptData 객체로 변환
            return (T) convertToReceiptData(record);
        } catch (Exception e) {
            LOG.error("Failed to deserialize Avro message: {}", e.getMessage(), e);
            throw new IOException("Failed to deserialize Avro message", e);
        }
    }

    /**
     * Avro GenericRecord를 ReceiptData 객체로 변환
     * 
     * @param record Avro GenericRecord
     * @return 변환된 ReceiptData 객체
     */
    private ReceiptData convertToReceiptData(GenericRecord record) {
        ReceiptData receipt = new ReceiptData();
        
        // 기본 필드 설정
        receipt.setFranchise_id((Integer) record.get("franchise_id"));
        receipt.setStore_brand(record.get("store_brand").toString());
        receipt.setStore_id((Integer) record.get("store_id"));
        receipt.setStore_name(record.get("store_name").toString());
        receipt.setRegion(record.get("region").toString());
        receipt.setStore_address(record.get("store_address").toString());
        receipt.setTotal_price((Integer) record.get("total_price"));
        receipt.setUser_id((Integer) record.get("user_id"));
        receipt.setTime(record.get("time").toString());
        receipt.setUser_name(record.get("user_name").toString());
        receipt.setUser_gender(record.get("user_gender").toString());
        receipt.setUser_age((Integer) record.get("user_age"));
        
        // 메뉴 항목 배열 변환
        List<ReceiptData.MenuItem> menuItems = new ArrayList<>();
        List<GenericRecord> items = (List<GenericRecord>) record.get("menu_items");
        for (GenericRecord item : items) {
            ReceiptData.MenuItem menuItem = new ReceiptData.MenuItem();
            menuItem.setMenu_id((Integer) item.get("menu_id"));
            menuItem.setMenu_name(item.get("menu_name").toString());
            menuItem.setUnit_price((Integer) item.get("unit_price"));
            menuItem.setQuantity((Integer) item.get("quantity"));
            menuItems.add(menuItem);
        }
        receipt.setMenu_items(menuItems);
        
        return receipt;
    }

    /**
     * 스트림의 끝을 나타내는지 확인
     * 
     * @param nextElement 다음 요소
     * @return 스트림 종료 여부 (항상 false 반환)
     */
    @Override
    public boolean isEndOfStream(T nextElement) {
        return false;
    }

    /**
     * 생성되는 타입 정보 반환
     * 
     * @return 대상 클래스의 타입 정보
     */
    @Override
    public TypeInformation<T> getProducedType() {
        return TypeInformation.of(targetType);
    }
}
