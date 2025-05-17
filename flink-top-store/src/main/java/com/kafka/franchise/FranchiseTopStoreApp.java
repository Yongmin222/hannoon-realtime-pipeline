package com.kafka.franchise;

import com.kafka.franchise.functions.FranchiseKeySelector;
import com.kafka.franchise.functions.TodayReceiptFilter;
import com.kafka.franchise.functions.DailyTopStoreProcessor;
import com.kafka.franchise.model.ReceiptData;
import com.kafka.franchise.model.TopStoreRankingData;
import com.kafka.franchise.utils.SimpleAvroDeserializationSchema;
import com.kafka.franchise.utils.TopStoreJsonSerializationSchema;
import org.apache.flink.api.common.eventtime.WatermarkStrategy;
import org.apache.flink.connector.kafka.sink.KafkaSink;
import org.apache.flink.connector.kafka.source.KafkaSource;
import org.apache.flink.connector.kafka.source.enumerator.initializer.OffsetsInitializer;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.datastream.KeyedStream;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.InputStream;
import java.util.Properties;
import java.util.TimeZone;

/**
 * Franchise Top Store 애플리케이션의 메인 클래스
 * 
 * 이 애플리케이션은 카프카에서 영수증 데이터를 읽어 프랜차이즈별 매출 상위 매장을 실시간으로 집계하고,
 * 그 결과를 다른 카프카 토픽으로 전송합니다.
 * 
 * 처리 흐름:
 * 1. 카프카 소스에서 Avro 형식의 영수증 데이터를 읽습니다.
 * 2. 오늘 날짜에 해당하는 영수증만 필터링합니다.
 * 3. 프랜차이즈 ID로 데이터를 그룹화합니다.
 * 4. 각 프랜차이즈 내에서 매장별 매출을 집계하고 상위 3개 매장을 추출합니다.
 * 5. 결과를 JSON 형식으로 다른 카프카 토픽에 전송합니다.
 */
public class FranchiseTopStoreApp {
    // 로깅을 위한 Logger 인스턴스
    private static final Logger LOG = LoggerFactory.getLogger(FranchiseTopStoreApp.class);
    
    /**
     * 애플리케이션의 메인 메소드
     * 
     * @param args 명령줄 인수 (사용하지 않음)
     * @throws Exception 실행 중 발생할 수 있는 모든 예외
     */
    public static void main(String[] args) throws Exception {
        // JVM 기본 시간대를 한국 시간(KST)으로 설정
        // 날짜/시간 처리에 중요한 설정으로, 모든 타임스탬프가 한국 시간 기준으로 처리됨
        TimeZone.setDefault(TimeZone.getTimeZone("Asia/Seoul"));
        
        // 설정 파일(application.properties)에서 설정 로드
        Properties appProps = loadApplicationProperties();
        
        // 설정 파일 또는 기본값에서 주요 설정 추출
        String bootstrapServers = appProps.getProperty("kafka.bootstrap.servers", "localhost:9092");
        String sourceTopic = appProps.getProperty("kafka.source.topic", "test-topic");
        String sinkTopic = appProps.getProperty("kafka.sink.topic", "franchise-top-stores");
        String consumerGroup = appProps.getProperty("kafka.consumer.group", "franchise-top-store-finder");
        long checkpointInterval = Long.parseLong(appProps.getProperty("flink.checkpoint.interval", "60000"));
        
        // 주요 설정 로깅
        LOG.info("Starting Franchise Top Store Application");
        LOG.info("Bootstrap Servers: {}", bootstrapServers);
        LOG.info("Source Topic: {}", sourceTopic);
        LOG.info("Sink Topic: {}", sinkTopic);
        
        // Apache Flink 실행 환경 설정
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        env.setParallelism(1);  // 로컬 테스트를 위해 병렬 처리를 1로 설정 (실제 프로덕션에서는 적절히 조정 필요)
        env.enableCheckpointing(checkpointInterval);  // 장애 복구를 위한 체크포인트 활성화 (기본 60초)
        
        // 카프카 소스 연결 구성
        // Avro 형식의 영수증 데이터를 카프카에서 읽어오는 소스 설정
        KafkaSource<ReceiptData> source = KafkaSource.<ReceiptData>builder()
                .setBootstrapServers(bootstrapServers)  // 카프카 브로커 주소
                .setTopics(sourceTopic)                 // 소스 토픽
                .setGroupId(consumerGroup)              // 컨슈머 그룹 ID
                .setStartingOffsets(OffsetsInitializer.latest())  // 최신 오프셋부터 소비 시작
                .setValueOnlyDeserializer(new SimpleAvroDeserializationSchema<>(ReceiptData.class))  // Avro 역직렬화 스키마
                .build();
        
        // 카프카로부터 영수증 데이터 스트림 생성
        DataStream<ReceiptData> receiptStream = env.fromSource(
                source,
                WatermarkStrategy.noWatermarks(),  // 이벤트 시간 처리를 사용하지 않음
                "Receipt Source"                   // 소스 이름 (모니터링용)
        ).map(receipt -> {
            // 디버깅을 위한 로깅 추가
            LOG.debug("Received receipt: franchise_id={}, store_id={}, time={}", 
                    receipt.getFranchise_id(), receipt.getStore_id(), receipt.getTime());
            return receipt;
        }).name("Receipt Logging");  // 이 단계에 이름 부여 (모니터링용)
        
        // 오늘 날짜의 영수증만 필터링
        // TodayReceiptFilter 클래스는 영수증 날짜가 오늘인지 확인
        DataStream<ReceiptData> todayReceiptStream = receiptStream
                .filter(new TodayReceiptFilter())
                .name("Today Receipt Filter");
        
        // 프랜차이즈 ID로 키 설정
        // 같은 프랜차이즈 ID를 가진 데이터는 같은 작업자가 처리하도록 함
        KeyedStream<ReceiptData, Integer> keyedByFranchise = todayReceiptStream
                .keyBy(new FranchiseKeySelector());
        
        // 프랜차이즈별 최고 매출 매장 집계 (상태 관리)
        // DailyTopStoreProcessor는 각 프랜차이즈별로 매장 매출을 집계하고 상위 3개 매장을 추출
        DataStream<TopStoreRankingData> topStoreStream = keyedByFranchise
                .process(new DailyTopStoreProcessor())
                .name("Daily Top Store Processor");
        
        // 카프카 싱크 구성 (JSON 형식으로 출력)
        // 집계 결과를 다른 카프카 토픽으로 전송
        KafkaSink<TopStoreRankingData> sink = KafkaSink.<TopStoreRankingData>builder()
                .setBootstrapServers(bootstrapServers)
                .setRecordSerializer(new TopStoreJsonSerializationSchema(sinkTopic))  // JSON 직렬화 스키마
                .build();
        
        // 결과 스트림을 카프카로 전송
        topStoreStream.sinkTo(sink).name("Top Store Sink");
        
        // Flink 작업 실행
        // 이 호출은 blocking이며, 작업이 종료되거나 오류가 발생할 때까지 기다림
        env.execute("Franchise Top Store Finder");
    }
    
    /**
     * 애플리케이션 설정을 클래스패스의 application.properties 파일에서 로드
     * 파일이 없거나 로드 중 오류가 발생하면 빈 Properties 객체 반환
     * 
     * @return 로드된 설정이 포함된 Properties 객체
     */
    private static Properties loadApplicationProperties() {
        Properties props = new Properties();
        
        try (InputStream inputStream = FranchiseTopStoreApp.class
                .getClassLoader()
                .getResourceAsStream("application.properties")) {
            
            if (inputStream != null) {
                props.load(inputStream);
                LOG.info("Loaded application properties from classpath");
            } else {
                LOG.warn("application.properties not found in classpath, using defaults");
            }
        } catch (Exception e) {
            LOG.warn("Failed to load application.properties, using defaults", e);
        }
        
        return props;
    }
}
