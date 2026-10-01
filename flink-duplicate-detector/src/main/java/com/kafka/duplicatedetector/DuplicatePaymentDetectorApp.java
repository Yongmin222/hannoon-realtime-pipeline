package com.kafka.duplicatedetector;

import com.kafka.duplicatedetector.functions.UserKeySelector;
import com.kafka.duplicatedetector.functions.ReceiptToFilteredMapper;
import com.kafka.duplicatedetector.functions.DuplicateDetectionWindowFunction;
import com.kafka.duplicatedetector.model.ReceiptData;
import com.kafka.duplicatedetector.model.FilteredReceiptData;
import com.kafka.duplicatedetector.model.DuplicateAlert;
import com.kafka.duplicatedetector.utils.AppProperties;
import com.kafka.duplicatedetector.utils.DuplicateAlertJsonSerializationSchema;
import com.kafka.duplicatedetector.utils.SimpleAvroDeserializationSchema;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.windowing.assigners.TumblingProcessingTimeWindows;
import org.apache.flink.streaming.api.windowing.time.Time;
import org.apache.flink.streaming.connectors.kafka.FlinkKafkaConsumer;
import org.apache.flink.streaming.connectors.kafka.FlinkKafkaProducer;
import org.apache.kafka.clients.consumer.ConsumerConfig;
import org.apache.kafka.clients.producer.ProducerConfig;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.Properties;

/**
 * =======================================================
 * 중복 결제 탐지 메인 애플리케이션
 * =======================================================
 * 
 * 📋 기능 개요:
 * - Kafka에서 Avro 형식의 영수증 데이터를 실시간으로 읽어옴
 * - 동일 사용자가 짧은 시간 내에 여러 매장에서 결제하는 패턴 탐지
 * - 사용자별로 그룹화하여 윈도우 기반 중복 결제 분석
 * - 중복 탐지 시 알림을 JSON 형식으로 Kafka에 전송
 * 
 * 🔄 데이터 플로우:
 * Kafka(Avro) → Map(필터링) → KeyBy(사용자) → Window(시간) → Process(탐지) → Kafka(JSON)
 * 
 * ⚙️ 주요 설정:
 * - 입력: test-topic (Avro 형식)
 * - 출력: payment_same_user (JSON 형식)
 * - 윈도우: 10초 (설정 가능)
 * - 탐지 조건: 동일 사용자, 서로 다른 매장, 윈도우 시간 내
 */
public class DuplicatePaymentDetectorApp {
    private static final Logger LOG = LoggerFactory.getLogger(DuplicatePaymentDetectorApp.class);
    
    /**
     * =======================================================
     * 애플리케이션 진입점
     * =======================================================
     */
    public static void main(String[] args) throws Exception {
        // 📊 애플리케이션 시작 로그
        logApplicationInfo();
        
        // 🎯 Flink 실행 환경 구성
        StreamExecutionEnvironment env = createFlinkEnvironment();
        
        // 📥 Kafka Consumer 설정 (입력 스트림)
        FlinkKafkaConsumer<ReceiptData> consumer = createKafkaConsumer();
        
        // 📤 Kafka Producer 설정 (출력 스트림)
        FlinkKafkaProducer<DuplicateAlert> producer = createKafkaProducer();
        
        // 🔄 데이터 처리 파이프라인 구성
        buildDataPipeline(env, consumer, producer);
        
        // 🚀 잡 실행
        env.execute("Duplicate Payment Detection Application");
    }
    
    /**
     * =======================================================
     * 애플리케이션 정보 로깅
     * =======================================================
     */
    private static void logApplicationInfo() {
        LOG.info("=".repeat(60));
        LOG.info("🚀 Starting Duplicate Payment Detection Application");
        LOG.info("=".repeat(60));
        LOG.info("📋 Application: {} v{}", AppProperties.getAppName(), AppProperties.getAppVersion());
        LOG.info("📥 Source Topic: {} → 📤 Sink Topic: {}", AppProperties.getSourceTopic(), AppProperties.getSinkTopic());
        LOG.info("🔧 Bootstrap Servers: {}", AppProperties.getBootstrapServers());
        LOG.info("👥 Consumer Group: {}", AppProperties.getConsumerGroup());
        LOG.info("⏱️ Window Size: {}s", AppProperties.getWindowSizeSeconds());
        LOG.info("🔍 Checkpoint Interval: {}ms", AppProperties.getCheckpointInterval());
        LOG.info("=".repeat(60));
    }
    
    /**
     * =======================================================
     * Flink 실행 환경 생성 및 구성
     * =======================================================
     * 
     * 🔧 설정 항목:
     * - 체크포인트 간격 (장애 복구용)
     * - 병렬 처리는 기본값 사용 (클러스터 환경에 따라 자동 조정)
     */
    private static StreamExecutionEnvironment createFlinkEnvironment() {
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        
        // ✅ 체크포인트 활성화 (장애 복구용)
        env.enableCheckpointing(AppProperties.getCheckpointInterval());
        
        LOG.info("✅ Flink environment configured with checkpoint interval: {}ms", 
                AppProperties.getCheckpointInterval());
        return env;
    }
    
    /**
     * =======================================================
     * Kafka Consumer 생성 (입력 스트림)
     * =======================================================
     * 
     * 📥 역할:
     * - test-topic에서 Avro 형식의 영수증 데이터 읽기
     * - 자동으로 ReceiptData 객체로 역직렬화
     * - Consumer Group으로 중복 처리 방지
     */
    private static FlinkKafkaConsumer<ReceiptData> createKafkaConsumer() {
        Properties consumerProps = createConsumerProperties();
        
        FlinkKafkaConsumer<ReceiptData> consumer = new FlinkKafkaConsumer<>(
            AppProperties.getSourceTopic(),
            new SimpleAvroDeserializationSchema(),
            consumerProps
        );
        
        LOG.info("📥 Kafka Consumer created for topic: {}", AppProperties.getSourceTopic());
        return consumer;
    }
    
    /**
     * =======================================================
     * Kafka Producer 생성 (출력 스트림)
     * =======================================================
     * 
     * 📤 역할:
     * - DuplicateAlert 객체를 JSON으로 직렬화
     * - payment_same_user 토픽으로 전송
     * - At-Least-Once 보장 (데이터 손실 방지)
     */
    private static FlinkKafkaProducer<DuplicateAlert> createKafkaProducer() {
        Properties producerProps = createProducerProperties();
        
        FlinkKafkaProducer<DuplicateAlert> producer = new FlinkKafkaProducer<>(
            AppProperties.getSinkTopic(),
            new DuplicateAlertJsonSerializationSchema(AppProperties.getSinkTopic()),
            producerProps,
            FlinkKafkaProducer.Semantic.AT_LEAST_ONCE
        );
        
        LOG.info("📤 Kafka Producer created for topic: {}", AppProperties.getSinkTopic());
        return producer;
    }
    
    /**
     * =======================================================
     * 데이터 처리 파이프라인 구성
     * =======================================================
     * 
     * 🔄 처리 순서:
     * 1. Kafka에서 영수증 데이터 읽기
     * 2. 필요한 정보만 추출하여 경량화
     * 3. 사용자별로 그룹화
     * 4. 시간 윈도우 적용
     * 5. 중복 결제 탐지 처리
     * 6. 알림을 Kafka로 전송
     */
    private static void buildDataPipeline(StreamExecutionEnvironment env, 
                                         FlinkKafkaConsumer<ReceiptData> consumer,
                                         FlinkKafkaProducer<DuplicateAlert> producer) {
        
        // 📥 1. 소스에서 데이터 읽기
        DataStream<ReceiptData> receiptStream = env.addSource(consumer)
            .name("Receipt Data Source");
        
        // 🔍 2. 데이터 유효성 검증 및 로깅
        DataStream<ReceiptData> validatedStream = receiptStream
            .filter(receipt -> {
                if (receipt == null) {
                    LOG.warn("⚠️ Received null receipt");
                    return false;
                }
                if (LOG.isDebugEnabled()) {
                    LOG.debug("📨 Received receipt from user: {} at store: {}", 
                             receipt.getUserId(), receipt.getStoreName());
                }
                return true;
            })
            .name("Receipt Validation");
        
        // 🎯 3. 필요한 정보만 추출 (데이터 경량화)
        DataStream<FilteredReceiptData> filteredStream = validatedStream
            .map(new ReceiptToFilteredMapper())
            .name("Receipt Filtering");
        
        // 🔑 4. 사용자별 그룹화 및 윈도우 적용
        DataStream<DuplicateAlert> alertStream = filteredStream
            .keyBy(new UserKeySelector())
            .window(TumblingProcessingTimeWindows.of(Time.seconds(AppProperties.getWindowSizeSeconds())))
            .process(new DuplicateDetectionWindowFunction())
            .name("Duplicate Detection");
        
        // 📤 5. 결과를 Kafka로 전송
        alertStream.addSink(producer)
            .name("Duplicate Alert Sink");
        
        // 🖥️ 6. 콘솔 출력 (디버깅용)
        alertStream.print("🚨 DUPLICATE ALERT");
        
        LOG.info("✅ Data pipeline configured successfully");
        LOG.info("🔄 Pipeline: Source → Filter → KeyBy(User) → Window({}s) → Process → Sink", 
                AppProperties.getWindowSizeSeconds());
    }
    
    /**
     * =======================================================
     * Kafka Consumer 설정 생성
     * =======================================================
     * 
     * 🔧 주요 설정:
     * - bootstrap.servers: Kafka 브로커 주소
     * - group.id: Consumer Group (중복 처리 방지)
     * - auto.offset.reset: 처음 실행 시 latest부터 읽기
     */
    private static Properties createConsumerProperties() {
        Properties props = new Properties();
        props.setProperty(ConsumerConfig.BOOTSTRAP_SERVERS_CONFIG, AppProperties.getBootstrapServers());
        props.setProperty(ConsumerConfig.GROUP_ID_CONFIG, AppProperties.getConsumerGroup());
        props.setProperty(ConsumerConfig.AUTO_OFFSET_RESET_CONFIG, "latest");
        
        LOG.debug("📋 Consumer properties configured");
        return props;
    }
    
    /**
     * =======================================================
     * Kafka Producer 설정 생성
     * =======================================================
     * 
     * 🔧 주요 설정:
     * - acks=all: 모든 복제본에 쓰기 완료 후 응답 (안정성)
     * - retries=3: 실패 시 3번 재시도
     * - 배치 처리 및 압축 최적화 설정
     */
    private static Properties createProducerProperties() {
        Properties props = new Properties();
        props.setProperty(ProducerConfig.BOOTSTRAP_SERVERS_CONFIG, AppProperties.getBootstrapServers());
        props.setProperty(ProducerConfig.ACKS_CONFIG, "all");
        props.setProperty(ProducerConfig.RETRIES_CONFIG, "3");
        props.setProperty(ProducerConfig.COMPRESSION_TYPE_CONFIG, "snappy");
        props.setProperty(ProducerConfig.BATCH_SIZE_CONFIG, "16384");
        props.setProperty(ProducerConfig.LINGER_MS_CONFIG, "5");
        
        LOG.debug("📋 Producer properties configured");
        return props;
    }
}
