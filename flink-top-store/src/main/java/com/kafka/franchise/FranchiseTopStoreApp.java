package com.kafka.franchise;

import com.kafka.franchise.functions.FranchiseKeySelector;
import com.kafka.franchise.functions.TodayReceiptFilter;
import com.kafka.franchise.functions.DailyTopStoreProcessor;
import com.kafka.franchise.model.ReceiptData;
import com.kafka.franchise.model.TopStoreRankingData;
import com.kafka.franchise.utils.AppProperties;
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

import java.util.TimeZone;

/**
 * =======================================================
 * 프랜차이즈 상위 매장 실시간 분석 메인 애플리케이션
 * =======================================================
 * 
 * 📋 기능 개요:
 * - Kafka에서 Avro 형식의 영수증 데이터를 실시간으로 읽어옴
 * - 오늘 날짜의 데이터만 필터링
 * - 프랜차이즈별로 그룹화하여 매장별 매출 집계
 * - 각 프랜차이즈별 상위 3개 매장을 실시간으로 추출
 * - 결과를 JSON 형식으로 Kafka에 전송
 * 
 * 🔄 데이터 플로우:
 * Kafka(Avro) → Filter(오늘) → KeyBy(프랜차이즈) → Process(상위매장추출) → Kafka(JSON)
 * 
 * ⚙️ 주요 설정:
 * - 입력: test-topic (Avro 형식)
 * - 출력: franchise-top-stores (JSON 형식)
 * - 타임존: Asia/Seoul
 * - 상위 매장 수: 3개
 */
public class FranchiseTopStoreApp {
    private static final Logger LOG = LoggerFactory.getLogger(FranchiseTopStoreApp.class);
    
    /**
     * =======================================================
     * 애플리케이션 진입점
     * =======================================================
     */
    public static void main(String[] args) throws Exception {
        // 🔧 시스템 타임존 설정 (한국 시간)
        TimeZone.setDefault(TimeZone.getTimeZone(AppProperties.getTimezone()));
        LOG.info("Setting timezone to: {}", AppProperties.getTimezone());
        
        // 📊 애플리케이션 시작 로그
        logApplicationInfo();
        
        // 🎯 Flink 실행 환경 구성
        StreamExecutionEnvironment env = createFlinkEnvironment();
        
        // 📥 Kafka Source 설정 (입력 스트림)
        KafkaSource<ReceiptData> source = createKafkaSource();
        
        // 📤 Kafka Sink 설정 (출력 스트림)
        KafkaSink<TopStoreRankingData> sink = createKafkaSink();
        
        // 🔄 데이터 처리 파이프라인 구성
        buildDataPipeline(env, source, sink);
        
        // 🚀 잡 실행
        env.execute("Franchise Top Store Finder");
    }
    
    /**
     * =======================================================
     * 애플리케이션 정보 로깅
     * =======================================================
     */
    private static void logApplicationInfo() {
        LOG.info("Starting Franchise Top Store Application");
        LOG.info("Source Topic: {} → Sink Topic: {}", AppProperties.getSourceTopic(), AppProperties.getSinkTopic());
        LOG.info("Bootstrap Servers: {}", AppProperties.getBootstrapServers());
        LOG.info("Consumer Group: {}", AppProperties.getConsumerGroup());
        LOG.info("Parallelism: {}", AppProperties.getParallelism());
        LOG.info("Top Store Count: {}", AppProperties.getTopStoreCount());
    }
    
    /**
     * =======================================================
     * Flink 실행 환경 생성 및 구성
     * =======================================================
     * 
     * 🔧 설정 항목:
     * - 병렬 처리 수준 (parallelism)
     * - 체크포인트 간격 (장애 복구용)
     */
    private static StreamExecutionEnvironment createFlinkEnvironment() {
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        env.setParallelism(AppProperties.getParallelism());
        env.enableCheckpointing(AppProperties.getCheckpointInterval());
        
        LOG.info("Flink environment configured with parallelism: {} and checkpoint interval: {}ms", 
                AppProperties.getParallelism(), AppProperties.getCheckpointInterval());
        return env;
    }
    
    /**
     * =======================================================
     * Kafka Source 생성 (입력 스트림)
     * =======================================================
     * 
     * 📥 역할:
     * - test-topic에서 Avro 형식의 영수증 데이터 읽기
     * - 자동으로 ReceiptData 객체로 역직렬화
     * - Consumer Group으로 중복 처리 방지
     * - 최신 오프셋부터 소비 시작
     */
    private static KafkaSource<ReceiptData> createKafkaSource() {
        return KafkaSource.<ReceiptData>builder()
                .setBootstrapServers(AppProperties.getBootstrapServers())
                .setTopics(AppProperties.getSourceTopic())
                .setGroupId(AppProperties.getConsumerGroup())
                .setStartingOffsets(OffsetsInitializer.latest())
                .setValueOnlyDeserializer(new SimpleAvroDeserializationSchema<>(ReceiptData.class))
                .build();
    }
    
    /**
     * =======================================================
     * Kafka Sink 생성 (출력 스트림)
     * =======================================================
     * 
     * 📤 역할:
     * - TopStoreRankingData 객체를 JSON으로 직렬화
     * - franchise-top-stores 토픽으로 전송
     * - 프랜차이즈별 상위 매장 정보 실시간 전달
     */
    private static KafkaSink<TopStoreRankingData> createKafkaSink() {
        return KafkaSink.<TopStoreRankingData>builder()
                .setBootstrapServers(AppProperties.getBootstrapServers())
                .setRecordSerializer(new TopStoreJsonSerializationSchema(AppProperties.getSinkTopic()))
                .build();
    }
    
    /**
     * =======================================================
     * 데이터 처리 파이프라인 구성
     * =======================================================
     * 
     * 🔄 처리 순서:
     * 1. Kafka에서 영수증 데이터 읽기
     * 2. 디버깅을 위한 데이터 로깅
     * 3. 오늘 날짜 데이터만 필터링
     * 4. 프랜차이즈 ID별로 그룹화
     * 5. 매장별 매출 집계 및 상위 매장 추출 (상태 기반)
     * 6. 결과를 Kafka로 전송
     */
    private static void buildDataPipeline(StreamExecutionEnvironment env, 
                                         KafkaSource<ReceiptData> source,
                                         KafkaSink<TopStoreRankingData> sink) {
        
        // 📥 Kafka에서 영수증 데이터 스트림 생성
        DataStream<ReceiptData> receiptStream = env.fromSource(
                source,
                WatermarkStrategy.noWatermarks(),
                "Receipt Source"
        );
        
        // 📊 디버깅을 위한 데이터 로깅
        DataStream<ReceiptData> loggedStream = receiptStream
                .map(receipt -> {
                    if (LOG.isDebugEnabled()) {
                        LOG.debug("Received receipt: franchise_id={}, store_id={}, time={}", 
                                receipt.getFranchise_id(), receipt.getStore_id(), receipt.getTime());
                    }
                    return receipt;
                })
                .name("Receipt Logging");
        
        // 🗓️ 오늘 날짜 데이터만 필터링
        DataStream<ReceiptData> todayReceiptStream = loggedStream
                .filter(new TodayReceiptFilter())
                .name("Today Receipt Filter");
        
        // 🔑 프랜차이즈 ID로 그룹화 (병렬 처리를 위한 키 분할)
        KeyedStream<ReceiptData, Integer> keyedByFranchise = todayReceiptStream
                .keyBy(new FranchiseKeySelector());
        
        // 🏪 프랜차이즈별 상위 매장 추출 (상태 기반 처리)
        DataStream<TopStoreRankingData> topStoreStream = keyedByFranchise
                .process(new DailyTopStoreProcessor())
                .name("Daily Top Store Processor");
        
        // 📤 결과를 Kafka로 전송
        topStoreStream
                .sinkTo(sink)
                .name("Top Store Sink");
    }
}
