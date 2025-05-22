package com.kafka.franchise.utils;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.InputStream;
import java.util.Properties;

/**
 * =======================================================
 * 애플리케이션 설정 관리 유틸리티 클래스
 * =======================================================
 * 
 * 📋 역할:
 * - application.properties 파일에서 설정값을 로드
 * - 기본값 제공으로 안정성 보장
 * - 정적 메서드로 편리한 접근 제공
 * 
 * 🔧 관리 설정:
 * - Kafka 연결 정보 (브로커, 토픽)
 * - Flink 실행 설정 (병렬도, 체크포인트)
 * - 애플리케이션 로직 설정 (타임존, Consumer Group)
 */
public class AppProperties {
    private static final Logger LOG = LoggerFactory.getLogger(AppProperties.class);
    private static final Properties properties = new Properties();
    
    // 정적 초기화 블록에서 설정 파일 로드
    static {
        loadProperties();
    }
    
    /**
     * =======================================================
     * 설정 파일 로드 (application.properties)
     * =======================================================
     * 
     * 📁 로드 순서:
     * 1. 클래스패스에서 application.properties 파일 찾기
     * 2. 파일이 없으면 기본값 사용 (로그 경고)
     * 3. 로드 실패 시에도 기본값으로 안전하게 실행
     */
    private static void loadProperties() {
        try (InputStream inputStream = AppProperties.class
                .getClassLoader()
                .getResourceAsStream("application.properties")) {
            
            if (inputStream != null) {
                properties.load(inputStream);
                LOG.info("Successfully loaded application.properties from classpath");
                logLoadedProperties();
            } else {
                LOG.warn("application.properties not found in classpath, using default values");
            }
        } catch (Exception e) {
            LOG.error("Failed to load application.properties, using default values", e);
        }
    }
    
    /**
     * =======================================================
     * 로드된 주요 설정값 로깅 (디버깅용)
     * =======================================================
     */
    private static void logLoadedProperties() {
        LOG.debug("Loaded properties:");
        LOG.debug("  Bootstrap Servers: {}", getBootstrapServers());
        LOG.debug("  Source Topic: {}", getSourceTopic());
        LOG.debug("  Sink Topic: {}", getSinkTopic());
        LOG.debug("  Consumer Group: {}", getConsumerGroup());
        LOG.debug("  Parallelism: {}", getParallelism());
    }
    
    // =======================================================
    // Kafka 연결 설정
    // =======================================================
    
    /**
     * Kafka 브로커 서버 주소 목록
     * @return 콤마로 구분된 브로커 주소 문자열
     */
    public static String getBootstrapServers() {
        return properties.getProperty("kafka.bootstrap.servers", 
                "localhost:9092");
    }
    
    /**
     * 입력 데이터를 읽어올 Kafka 토픽명
     * @return 소스 토픽명
     */
    public static String getSourceTopic() {
        return properties.getProperty("kafka.source.topic", "test-topic");
    }
    
    /**
     * 처리 결과를 전송할 Kafka 토픽명
     * @return 싱크 토픽명
     */
    public static String getSinkTopic() {
        return properties.getProperty("kafka.sink.topic", "franchise-top-stores");
    }
    
    /**
     * Kafka Consumer Group ID
     * @return Consumer Group 식별자
     */
    public static String getConsumerGroup() {
        return properties.getProperty("kafka.consumer.group", "franchise-top-store-finder");
    }
    
    // =======================================================
    // Flink 실행 설정
    // =======================================================
    
    /**
     * Flink 작업의 병렬 처리 수준
     * @return 병렬도 (기본값: 1)
     */
    public static int getParallelism() {
        return Integer.parseInt(properties.getProperty("flink.parallelism", "1"));
    }
    
    /**
     * 체크포인트 생성 간격 (밀리초)
     * 장애 복구를 위한 상태 저장 주기
     * @return 체크포인트 간격 (기본값: 60초)
     */
    public static long getCheckpointInterval() {
        return Long.parseLong(properties.getProperty("flink.checkpoint.interval", "60000"));
    }
    
    // =======================================================
    // 애플리케이션 로직 설정
    // =======================================================
    
    /**
     * 애플리케이션에서 사용할 시간대
     * @return 시간대 문자열 (기본값: Asia/Seoul)
     */
    public static String getTimezone() {
        return properties.getProperty("application.timezone", "Asia/Seoul");
    }
    
    /**
     * 프랜차이즈별 상위 매장 개수
     * @return 랭킹에 포함할 매장 수 (기본값: 3)
     */
    public static int getTopStoreCount() {
        return Integer.parseInt(properties.getProperty("application.top.store.count", "3"));
    }
    
    // =======================================================
    // 설정값 직접 접근 (고급 사용)
    // =======================================================
    
    /**
     * 원시 Properties 객체 반환
     * 특별한 설정이 필요한 경우 직접 접근 가능
     * @return 로드된 Properties 객체
     */
    public static Properties getRawProperties() {
        return new Properties(properties); // 복사본 반환으로 불변성 보장
    }
    
    /**
     * 특정 키의 설정값 조회 (기본값 포함)
     * @param key 설정 키
     * @param defaultValue 기본값
     * @return 설정값 또는 기본값
     */
    public static String getProperty(String key, String defaultValue) {
        return properties.getProperty(key, defaultValue);
    }
}
