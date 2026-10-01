package com.kafka.duplicatedetector.utils;

import java.io.InputStream;
import java.util.Properties;

/**
 * =======================================================
 * 애플리케이션 설정 관리 클래스
 * =======================================================
 * 
 * 📋 기능:
 * - application.properties 파일에서 설정값 로드
 * - 정적 메서드로 어디서든 설정값 접근 가능
 * - 기본값 제공으로 안정성 확보
 * 
 * 🔧 관리하는 설정:
 * - Kafka 브로커 주소, 토픽명, 컨슈머 그룹
 * - Flink 체크포인트 간격, 윈도우 크기
 * - 애플리케이션 정보
 * 
 * 💡 사용법:
 * String servers = AppProperties.getBootstrapServers();
 */
public class AppProperties {
    private static Properties properties;
    
    // 🚀 클래스 로딩 시 자동으로 설정 파일 로드
    static {
        loadProperties();
    }
    
    /**
     * =======================================================
     * application.properties 파일 로드
     * =======================================================
     * 
     * 📁 파일 위치: src/main/resources/application.properties
     * 🚨 실패 시: RuntimeException 발생 (애플리케이션 시작 중단)
     */
    private static void loadProperties() {
        try (InputStream input = AppProperties.class.getClassLoader()
                .getResourceAsStream("application.properties")) {
            if (input == null) {
                throw new RuntimeException("application.properties not found in classpath");
            }
            properties = new Properties();
            properties.load(input);
        } catch (Exception e) {
            throw new RuntimeException("Failed to load application.properties", e);
        }
    }
    
    // =======================================================
    // Kafka 관련 설정
    // =======================================================
    
    /**
     * Kafka 브로커 서버 주소 목록
     * @return comma로 구분된 브로커 주소 (예: server1:9092,server2:9092)
     */
    public static String getBootstrapServers() {
        return properties.getProperty("kafka.bootstrap.servers");
    }
    
    /**
     * 입력 토픽명 (영수증 데이터 읽기)
     * @return 토픽명 (예: test-topic)
     */
    public static String getSourceTopic() {
        return properties.getProperty("kafka.source.topic");
    }
    
    /**
     * 출력 토픽명 (중복 결제 알림 데이터 쓰기)
     * @return 토픽명 (예: payment_same_user)
     */
    public static String getSinkTopic() {
        return properties.getProperty("kafka.sink.topic");
    }
    
    /**
     * 컨슈머 그룹 ID (중복 처리 방지)
     * @return 그룹 ID (예: duplicate-payment-detector)
     */
    public static String getConsumerGroup() {
        return properties.getProperty("kafka.consumer.group");
    }
    
    // =======================================================
    // Flink 관련 설정
    // =======================================================
    
    /**
     * 윈도우 크기 (초 단위)
     * @return 윈도우 크기 (기본값: 10초)
     */
    public static int getWindowSizeSeconds() {
        return Integer.parseInt(properties.getProperty("flink.window.size.seconds", "10"));
    }
    
    /**
     * 체크포인트 간격 (장애 복구용)
     * @return 간격(밀리초) (기본값: 60000ms = 1분)
     */
    public static long getCheckpointInterval() {
        return Long.parseLong(properties.getProperty("flink.checkpoint.interval", "60000"));
    }
    
    // =======================================================
    // 애플리케이션 관련 설정
    // =======================================================
    
    /**
     * 애플리케이션 이름
     * @return 앱 이름 (기본값: duplicate-payment-detector)
     */
    public static String getAppName() {
        return properties.getProperty("app.name", "duplicate-payment-detector");
    }
    
    /**
     * 애플리케이션 버전
     * @return 버전 (기본값: 1.0.0)
     */
    public static String getAppVersion() {
        return properties.getProperty("app.version", "1.0.0");
    }
}
