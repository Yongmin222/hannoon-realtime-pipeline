package com.kafka.duplicatedetector.functions;

import com.kafka.duplicatedetector.model.FilteredReceiptData;
import com.kafka.duplicatedetector.model.DuplicateAlert;
import org.apache.flink.streaming.api.functions.windowing.ProcessWindowFunction;
import org.apache.flink.streaming.api.windowing.windows.TimeWindow;
import org.apache.flink.util.Collector;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.time.Instant;
import java.time.ZoneId;
import java.time.ZonedDateTime;
import java.time.format.DateTimeFormatter;
import java.util.*;

/**
 * =======================================================
 * 중복 결제 탐지 윈도우 함수
 * =======================================================
 * 
 * 📋 기능:
 * - 지정된 시간 윈도우 내에서 동일 사용자의 중복 결제 탐지
 * - 2개 이상의 서로 다른 매장에서 결제 시 알림 생성
 * - 윈도우별로 처리되는 배치 함수
 * 
 * 🕐 윈도우 처리:
 * - 시간 기반 윈도우 (예: 10초)
 * - 사용자별로 그룹화된 데이터 처리
 * 
 * 🚨 탐지 조건:
 * - 동일 사용자 (키 기준)
 * - 윈도우 시간 내 2건 이상 결제
 * - 서로 다른 매장에서 발생
 * 
 * 📤 출력:
 * - DuplicateAlert 객체 생성
 * - 중복 매장 리스트 포함
 * - 한국시간 기준 탐지 시간 설정
 */
public class DuplicateDetectionWindowFunction 
        extends ProcessWindowFunction<FilteredReceiptData, DuplicateAlert, String, TimeWindow> {
    
    private static final Logger LOG = LoggerFactory.getLogger(DuplicateDetectionWindowFunction.class);
    
    // 🕐 한국시간 기준 시간 포맷터
    private static final DateTimeFormatter TIME_FORMATTER = DateTimeFormatter.ofPattern("yyyy-MM-dd HH:mm:ss");
    private static final ZoneId KOREA_ZONE = ZoneId.of("Asia/Seoul");
    
    /**
     * =======================================================
     * 윈도우 내 데이터 처리 및 중복 탐지
     * =======================================================
     * 
     * @param key 사용자 키 (이름_성별_나이)
     * @param context 윈도우 컨텍스트 정보
     * @param elements 윈도우 내 모든 결제 데이터
     * @param out 결과 DuplicateAlert 수집기
     */
    @Override
    public void process(String key, Context context, 
                       Iterable<FilteredReceiptData> elements, 
                       Collector<DuplicateAlert> out) throws Exception {
        
        // 📊 윈도우 내 모든 결제 데이터 수집
        List<FilteredReceiptData> payments = new ArrayList<>();
        for (FilteredReceiptData element : elements) {
            payments.add(element);
        }
        
        // 🔍 윈도우 처리 로깅
        LOG.info("Processing window for user key: '{}', payment count: {}, window: [{} - {}]", 
                key, payments.size(), 
                formatTimestamp(context.window().getStart()),
                formatTimestamp(context.window().getEnd()));
        
        // 📝 각 결제 정보 상세 로깅 (디버그 모드)
        if (LOG.isDebugEnabled()) {
            for (int i = 0; i < payments.size(); i++) {
                FilteredReceiptData payment = payments.get(i);
                LOG.debug("Payment {}: {} at {} (time: {})", 
                         i + 1, payment.getUserName(), payment.getStoreIdentifier(), payment.getTime());
            }
        }
        
        // 🚨 중복 결제 탐지 조건 확인
        if (payments.size() > 1) {
            // 🏪 서로 다른 매장 체크
            Set<String> uniqueStores = new HashSet<>();
            for (FilteredReceiptData payment : payments) {
                uniqueStores.add(payment.getStoreIdentifier());
            }
            
            // 서로 다른 매장에서 결제가 발생한 경우만 알림 생성
            if (uniqueStores.size() > 1) {
                LOG.warn("🚨 DUPLICATE DETECTED for user key: '{}' - {} payments across {} different stores", 
                        key, payments.size(), uniqueStores.size());
                
                DuplicateAlert alert = createDuplicateAlert(payments, uniqueStores);
                out.collect(alert);
                
                LOG.info("✅ DuplicateAlert created: {}", alert.getAlertMessage());
            } else {
                LOG.debug("Same user multiple payments at same store - not considered duplicate: {}", 
                         uniqueStores.iterator().next());
            }
        } else {
            LOG.debug("Single payment for user key: '{}' - no duplicate detected", key);
        }
    }
    
    /**
     * =======================================================
     * DuplicateAlert 객체 생성
     * =======================================================
     * 
     * @param payments 윈도우 내 모든 결제 데이터
     * @param uniqueStores 중복 발생 매장 집합
     * @return 생성된 DuplicateAlert 객체
     */
    private DuplicateAlert createDuplicateAlert(List<FilteredReceiptData> payments, Set<String> uniqueStores) {
        // 🏗️ 첫 번째 결제에서 사용자 정보 추출
        FilteredReceiptData firstPayment = payments.get(0);
        
        DuplicateAlert alert = new DuplicateAlert();
        alert.setUserId(firstPayment.getUserId());
        alert.setUserName(firstPayment.getUserName());
        alert.setUserGender(firstPayment.getUserGender());
        alert.setUserAge(firstPayment.getUserAge());
        
        // 🏪 중복 매장 리스트 설정
        List<String> storeList = new ArrayList<>(uniqueStores);
        alert.setDuplicateStores(storeList);
        
        // 📝 알림 메시지 생성
        String alertMessage = String.format(
            "결제자: %s (%s), 의심스러운 결제가 탐지되었습니다: %s",
            alert.getUserName(),
            alert.getUserId(),
            String.join(", ", storeList)
        );
        alert.setAlertMessage(alertMessage);
        
        // 🕐 탐지 시간 설정 (한국시간)
        String detectionTime = ZonedDateTime.now(KOREA_ZONE).format(TIME_FORMATTER);
        alert.setDetectionTime(detectionTime);
        
        return alert;
    }
    
    /**
     * =======================================================
     * 타임스탬프를 읽기 쉬운 형태로 포맷
     * =======================================================
     * 
     * @param timestamp Unix 타임스탬프 (밀리초)
     * @return 포맷된 시간 문자열
     */
    private String formatTimestamp(long timestamp) {
        return Instant.ofEpochMilli(timestamp)
                .atZone(KOREA_ZONE)
                .format(TIME_FORMATTER);
    }
}
