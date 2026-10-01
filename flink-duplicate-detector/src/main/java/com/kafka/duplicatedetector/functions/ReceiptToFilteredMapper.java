package com.kafka.duplicatedetector.functions;

import com.kafka.duplicatedetector.model.ReceiptData;
import com.kafka.duplicatedetector.model.FilteredReceiptData;
import org.apache.flink.api.common.functions.MapFunction;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * =======================================================
 * 영수증 데이터 필터링 매퍼
 * =======================================================
 * 
 * 📋 기능:
 * - 전체 ReceiptData에서 중복 탐지에 필요한 정보만 추출
 * - 데이터 크기를 줄여 처리 성능 향상
 * - 중간 처리 단계에서 사용되는 변환 함수
 * 
 * 🔄 변환 과정:
 * ReceiptData (전체 정보) → FilteredReceiptData (핵심 정보만)
 * 
 * 📊 추출 정보:
 * - 사용자: ID, 이름, 성별, 나이
 * - 매장: 브랜드, 매장명
 * - 시간: 결제 시간
 * 
 * 💡 사용법:
 * dataStream.map(new ReceiptToFilteredMapper())
 */
public class ReceiptToFilteredMapper implements MapFunction<ReceiptData, FilteredReceiptData> {
    
    private static final Logger LOG = LoggerFactory.getLogger(ReceiptToFilteredMapper.class);
    
    /**
     * =======================================================
     * ReceiptData를 FilteredReceiptData로 변환
     * =======================================================
     * 
     * @param receipt 원본 영수증 데이터
     * @return 필터링된 영수증 데이터
     * @throws Exception 변환 실패 시
     */
    @Override
    public FilteredReceiptData map(ReceiptData receipt) throws Exception {
        if (receipt == null) {
            LOG.warn("Received null ReceiptData");
            return null;
        }
        
        try {
            // 🔄 핵심 정보만 추출하여 새 객체 생성
            FilteredReceiptData filtered = new FilteredReceiptData(
                String.valueOf(receipt.getUserId()),    // int → String 변환
                receipt.getUserName(),
                receipt.getUserGender(),
                receipt.getUserAge(),
                receipt.getStoreBrand(),
                receipt.getStoreName(),
                receipt.getTime()
            );
            
            if (LOG.isDebugEnabled()) {
                LOG.debug("Mapped receipt for user {} at store {}", 
                         filtered.getUserId(), filtered.getStoreIdentifier());
            }
            
            return filtered;
            
        } catch (Exception e) {
            LOG.error("Failed to map ReceiptData to FilteredReceiptData: {}", receipt, e);
            throw e;
        }
    }
}
