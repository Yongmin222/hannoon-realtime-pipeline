package com.kafka.duplicatedetector.functions;

import com.kafka.duplicatedetector.model.FilteredReceiptData;
import org.apache.flink.api.java.functions.KeySelector;

/**
 * =======================================================
 * 사용자 기반 키 선택자
 * =======================================================
 * 
 * 📋 기능:
 * - FilteredReceiptData에서 사용자 기반 키를 생성
 * - 동일 사용자의 결제 데이터를 그룹화하기 위한 키 추출
 * - Flink의 keyBy() 연산에서 사용
 * 
 * 🔑 키 구성:
 * - 사용자명 + 성별 + 나이의 조합
 * - 형태: "김철수_남성_25"
 * 
 * 💡 사용법:
 * dataStream.keyBy(new UserKeySelector())
 */
public class UserKeySelector implements KeySelector<FilteredReceiptData, String> {
    
    /**
     * =======================================================
     * 사용자 정보를 기반으로 키 생성
     * =======================================================
     * 
     * @param data 필터링된 영수증 데이터
     * @return 사용자 식별 키 (이름_성별_나이)
     * @throws Exception 키 생성 실패 시
     */
    @Override
    public String getKey(FilteredReceiptData data) throws Exception {
        if (data == null) {
            throw new IllegalArgumentException("FilteredReceiptData cannot be null");
        }
        
        // 🔑 사용자 정보 조합으로 유니크 키 생성
        String userName = data.getUserName() != null ? data.getUserName() : "unknown";
        String userGender = data.getUserGender() != null ? data.getUserGender() : "unknown";
        int userAge = data.getUserAge();
        
        return userName + "_" + userGender + "_" + userAge;
    }
}
