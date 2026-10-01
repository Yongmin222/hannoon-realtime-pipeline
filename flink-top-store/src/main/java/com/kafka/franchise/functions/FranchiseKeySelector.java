package com.kafka.franchise.functions;

import com.kafka.franchise.model.ReceiptData;
import org.apache.flink.api.java.functions.KeySelector;

/**
 * 프랜차이즈 ID로 데이터 스트림을 키 지정하는 키 선택자
 * 
 * Flink의 keyBy 연산에서 사용되어 영수증 데이터를 프랜차이즈 ID를 기준으로
 * 분류하고 그룹화합니다. 이를 통해 동일한 프랜차이즈에 속한 영수증들이
 * 같은 키로 처리되어 프랜차이즈별 집계가 가능해집니다.
 * 
 * 동일한 프랜차이즈 ID를 가진 데이터는 같은 TaskManager에서 처리되며,
 * 같은 상태(state)에 접근하게 됩니다.
 */
public class FranchiseKeySelector implements KeySelector<ReceiptData, Integer> {
    private static final long serialVersionUID = 1L;

    /**
     * 영수증 데이터에서 프랜차이즈 ID를 키로 추출
     * 
     * @param receipt 영수증 데이터
     * @return 프랜차이즈 ID (정수)
     */
    @Override
    public Integer getKey(ReceiptData receipt) {
        return receipt.getFranchise_id();
    }
}
