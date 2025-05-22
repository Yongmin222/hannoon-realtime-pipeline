package com.kafka.franchise.functions;

import com.kafka.franchise.model.ReceiptData;
import com.kafka.franchise.utils.AppProperties;
import org.apache.flink.api.common.functions.FilterFunction;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.text.SimpleDateFormat;
import java.util.Date;
import java.util.TimeZone;

/**
 * 오늘 날짜의 영수증만 필터링하는 Flink Filter 함수
 * 
 * 영수증의 날짜가 현재 날짜와 일치하는지 확인하여,
 * 오늘 날짜의 영수증만 통과시키고 나머지는 필터링합니다.
 * 이를 통해 오늘의 매출만 집계할 수 있습니다.
 */
public class TodayReceiptFilter implements FilterFunction<ReceiptData> {
    private static final Logger LOG = LoggerFactory.getLogger(TodayReceiptFilter.class);
    
    // 날짜 비교를 위한 SimpleDateFormat 객체 (YYYY-MM-DD 형식만 비교)
    private static final SimpleDateFormat DATE_FORMAT = new SimpleDateFormat("yyyy-MM-dd");
    
    /**
     * 생성자에서 설정된 시간대 적용
     * SimpleDateFormat이 올바른 시간대로 날짜를 포맷팅하도록 합니다.
     */
    public TodayReceiptFilter() {
        // AppProperties에서 설정된 시간대 사용
        DATE_FORMAT.setTimeZone(TimeZone.getTimeZone(AppProperties.getTimezone()));
    }

    /**
     * Flink의 FilterFunction 인터페이스를 구현하는 filter 메소드
     * 영수증의 날짜가 오늘 날짜와 일치하는지 확인합니다.
     * 
     * @param receipt 필터링할 영수증 데이터
     * @return 영수증이 오늘 것이면 true, 아니면 false
     * @throws Exception 날짜 처리 중 발생할 수 있는 예외
     */
    @Override
    public boolean filter(ReceiptData receipt) throws Exception {
        // 현재 날짜를 YYYY-MM-DD 형식으로 가져옴
        String today = DATE_FORMAT.format(new Date());
        
        // 영수증의 타임스탬프를 가져옴 (ISO 형식: YYYY-MM-DD HH:MM:SS)
        String receiptTime = receipt.getTime();
        
        // 영수증 날짜가 오늘과 일치하는지 확인 (처음 10자리만 비교 = YYYY-MM-DD)
        boolean isToday = receiptTime.startsWith(today);
        
        // 디버깅을 위해 필터링 결과 로깅
        LOG.info("Receipt time: {}, Today: {}, Filter result: {}", 
                 receiptTime, today, isToday);
        
        // 오늘 날짜의 영수증만 통과
        return isToday;
    }
}
