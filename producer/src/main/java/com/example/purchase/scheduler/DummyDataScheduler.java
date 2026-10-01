package com.example.purchase.scheduler;

import com.example.purchase.producer.ReceiptDataProducer;
import com.example.purchase.service.ReceiptDataService;
import lombok.RequiredArgsConstructor;
import lombok.extern.slf4j.Slf4j;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.scheduling.annotation.Scheduled;
import org.springframework.stereotype.Component;

import java.io.IOException;

@Slf4j
@Component
@RequiredArgsConstructor
public class DummyDataScheduler {

    private final ReceiptDataService receiptDataService;
    private final ReceiptDataProducer receiptDataProducer;
    
    @Value("${app.data.excel-path}")
    private String excelPath;
    
    @Value("${app.data.generate-count}")
    private int generateCount;

    @Scheduled(fixedRateString = "${app.scheduler.fixed-rate}")
    public void produceDataFromExcel() {
        try {
            var dataList = receiptDataService.generateGroupedShuffledDataFromExcel(excelPath, generateCount);
            
            log.info("엑셀 데이터 생성 완료: {}건, Kafka 전송 시작", dataList.size());
            
            // 각 데이터를 Kafka에 전송
            dataList.forEach(receiptDataProducer::sendReceiptData);
            
            // 비동기 전송 시작만 로깅 (실제 성공/실패는 producer에서 확인)
            log.info("Kafka 전송 요청 완료: {}건 (비동기 처리 중)", dataList.size());
        } catch (IOException e) {
            log.error("엑셀 읽기 실패: {}", e.getMessage());
        }
    }
}
