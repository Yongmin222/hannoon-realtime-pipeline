package com.example.test1.scheduler;

import com.example.test1.producer.ReceiptDataProducer;
import com.example.test1.service.ReceiptDataService;
import lombok.RequiredArgsConstructor;
import org.springframework.scheduling.annotation.Scheduled;
import org.springframework.stereotype.Component;
import com.example.test1.entity.ReceiptData;


import java.io.IOException;
import java.util.List;

@Component
@RequiredArgsConstructor
public class DummyDataScheduler {

    private final ReceiptDataService receiptDataService;
    private final ReceiptDataProducer receiptDataProducer;

    @Scheduled(fixedRate = 1000)
    public void produceDataFromExcel() {
        String excelPath = "C:/Users/user/Desktop/kafka/test1/Dummy_data.xlsx"; // 파일 경로
        int generateCount = 10000; // 생성할 랜덤 데이터 개수 (필요에 따라 조절)

        try {
            List<ReceiptData> dataList = receiptDataService.generateGroupedShuffledDataFromExcel(excelPath, generateCount);
            dataList.forEach(receiptDataProducer::sendReceiptData);
            System.out.println("엑셀 기반 Kafka 전송 완료: " + dataList.size() + "건");
        } catch (IOException e) {
            System.err.println("엑셀 읽기 실패: " + e.getMessage());
        }
    }
}
