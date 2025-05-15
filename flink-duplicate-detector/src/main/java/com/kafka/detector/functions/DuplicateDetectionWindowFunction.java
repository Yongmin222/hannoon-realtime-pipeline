package com.kafka.detector.functions;

import com.kafka.detector.model.FilteredReceiptData;
import com.kafka.detector.model.DuplicateAlert;
import org.apache.flink.streaming.api.functions.windowing.ProcessWindowFunction;
import org.apache.flink.streaming.api.windowing.windows.TimeWindow;
import org.apache.flink.util.Collector;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.time.LocalDateTime;
import java.time.ZoneId;
import java.time.ZonedDateTime;
import java.time.format.DateTimeFormatter;
import java.util.*;

/**
 * Window function to detect duplicate payments
 */
public class DuplicateDetectionWindowFunction 
        extends ProcessWindowFunction<FilteredReceiptData, DuplicateAlert, String, TimeWindow> {
    
    private static final Logger logger = LoggerFactory.getLogger(DuplicateDetectionWindowFunction.class);
    private static final DateTimeFormatter TIME_FORMATTER = DateTimeFormatter.ofPattern("yyyy-MM-dd HH:mm:ss");
    
    @Override
    public void process(String key, Context context, 
                       Iterable<FilteredReceiptData> elements, 
                       Collector<DuplicateAlert> out) throws Exception {
        
        List<FilteredReceiptData> payments = new ArrayList<>();
        for (FilteredReceiptData element : elements) {
            payments.add(element);
        }
        
        // 로깅 추가
        logger.info("Processing window for user: {}, payment count: {}", key, payments.size());
        for (FilteredReceiptData payment : payments) {
            logger.debug("Payment at: {} - {}", payment.getStoreBrand(), payment.getStoreName());
        }
        
        // If same user has made 2 or more payments within 10 seconds
        if (payments.size() > 1) {
            logger.info("Duplicate detected for user: {} with {} payments", key, payments.size());
            
            DuplicateAlert alert = new DuplicateAlert();
            Set<String> storeSet = new HashSet<>();
            
            // Set user info from first payment
            FilteredReceiptData firstPayment = payments.get(0);
            alert.setUserId(firstPayment.getUserId());
            alert.setUserName(firstPayment.getUserName());
            alert.setUserGender(firstPayment.getUserGender());
            alert.setUserAge(firstPayment.getUserAge());
            
            // Collect all stores where duplicate payments were made
            for (FilteredReceiptData payment : payments) {
                String storeInfo = payment.getStoreBrand() + " " + payment.getStoreName();
                storeSet.add(storeInfo);
                alert.getDuplicateStores().add(storeInfo);
            }
            
            // Create alert message
            alert.setAlertMessage(String.format(
                "결제자: %s (%s), 의심스러운 결제가 탐지되었습니다: %s",
                alert.getUserName(),
                alert.getUserId(),
                String.join(", ", storeSet)
            ));
            
            // Set detection time
            alert.setDetectionTime(ZonedDateTime.now(ZoneId.of("Asia/Seoul")).format(TIME_FORMATTER));
            
            logger.info("Creating alert: {}", alert.getAlertMessage());
            
            out.collect(alert);
        } else {
            logger.debug("No duplicate for user: {} (only {} payment)", key, payments.size());
        }
    }
}
