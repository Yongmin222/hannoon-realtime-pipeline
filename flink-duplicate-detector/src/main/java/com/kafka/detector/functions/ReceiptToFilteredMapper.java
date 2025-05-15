package com.kafka.detector.functions;

import com.kafka.detector.model.ReceiptData;
import com.kafka.detector.model.FilteredReceiptData;
import org.apache.flink.api.common.functions.MapFunction;

/**
 * Function to map receipt data to filtered data
 */
public class ReceiptToFilteredMapper implements MapFunction<ReceiptData, FilteredReceiptData> {
    
    @Override
    public FilteredReceiptData map(ReceiptData receipt) throws Exception {
        return new FilteredReceiptData(
            String.valueOf(receipt.getUserId()),
            receipt.getUserName(),
            receipt.getUserGender(),
            receipt.getUserAge(),
            receipt.getStoreBrand(),
            receipt.getStoreName(),
            receipt.getTime()
        );
    }
}
