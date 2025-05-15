package com.kafka.detector.functions;

import com.kafka.detector.model.FilteredReceiptData;
import org.apache.flink.api.java.functions.KeySelector;

/**
 * Key selector based on user information
 */
public class UserKeySelector implements KeySelector<FilteredReceiptData, String> {
    
    @Override
    public String getKey(FilteredReceiptData data) throws Exception {
        // Create key by combining user name, gender, and age
        return data.getUserName() + "_" + data.getUserGender() + "_" + data.getUserAge();
    }
}
