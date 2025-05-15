package com.kafka.detector.model;

import java.util.ArrayList;
import java.util.List;

/**
 * Duplicate payment alert data
 */
public class DuplicateAlert {
    private String userId;
    private String userName;
    private String userGender;
    private int userAge;
    private List<String> duplicateStores;
    private String alertMessage;
    private String detectionTime;
    
    public DuplicateAlert() {
        this.duplicateStores = new ArrayList<>();
    }
    
    // Getters and Setters
    public String getUserId() {
        return userId;
    }
    
    public void setUserId(String userId) {
        this.userId = userId;
    }
    
    public String getUserName() {
        return userName;
    }
    
    public void setUserName(String userName) {
        this.userName = userName;
    }
    
    public String getUserGender() {
        return userGender;
    }
    
    public void setUserGender(String userGender) {
        this.userGender = userGender;
    }
    
    public int getUserAge() {
        return userAge;
    }
    
    public void setUserAge(int userAge) {
        this.userAge = userAge;
    }
    
    public List<String> getDuplicateStores() {
        return duplicateStores;
    }
    
    public void setDuplicateStores(List<String> duplicateStores) {
        this.duplicateStores = duplicateStores;
    }
    
    public String getAlertMessage() {
        return alertMessage;
    }
    
    public void setAlertMessage(String alertMessage) {
        this.alertMessage = alertMessage;
    }
    
    public String getDetectionTime() {
        return detectionTime;
    }
    
    public void setDetectionTime(String detectionTime) {
        this.detectionTime = detectionTime;
    }
    
    @Override
    public String toString() {
        return "DuplicateAlert{" +
                "userId='" + userId + '\'' +
                ", userName='" + userName + '\'' +
                ", userGender='" + userGender + '\'' +
                ", userAge=" + userAge +
                ", duplicateStores=" + duplicateStores +
                ", alertMessage='" + alertMessage + '\'' +
                ", detectionTime='" + detectionTime + '\'' +
                '}';
    }
}
