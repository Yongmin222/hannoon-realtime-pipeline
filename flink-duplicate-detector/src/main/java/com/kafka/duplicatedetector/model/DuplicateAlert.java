package com.kafka.duplicatedetector.model;

import java.util.ArrayList;
import java.util.List;

/**
 * =======================================================
 * 중복 결제 알림 데이터 모델
 * =======================================================
 * 
 * 📋 기능:
 * - 중복 결제가 탐지되었을 때 생성되는 알림 데이터
 * - 사용자 정보와 중복 발생 매장 리스트 포함
 * - JSON 형식으로 Kafka에 전송되는 최종 출력 데이터
 * 
 * 🚨 알림 조건:
 * - 동일 사용자가 짧은 시간 내에 여러 매장에서 결제
 * - 윈도우 시간 내에 서로 다른 매장에서 결제 발생
 * 
 * 📤 출력 형식:
 * - Kafka Topic: payment_same_user
 * - Format: JSON
 */
public class DuplicateAlert {
    
    // =======================================================
    // 사용자 정보 필드
    // =======================================================
    private String userId;        // 사용자 ID (문자열)
    private String userName;      // 사용자 이름
    private String userGender;    // 사용자 성별
    private int userAge;          // 사용자 나이
    
    // =======================================================
    // 중복 탐지 정보 필드
    // =======================================================
    private List<String> duplicateStores;  // 중복 발생 매장 리스트
    private String alertMessage;           // 알림 메시지
    private String detectionTime;          // 탐지 시간
    
    /**
     * =======================================================
     * 기본 생성자
     * =======================================================
     * 
     * 🏗️ 초기화:
     * - duplicateStores를 빈 ArrayList로 초기화
     */
    public DuplicateAlert() {
        this.duplicateStores = new ArrayList<>();
    }
    
    /**
     * =======================================================
     * 편의 생성자
     * =======================================================
     * 
     * @param userId 사용자 ID
     * @param userName 사용자 이름
     * @param userGender 사용자 성별
     * @param userAge 사용자 나이
     * @param alertMessage 알림 메시지
     * @param detectionTime 탐지 시간
     */
    public DuplicateAlert(String userId, String userName, String userGender, 
                         int userAge, String alertMessage, String detectionTime) {
        this();
        this.userId = userId;
        this.userName = userName;
        this.userGender = userGender;
        this.userAge = userAge;
        this.alertMessage = alertMessage;
        this.detectionTime = detectionTime;
    }
    
    // =======================================================
    // Getters and Setters
    // =======================================================
    
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
        this.duplicateStores = duplicateStores != null ? duplicateStores : new ArrayList<>();
    }
    
    /**
     * =======================================================
     * 중복 매장 추가 메서드
     * =======================================================
     * 
     * @param storeName 추가할 매장명
     */
    public void addDuplicateStore(String storeName) {
        if (storeName != null && !this.duplicateStores.contains(storeName)) {
            this.duplicateStores.add(storeName);
        }
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
