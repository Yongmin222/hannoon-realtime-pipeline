package com.kafka.duplicatedetector.model;

/**
 * =======================================================
 * 필터링된 영수증 데이터 모델
 * =======================================================
 * 
 * 📋 기능:
 * - 중복 탐지에 필요한 핵심 정보만 추출한 경량화된 데이터
 * - 원본 ReceiptData에서 사용자와 매장 정보만 선별
 * - 윈도우 기반 처리에서 중간 데이터로 사용
 * 
 * 🔄 변환 과정:
 * ReceiptData → FilteredReceiptData → DuplicateAlert
 * 
 * 📊 포함 정보:
 * - 사용자 정보: ID, 이름, 성별, 나이
 * - 매장 정보: 브랜드, 매장명
 * - 시간 정보: 결제 시간
 */
public class FilteredReceiptData {
    
    // =======================================================
    // 사용자 정보 필드
    // =======================================================
    private String userId;        // 사용자 ID (문자열)
    private String userName;      // 사용자 이름
    private String userGender;    // 사용자 성별
    private int userAge;          // 사용자 나이
    
    // =======================================================
    // 매장 정보 필드
    // =======================================================
    private String storeBrand;    // 매장 브랜드명
    private String storeName;     // 매장명
    
    // =======================================================
    // 시간 정보 필드
    // =======================================================
    private String time;          // 결제 시간
    
    /**
     * =======================================================
     * 기본 생성자
     * =======================================================
     */
    public FilteredReceiptData() {}
    
    /**
     * =======================================================
     * 전체 필드 생성자
     * =======================================================
     * 
     * @param userId 사용자 ID
     * @param userName 사용자 이름
     * @param userGender 사용자 성별
     * @param userAge 사용자 나이
     * @param storeBrand 매장 브랜드
     * @param storeName 매장명
     * @param time 결제 시간
     */
    public FilteredReceiptData(String userId, String userName, String userGender, 
                              int userAge, String storeBrand, String storeName, String time) {
        this.userId = userId;
        this.userName = userName;
        this.userGender = userGender;
        this.userAge = userAge;
        this.storeBrand = storeBrand;
        this.storeName = storeName;
        this.time = time;
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
    
    public String getStoreBrand() {
        return storeBrand;
    }
    
    public void setStoreBrand(String storeBrand) {
        this.storeBrand = storeBrand;
    }
    
    public String getStoreName() {
        return storeName;
    }
    
    public void setStoreName(String storeName) {
        this.storeName = storeName;
    }
    
    public String getTime() {
        return time;
    }
    
    public void setTime(String time) {
        this.time = time;
    }
    
    /**
     * =======================================================
     * 매장 식별자 생성 메서드
     * =======================================================
     * 
     * @return "브랜드명 - 매장명" 형태의 식별자
     */
    public String getStoreIdentifier() {
        return storeBrand + " - " + storeName;
    }
    
    @Override
    public String toString() {
        return "FilteredReceiptData{" +
                "userId='" + userId + '\'' +
                ", userName='" + userName + '\'' +
                ", userGender='" + userGender + '\'' +
                ", userAge=" + userAge +
                ", storeBrand='" + storeBrand + '\'' +
                ", storeName='" + storeName + '\'' +
                ", time='" + time + '\'' +
                '}';
    }
}
