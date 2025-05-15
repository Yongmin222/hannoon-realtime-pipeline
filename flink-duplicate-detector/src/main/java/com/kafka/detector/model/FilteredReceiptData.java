package com.kafka.detector.model;

/**
 * Filtered receipt data containing only user and store information
 */
public class FilteredReceiptData {
    private String userId;
    private String userName;
    private String userGender;
    private int userAge;
    private String storeBrand;
    private String storeName;
    private String time;
    
    public FilteredReceiptData() {}
    
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
