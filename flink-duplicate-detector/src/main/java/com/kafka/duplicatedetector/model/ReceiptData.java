package com.kafka.duplicatedetector.model;

import java.util.List;

/**
 * =======================================================
 * 영수증 데이터 모델 클래스
 * =======================================================
 * 
 * 📋 기능:
 * - Avro 형식에서 역직렬화된 영수증 데이터를 담는 POJO
 * - 매장 정보, 메뉴 아이템, 사용자 정보 포함
 * - 중복 결제 탐지의 입력 데이터로 사용
 * 
 * 🏗️ 구조:
 * - 매장 정보: 프랜차이즈 ID, 브랜드, 매장명, 주소 등
 * - 메뉴 정보: 주문 아이템 리스트
 * - 사용자 정보: 사용자 ID, 이름, 성별, 나이
 * - 결제 정보: 총 금액, 시간
 */
public class ReceiptData {
    
    // =======================================================
    // 매장 정보 필드
    // =======================================================
    private int franchiseId;      // 프랜차이즈 ID
    private String storeBrand;    // 매장 브랜드명
    private int storeId;          // 매장 ID
    private String storeName;     // 매장명
    private String region;        // 지역
    private String storeAddress;  // 매장 주소
    
    // =======================================================
    // 주문 정보 필드
    // =======================================================
    private List<MenuItem> menuItems;  // 주문 메뉴 리스트
    private int totalPrice;            // 총 결제 금액
    
    // =======================================================
    // 사용자 정보 필드
    // =======================================================
    private int userId;           // 사용자 ID
    private String userName;      // 사용자 이름
    private String userGender;    // 사용자 성별
    private int userAge;          // 사용자 나이
    
    // =======================================================
    // 결제 시간 정보
    // =======================================================
    private String time;          // 결제 시간 (문자열 형태)
    
    /**
     * =======================================================
     * 메뉴 아이템 내부 클래스
     * =======================================================
     * 
     * 📋 기능:
     * - 개별 메뉴 아이템 정보를 담는 클래스
     * - 메뉴 ID, 이름, 단가, 수량 정보 포함
     */
    public static class MenuItem {
        private int menuId;       // 메뉴 ID
        private String menuName;  // 메뉴명
        private int unitPrice;    // 단가
        private int quantity;     // 주문 수량
        
        // =======================================================
        // MenuItem Getters and Setters
        // =======================================================
        
        public int getMenuId() {
            return menuId;
        }
        
        public void setMenuId(int menuId) {
            this.menuId = menuId;
        }
        
        public String getMenuName() {
            return menuName;
        }
        
        public void setMenuName(String menuName) {
            this.menuName = menuName;
        }
        
        public int getUnitPrice() {
            return unitPrice;
        }
        
        public void setUnitPrice(int unitPrice) {
            this.unitPrice = unitPrice;
        }
        
        public int getQuantity() {
            return quantity;
        }
        
        public void setQuantity(int quantity) {
            this.quantity = quantity;
        }
        
        @Override
        public String toString() {
            return "MenuItem{" +
                    "menuId=" + menuId +
                    ", menuName='" + menuName + '\'' +
                    ", unitPrice=" + unitPrice +
                    ", quantity=" + quantity +
                    '}';
        }
    }
    
    // =======================================================
    // ReceiptData Getters and Setters
    // =======================================================
    
    public int getFranchiseId() {
        return franchiseId;
    }
    
    public void setFranchiseId(int franchiseId) {
        this.franchiseId = franchiseId;
    }
    
    public String getStoreBrand() {
        return storeBrand;
    }
    
    public void setStoreBrand(String storeBrand) {
        this.storeBrand = storeBrand;
    }
    
    public int getStoreId() {
        return storeId;
    }
    
    public void setStoreId(int storeId) {
        this.storeId = storeId;
    }
    
    public String getStoreName() {
        return storeName;
    }
    
    public void setStoreName(String storeName) {
        this.storeName = storeName;
    }
    
    public String getRegion() {
        return region;
    }
    
    public void setRegion(String region) {
        this.region = region;
    }
    
    public String getStoreAddress() {
        return storeAddress;
    }
    
    public void setStoreAddress(String storeAddress) {
        this.storeAddress = storeAddress;
    }
    
    public List<MenuItem> getMenuItems() {
        return menuItems;
    }
    
    public void setMenuItems(List<MenuItem> menuItems) {
        this.menuItems = menuItems;
    }
    
    public int getTotalPrice() {
        return totalPrice;
    }
    
    public void setTotalPrice(int totalPrice) {
        this.totalPrice = totalPrice;
    }
    
    public int getUserId() {
        return userId;
    }
    
    public void setUserId(int userId) {
        this.userId = userId;
    }
    
    public String getTime() {
        return time;
    }
    
    public void setTime(String time) {
        this.time = time;
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
    
    @Override
    public String toString() {
        return "ReceiptData{" +
                "franchiseId=" + franchiseId +
                ", storeBrand='" + storeBrand + '\'' +
                ", storeId=" + storeId +
                ", storeName='" + storeName + '\'' +
                ", region='" + region + '\'' +
                ", storeAddress='" + storeAddress + '\'' +
                ", menuItems=" + menuItems +
                ", totalPrice=" + totalPrice +
                ", userId=" + userId +
                ", userName='" + userName + '\'' +
                ", userGender='" + userGender + '\'' +
                ", userAge=" + userAge +
                ", time='" + time + '\'' +
                '}';
    }
}
