package com.kafka.detector.model;

import java.util.List;

/**
 * Receipt data from Avro format
 */
public class ReceiptData {
    private int franchiseId;
    private String storeBrand;
    private int storeId;
    private String storeName;
    private String region;
    private String storeAddress;
    private List<MenuItem> menuItems;
    private int totalPrice;
    private int userId;
    private String time;
    private String userName;
    private String userGender;
    private int userAge;
    
    public static class MenuItem {
        private int menuId;
        private String menuName;
        private int unitPrice;
        private int quantity;
        
        // Getters and Setters
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
    }
    
    // Getters and Setters
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
}
