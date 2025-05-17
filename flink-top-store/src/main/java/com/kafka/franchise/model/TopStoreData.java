package com.kafka.franchise.model;

public class TopStoreData {
    private int franchise_id;
    private int store_id;
    private String store_name;
    private String store_brand;
    private String store_address;
    private int total_sales;
    private String timestamp;

    // Default constructor
    public TopStoreData() {}

    // Full constructor
    public TopStoreData(
            int franchise_id,
            int store_id,
            String store_name,
            String store_brand,
            String store_address,
            int total_sales,
            String timestamp) {
        this.franchise_id = franchise_id;
        this.store_id = store_id;
        this.store_name = store_name;
        this.store_brand = store_brand;
        this.store_address = store_address;
        this.total_sales = total_sales;
        this.timestamp = timestamp;
    }

    // Getters and Setters
    public int getFranchise_id() {
        return franchise_id;
    }

    public void setFranchise_id(int franchise_id) {
        this.franchise_id = franchise_id;
    }

    public int getStore_id() {
        return store_id;
    }

    public void setStore_id(int store_id) {
        this.store_id = store_id;
    }

    public String getStore_name() {
        return store_name;
    }

    public void setStore_name(String store_name) {
        this.store_name = store_name;
    }

    public String getStore_brand() {
        return store_brand;
    }

    public void setStore_brand(String store_brand) {
        this.store_brand = store_brand;
    }

    public String getStore_address() {
        return store_address;
    }

    public void setStore_address(String store_address) {
        this.store_address = store_address;
    }

    public int getTotal_sales() {
        return total_sales;
    }

    public void setTotal_sales(int total_sales) {
        this.total_sales = total_sales;
    }

    public String getTimestamp() {
        return timestamp;
    }

    public void setTimestamp(String timestamp) {
        this.timestamp = timestamp;
    }

    @Override
    public String toString() {
        return "TopStoreData{" +
                "franchise_id=" + franchise_id +
                ", store_id=" + store_id +
                ", store_name='" + store_name + '\'' +
                ", store_brand='" + store_brand + '\'' +
                ", store_address='" + store_address + '\'' +
                ", total_sales=" + total_sales +
                ", timestamp='" + timestamp + '\'' +
                '}';
    }
}
