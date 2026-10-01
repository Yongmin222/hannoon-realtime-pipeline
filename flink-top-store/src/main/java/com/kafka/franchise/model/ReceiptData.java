package com.kafka.franchise.model;

import java.util.List;

/**
 * 영수증 데이터 모델 클래스
 * 
 * 카프카에서 수신한 Avro 영수증 데이터를 Java 객체로 표현합니다.
 * 영수증에는 프랜차이즈/매장 정보, 주문 메뉴 항목, 총 가격, 고객 정보 등이 포함됩니다.
 * 이 클래스는 receipt.avsc 스키마에 정의된 구조와 일치합니다.
 */
public class ReceiptData {
    // 프랜차이즈 식별자
    private int franchise_id;
    // 매장 브랜드명
    private String store_brand;
    // 매장 식별자
    private int store_id;
    // 매장 이름
    private String store_name;
    // 매장 지역
    private String region;
    // 매장 주소
    private String store_address;
    // 주문한 메뉴 항목 목록
    private List<MenuItem> menu_items;
    // 총 주문 금액
    private int total_price;
    // 고객 식별자
    private int user_id;
    // 주문 시간 (ISO 형식: YYYY-MM-DD HH:MM:SS)
    private String time;
    // 고객 이름
    private String user_name;
    // 고객 성별
    private String user_gender;
    // 고객 나이
    private int user_age;

    /**
     * 기본 생성자
     * Avro 데이터 변환 및 Flink 상태 관리에 필요
     */
    public ReceiptData() {}

    // --- Getters and Setters ---
    
    /**
     * 프랜차이즈 ID 반환
     * @return 프랜차이즈 ID
     */
    public int getFranchise_id() {
        return franchise_id;
    }

    /**
     * 프랜차이즈 ID 설정
     * @param franchise_id 프랜차이즈 ID
     */
    public void setFranchise_id(int franchise_id) {
        this.franchise_id = franchise_id;
    }

    /**
     * 매장 브랜드 반환
     * @return 매장 브랜드명
     */
    public String getStore_brand() {
        return store_brand;
    }

    /**
     * 매장 브랜드 설정
     * @param store_brand 매장 브랜드명
     */
    public void setStore_brand(String store_brand) {
        this.store_brand = store_brand;
    }

    /**
     * 매장 ID 반환
     * @return 매장 ID
     */
    public int getStore_id() {
        return store_id;
    }

    /**
     * 매장 ID 설정
     * @param store_id 매장 ID
     */
    public void setStore_id(int store_id) {
        this.store_id = store_id;
    }

    /**
     * 매장 이름 반환
     * @return 매장 이름
     */
    public String getStore_name() {
        return store_name;
    }

    /**
     * 매장 이름 설정
     * @param store_name 매장 이름
     */
    public void setStore_name(String store_name) {
        this.store_name = store_name;
    }

    /**
     * 매장 지역 반환
     * @return 매장 지역
     */
    public String getRegion() {
        return region;
    }

    /**
     * 매장 지역 설정
     * @param region 매장 지역
     */
    public void setRegion(String region) {
        this.region = region;
    }

    /**
     * 매장 주소 반환
     * @return 매장 주소
     */
    public String getStore_address() {
        return store_address;
    }

    /**
     * 매장 주소 설정
     * @param store_address 매장 주소
     */
    public void setStore_address(String store_address) {
        this.store_address = store_address;
    }

    /**
     * 메뉴 항목 목록 반환
     * @return 메뉴 항목 목록
     */
    public List<MenuItem> getMenu_items() {
        return menu_items;
    }

    /**
     * 메뉴 항목 목록 설정
     * @param menu_items 메뉴 항목 목록
     */
    public void setMenu_items(List<MenuItem> menu_items) {
        this.menu_items = menu_items;
    }

    /**
     * 총 주문 금액 반환
     * @return 총 주문 금액
     */
    public int getTotal_price() {
        return total_price;
    }

    /**
     * 총 주문 금액 설정
     * @param total_price 총 주문 금액
     */
    public void setTotal_price(int total_price) {
        this.total_price = total_price;
    }

    /**
     * 고객 ID 반환
     * @return 고객 ID
     */
    public int getUser_id() {
        return user_id;
    }

    /**
     * 고객 ID 설정
     * @param user_id 고객 ID
     */
    public void setUser_id(int user_id) {
        this.user_id = user_id;
    }

    /**
     * 주문 시간 반환
     * @return 주문 시간 (ISO 형식: YYYY-MM-DD HH:MM:SS)
     */
    public String getTime() {
        return time;
    }

    /**
     * 주문 시간 설정
     * @param time 주문 시간 (ISO 형식: YYYY-MM-DD HH:MM:SS)
     */
    public void setTime(String time) {
        this.time = time;
    }

    /**
     * 고객 이름 반환
     * @return 고객 이름
     */
    public String getUser_name() {
        return user_name;
    }

    /**
     * 고객 이름 설정
     * @param user_name 고객 이름
     */
    public void setUser_name(String user_name) {
        this.user_name = user_name;
    }

    /**
     * 고객 성별 반환
     * @return 고객 성별
     */
    public String getUser_gender() {
        return user_gender;
    }

    /**
     * 고객 성별 설정
     * @param user_gender 고객 성별
     */
    public void setUser_gender(String user_gender) {
        this.user_gender = user_gender;
    }

    /**
     * 고객 나이 반환
     * @return 고객 나이
     */
    public int getUser_age() {
        return user_age;
    }

    /**
     * 고객 나이 설정
     * @param user_age 고객 나이
     */
    public void setUser_age(int user_age) {
        this.user_age = user_age;
    }

    /**
     * 메뉴 항목 클래스
     * 
     * 영수증에 포함된 개별 메뉴 항목 정보를 저장합니다.
     * 메뉴 ID, 이름, 단가, 수량 정보를 포함합니다.
     */
    public static class MenuItem {
        // 메뉴 ID
        private int menu_id;
        // 메뉴 이름
        private String menu_name;
        // 메뉴 단가
        private int unit_price;
        // 주문 수량
        private int quantity;

        /**
         * 기본 생성자
         */
        public MenuItem() {}

        /**
         * 메뉴 ID 반환
         * @return 메뉴 ID
         */
        public int getMenu_id() {
            return menu_id;
        }

        /**
         * 메뉴 ID 설정
         * @param menu_id 메뉴 ID
         */
        public void setMenu_id(int menu_id) {
            this.menu_id = menu_id;
        }

        /**
         * 메뉴 이름 반환
         * @return 메뉴 이름
         */
        public String getMenu_name() {
            return menu_name;
        }

        /**
         * 메뉴 이름 설정
         * @param menu_name 메뉴 이름
         */
        public void setMenu_name(String menu_name) {
            this.menu_name = menu_name;
        }

        /**
         * 메뉴 단가 반환
         * @return 메뉴 단가
         */
        public int getUnit_price() {
            return unit_price;
        }

        /**
         * 메뉴 단가 설정
         * @param unit_price 메뉴 단가
         */
        public void setUnit_price(int unit_price) {
            this.unit_price = unit_price;
        }

        /**
         * 주문 수량 반환
         * @return 주문 수량
         */
        public int getQuantity() {
            return quantity;
        }

        /**
         * 주문 수량 설정
         * @param quantity 주문 수량
         */
        public void setQuantity(int quantity) {
            this.quantity = quantity;
        }
    }
}
