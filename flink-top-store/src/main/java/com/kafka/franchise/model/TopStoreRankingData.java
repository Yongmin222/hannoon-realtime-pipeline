package com.kafka.franchise.model;

import java.util.List;

/**
 * 프랜차이즈별 상위 매장 랭킹 데이터 모델 클래스
 * 
 * 각 프랜차이즈별로 매출 기준 상위 N개 매장의 랭킹 정보를 저장합니다.
 * 이 클래스는 Flink 처리 후 카프카로 출력되는 최종 결과 데이터 구조입니다.
 */
public class TopStoreRankingData {
    // 프랜차이즈 ID
    private int franchise_id;
    // 상위 매장 랭킹 정보 목록
    private List<StoreRankingInfo> top_stores;
    // 결과 생성 타임스탬프
    private String timestamp;

    /**
     * 매장 랭킹 정보를 저장하기 위한 내부 클래스
     * 
     * 개별 매장의 랭킹, 매장 정보, 매출액 등을 저장합니다.
     */
    public static class StoreRankingInfo {
        // 매장 순위 (1부터 시작)
        private int rank;
        // 매장 ID
        private int store_id;
        // 매장 이름
        private String store_name;
        // 매장 브랜드
        private String store_brand;
        // 매장 주소
        private String store_address;
        // 총 매출액
        private int total_sales;

        /**
         * 기본 생성자
         */
        public StoreRankingInfo() {}

        /**
         * 전체 필드 초기화 생성자
         * 
         * @param rank 매장 순위 (1부터 시작)
         * @param store_id 매장 ID
         * @param store_name 매장 이름
         * @param store_brand 매장 브랜드
         * @param store_address 매장 주소
         * @param total_sales 총 매출액
         */
        public StoreRankingInfo(int rank, int store_id, String store_name, 
                           String store_brand, String store_address, int total_sales) {
            this.rank = rank;
            this.store_id = store_id;
            this.store_name = store_name;
            this.store_brand = store_brand;
            this.store_address = store_address;
            this.total_sales = total_sales;
        }

        /**
         * 매장 순위 반환
         * @return 매장 순위 (1부터 시작)
         */
        public int getRank() {
            return rank;
        }

        /**
         * 매장 순위 설정
         * @param rank 매장 순위 (1부터 시작)
         */
        public void setRank(int rank) {
            this.rank = rank;
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
         * 매장 브랜드 반환
         * @return 매장 브랜드
         */
        public String getStore_brand() {
            return store_brand;
        }

        /**
         * 매장 브랜드 설정
         * @param store_brand 매장 브랜드
         */
        public void setStore_brand(String store_brand) {
            this.store_brand = store_brand;
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
         * 총 매출액 반환
         * @return 총 매출액
         */
        public int getTotal_sales() {
            return total_sales;
        }

        /**
         * 총 매출액 설정
         * @param total_sales 총 매출액
         */
        public void setTotal_sales(int total_sales) {
            this.total_sales = total_sales;
        }
    }

    /**
     * 기본 생성자
     */
    public TopStoreRankingData() {}

    /**
     * 전체 필드 초기화 생성자
     * 
     * @param franchise_id 프랜차이즈 ID
     * @param top_stores 상위 매장 목록
     * @param timestamp 결과 생성 타임스탬프
     */
    public TopStoreRankingData(int franchise_id, List<StoreRankingInfo> top_stores, String timestamp) {
        this.franchise_id = franchise_id;
        this.top_stores = top_stores;
        this.timestamp = timestamp;
    }

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
     * 상위 매장 목록 반환
     * @return 상위 매장 랭킹 정보 목록
     */
    public List<StoreRankingInfo> getTop_stores() {
        return top_stores;
    }

    /**
     * 상위 매장 목록 설정
     * @param top_stores 상위 매장 랭킹 정보 목록
     */
    public void setTop_stores(List<StoreRankingInfo> top_stores) {
        this.top_stores = top_stores;
    }

    /**
     * 타임스탬프 반환
     * @return 결과 생성 타임스탬프
     */
    public String getTimestamp() {
        return timestamp;
    }

    /**
     * 타임스탬프 설정
     * @param timestamp 결과 생성 타임스탬프
     */
    public void setTimestamp(String timestamp) {
        this.timestamp = timestamp;
    }
}
