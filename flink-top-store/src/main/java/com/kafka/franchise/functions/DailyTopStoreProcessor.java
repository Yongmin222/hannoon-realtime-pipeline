package com.kafka.franchise.functions;

import com.kafka.franchise.model.ReceiptData;
import com.kafka.franchise.model.TopStoreRankingData;
import com.kafka.franchise.utils.AppProperties;
import org.apache.flink.api.common.state.MapState;
import org.apache.flink.api.common.state.MapStateDescriptor;
import org.apache.flink.api.common.state.ValueState;
import org.apache.flink.api.common.state.ValueStateDescriptor;
import org.apache.flink.api.common.typeinfo.Types;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.streaming.api.functions.KeyedProcessFunction;
import org.apache.flink.util.Collector;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.text.SimpleDateFormat;
import java.util.*;
import java.util.stream.Collectors;
import java.util.stream.StreamSupport;

/**
 * 프랜차이즈별 일일 매출 상위 매장을 처리하는 Flink 처리 함수
 * 
 * 이 클래스는 각 프랜차이즈별로 매장의 일일 매출을 집계하고,
 * 매출 기준 상위 매장을 추출하여 결과를 생성합니다.
 * Flink의 상태(state) 관리 기능을 활용해 매장별 매출을 누적하고,
 * 변경 사항이 있을 때만 결과를 출력합니다.
 */
public class DailyTopStoreProcessor extends KeyedProcessFunction<Integer, ReceiptData, TopStoreRankingData> {
    private static final Logger LOG = LoggerFactory.getLogger(DailyTopStoreProcessor.class);
    
    // 날짜와 시간 포맷 (YYYY-MM-DD 및 YYYY-MM-DD HH:MM:SS 형식)
    private static final SimpleDateFormat DATE_FORMAT = new SimpleDateFormat("yyyy-MM-dd");
    private static final SimpleDateFormat DATETIME_FORMAT = new SimpleDateFormat("yyyy-MM-dd HH:mm:ss");
    
    // 상위 몇 개 매장을 추출할지 설정 (AppProperties에서 동적으로 가져옴)
    private static final int TOP_STORE_COUNT = AppProperties.getTopStoreCount();
    
    /**
     * 생성자에서 한국 시간대 설정
     * SimpleDateFormat이 올바른 시간대로 날짜를 포맷팅하도록 합니다.
     */
    public DailyTopStoreProcessor() {
        // AppProperties에서 설정된 시간대 사용
        TimeZone timeZone = TimeZone.getTimeZone(AppProperties.getTimezone());
        DATE_FORMAT.setTimeZone(timeZone);
        DATETIME_FORMAT.setTimeZone(timeZone);
    }
    
    // --- 상태(State) 변수 선언 ---
    
    // 마지막으로 처리한 날짜 (YYYY-MM-DD 형식)
    // 날짜가 바뀌면 모든 매장 정보를 초기화하기 위해 사용
    private ValueState<String> lastDateState;
    
    // 매장별 정보 및 누적 매출을 저장하는 맵 상태
    // 키: 매장 ID, 값: 매장 정보(StoreInfo)
    private MapState<Integer, StoreInfo> storeInfoState;
    
    // 마지막으로 전송한 매장의 순위 정보 저장
    // 키: 매장 ID, 값: 순위(1부터 시작)
    private MapState<Integer, Integer> lastRankingState;
    
    // 마지막으로 전송한 매장의 매출 정보 저장
    // 키: 매장 ID, 값: 누적 매출액
    private MapState<Integer, Integer> lastSalesState;
    
    /**
     * 매장 정보를 저장하기 위한 내부 클래스
     * 매장 ID, 이름, 브랜드, 주소, 지역 및 누적 매출 정보를 포함
     */
    public static class StoreInfo {
        // 매장 ID
        public int storeId;
        // 매장 이름
        public String storeName;
        // 매장 브랜드
        public String storeBrand;
        // 매장 주소
        public String storeAddress;
        // 매장 지역
        public String region;
        // 누적 매출액
        public int totalSales;
        
        // 기본 생성자 (Flink 상태 관리에 필요)
        public StoreInfo() {}
        
        // 전체 필드 초기화 생성자
        public StoreInfo(int storeId, String storeName, String storeBrand, 
                         String storeAddress, String region, int totalSales) {
            this.storeId = storeId;
            this.storeName = storeName;
            this.storeBrand = storeBrand;
            this.storeAddress = storeAddress;
            this.region = region;
            this.totalSales = totalSales;
        }
    }
    
    /**
     * Flink 작업 초기화 시 호출되는 메소드
     * 상태(state) 변수들을 초기화합니다.
     * 
     * @param parameters 설정 파라미터
     * @throws Exception 초기화 중 발생할 수 있는 예외
     */
    @Override
    public void open(Configuration parameters) throws Exception {
        // 마지막 처리 날짜 상태 초기화
        lastDateState = getRuntimeContext().getState(
            new ValueStateDescriptor<>("lastDate", String.class));
            
        // 매장별 정보 상태 초기화
        storeInfoState = getRuntimeContext().getMapState(
            new MapStateDescriptor<>("storeInfo", Types.INT, Types.POJO(StoreInfo.class)));
            
        // 매장별 마지막 순위 상태 초기화
        lastRankingState = getRuntimeContext().getMapState(
            new MapStateDescriptor<>("lastRanking", Types.INT, Types.INT));
            
        // 매장별 마지막 매출 상태 초기화
        lastSalesState = getRuntimeContext().getMapState(
            new MapStateDescriptor<>("lastSales", Types.INT, Types.INT));
    }
    
    /**
     * 각 영수증 데이터를 처리하는 핵심 메소드
     * 영수증을 받아 매장별 매출을 업데이트하고, 상위 매장 정보를 생성합니다.
     * 
     * @param receipt 처리할 영수증 데이터
     * @param ctx 컨텍스트 객체 (타이머 서비스 등 포함)
     * @param out 결과를 출력할 컬렉터
     * @throws Exception 처리 중 발생할 수 있는 예외
     */
    @Override
    public void processElement(ReceiptData receipt, Context ctx, Collector<TopStoreRankingData> out) throws Exception {
        // AppProperties에서 설정된 시간대 명시적으로 사용
        TimeZone timeZone = TimeZone.getTimeZone(AppProperties.getTimezone());
        DATE_FORMAT.setTimeZone(timeZone);
        DATETIME_FORMAT.setTimeZone(timeZone);
        
        // 영수증의 날짜 추출 (처음 10자리만 사용 - YYYY-MM-DD 형식)
        String receiptDate = receipt.getTime().substring(0, 10);
        String lastDate = lastDateState.value();
        
        // 영수증 날짜가 바뀌면 상태 초기화 (날짜가 변경되었거나 첫 데이터인 경우)
        if (lastDate == null || !receiptDate.equals(lastDate)) {
            LOG.info("New day detected in receipt ({}). Resetting state for franchise {}", 
                    receiptDate, receipt.getFranchise_id());
            storeInfoState.clear();
            lastRankingState.clear();
            lastSalesState.clear();
            lastDateState.update(receiptDate);
        }
        
        // 현재 영수증의 매장 정보 가져오기
        int storeId = receipt.getStore_id();
        StoreInfo storeInfo = storeInfoState.get(storeId);
        
        // 신규 매장이면 정보 생성
        if (storeInfo == null) {
            storeInfo = new StoreInfo(
                storeId,
                receipt.getStore_name(),
                receipt.getStore_brand(),
                receipt.getStore_address(),
                receipt.getRegion(),
                0
            );
            LOG.info("New store detected: {} - {} for franchise {}", 
                    storeId, receipt.getStore_name(), receipt.getFranchise_id());
        }
        
        // 매출 업데이트 전의 이전 매출 금액 저장 (변경 여부 확인용)
        int oldSales = storeInfo.totalSales;
        
        // 현재 영수증의 매출 금액을 누적
        storeInfo.totalSales += receipt.getTotal_price();
        
        // 업데이트된 매장 정보를 상태에 저장
        storeInfoState.put(storeId, storeInfo);
        
        // 상위 매장 추출 및 변경 사항이 있으면 결과 생성
        // 변경된 매장 ID와 이전 매출을 함께 전달하여 변경 여부 확인
        checkAndEmitTopStores(receipt.getFranchise_id(), out, storeId, oldSales);
        
        // 타이머 설정 (첫 영수증인 경우에만)
        // 주기적으로 상태를 확인하기 위한 타이머 등록
        if (lastDate == null) {
            setPeriodicTimer(ctx);
        }
    }
    
    /**
     * 상위 매장 추출 및 결과 생성
     * 변경 사항이 있는 경우에만 결과를 출력합니다.
     * 
     * @param franchiseId 프랜차이즈 ID
     * @param out 결과를 출력할 컬렉터
     * @param updatedStoreId 업데이트된 매장 ID (변경 확인용)
     * @param oldSales 이전 매출 금액 (변경 확인용)
     * @throws Exception 처리 중 발생할 수 있는 예외
     */
    private void checkAndEmitTopStores(int franchiseId, Collector<TopStoreRankingData> out, 
                                     int updatedStoreId, int oldSales) throws Exception {
        // 모든 매장 정보를 리스트로 변환
        // MapState는 직접 정렬이 불가능하므로 일반 리스트로 변환
        List<StoreInfo> allStores = StreamSupport
                .stream(storeInfoState.entries().spliterator(), false)
                .map(Map.Entry::getValue)
                .collect(Collectors.toList());
        
        // 매출 기준 내림차순 정렬 (높은 매출 순)
        allStores.sort((a, b) -> Integer.compare(b.totalSales, a.totalSales));
        
        // 전체 매장 수 로깅
        LOG.info("Total stores for franchise {}: {}", franchiseId, allStores.size());
        
        // 매출 순위에 따라 상위 N개 매장 선택
        // 매장 수가 TOP_STORE_COUNT보다 적을 수 있으므로 최소값 사용
        List<TopStoreRankingData.StoreRankingInfo> topStores = new ArrayList<>();
        int maxRanking = Math.min(TOP_STORE_COUNT, allStores.size());
        
        LOG.info("Max ranking for franchise {}: {}", franchiseId, maxRanking);
        
        // 변경 사항 확인을 위한 플래그
        boolean hasChanges = false;
        
        // 현재 상위 매장의 ID를 저장하는 Set (이전에 상위였지만 현재 아닌 매장 확인용)
        Set<Integer> currentTopStoreIds = new HashSet<>();
        
        // 상위 매장 데이터 생성 및 변경 사항 확인
        for (int i = 0; i < maxRanking; i++) {
            StoreInfo store = allStores.get(i);
            int rank = i + 1;  // 순위는 1부터 시작
            
            // 현재 상위 매장 ID 기록
            currentTopStoreIds.add(store.storeId);
            
            // 이전 순위 및 매출 가져오기
            Integer lastRank = lastRankingState.get(store.storeId);
            Integer lastSale = lastSalesState.get(store.storeId);
            
            // 순위나 매출에 변화가 있는지 확인
            // 1) 신규 진입 매장 (lastRank가 null)
            // 2) 순위 변경 (lastRank != rank)
            // 3) 매출 변경 (lastSale != store.totalSales)
            if (lastRank == null || lastRank != rank || lastSale == null || lastSale != store.totalSales) {
                hasChanges = true;
                
                LOG.info("Rank change detected for store {} - {}: prev_rank={}, new_rank={}, prev_sales={}, new_sales={}",
                        store.storeId, store.storeName, 
                        (lastRank != null ? lastRank : "null"), rank,
                        (lastSale != null ? lastSale : "null"), store.totalSales);
            }
            
            // 현재 순위 및 매출 정보 상태에 저장 (다음 비교를 위해)
            lastRankingState.put(store.storeId, rank);
            lastSalesState.put(store.storeId, store.totalSales);
            
            // 랭킹 정보 생성
            TopStoreRankingData.StoreRankingInfo rankingInfo = new TopStoreRankingData.StoreRankingInfo(
                rank,               // 순위 (1부터 시작)
                store.storeId,      // 매장 ID
                store.storeName,    // 매장 이름
                store.storeBrand,   // 매장 브랜드
                store.storeAddress, // 매장 주소
                store.totalSales    // 누적 매출
            );
            topStores.add(rankingInfo);
            
            LOG.info("Added rank {} store to result: {} - {} ({}원)", 
                    rank, store.storeId, store.storeName, store.totalSales);
        }
        
        // 이전에 상위 N위였지만 현재 아닌 매장을 확인
        // 먼저 삭제할 매장 ID를 목록에 저장 (concurrent modification 방지)
        List<Integer> storeIdsToRemove = new ArrayList<>();
        
        // 모든 이전 순위를 가진 매장을 확인
        for (Map.Entry<Integer, Integer> entry : lastRankingState.entries()) {
            int storeId = entry.getKey();
            int rank = entry.getValue();
            
            // 이전에 상위 N위였지만 현재 리스트에 없는 경우
            // (순위권에서 밀려난 경우)
            if (!currentTopStoreIds.contains(storeId) && rank <= TOP_STORE_COUNT) {
                hasChanges = true;
                storeIdsToRemove.add(storeId);  // 나중에 삭제하기 위해 목록에 추가
                
                LOG.info("Store dropped from top {}: store_id={}, prev_rank={}", 
                        TOP_STORE_COUNT, storeId, rank);
            }
        }
        
        // 반복이 끝난 후 한꺼번에 상태에서 삭제
        // (반복 중 상태를 변경하면 ConcurrentModificationException 발생 가능)
        for (Integer storeId : storeIdsToRemove) {
            lastRankingState.remove(storeId);
            lastSalesState.remove(storeId);
        }
        
        // 최종 결과 내용 로깅
        LOG.info("Result for franchise {}: topStores.size()={}, hasChanges={}", 
                franchiseId, topStores.size(), hasChanges);
        
        // 변경 사항이 있는 경우에만 결과 출력
        // 불필요한 데이터 전송을 방지하기 위함
        if (hasChanges && !topStores.isEmpty()) {
            LOG.info("Changes detected in top {} stores for franchise {}: {}", 
                    maxRanking, franchiseId, 
                    topStores.stream()
                        .map(s -> String.format("%d. %s (%d원)", s.getRank(), s.getStore_name(), s.getTotal_sales()))
                        .collect(Collectors.joining(", ")));
            
            // 현재 타임스탬프 생성 (YYYY-MM-DD HH:MM:SS 형식)
            String timestamp = DATETIME_FORMAT.format(new Date());
            
            // 최종 결과 객체 생성
            TopStoreRankingData result = new TopStoreRankingData(
                franchiseId,  // 프랜차이즈 ID
                topStores,    // 상위 매장 목록
                timestamp     // 타임스탬프
            );
            
            LOG.info("Emitting result with {} stores", topStores.size());
            out.collect(result);  // 결과 출력
        } else {
            LOG.debug("No changes in top stores for franchise {}", franchiseId);
        }
    }
    
    /**
     * 주기적으로 현재 상태를 확인하기 위한 타이머 설정
     * 
     * @param ctx 컨텍스트 객체 (타이머 서비스 접근용)
     */
    private void setPeriodicTimer(Context ctx) {
        // 10초마다 현재 상태 확인을 위한 타이머 등록
        long currentTime = ctx.timerService().currentProcessingTime();
        ctx.timerService().registerProcessingTimeTimer(currentTime + 10000); // 10초
    }
    
    /**
     * 타이머가 호출될 때 실행되는 메소드
     * 주기적으로 상위 매장 정보를 확인하고, 필요시 결과를 출력합니다.
     * 
     * @param timestamp 타이머 타임스탬프
     * @param ctx 타이머 컨텍스트
     * @param out 결과를 출력할 컬렉터
     * @throws Exception 처리 중 발생할 수 있는 예외
     */
    @Override
    public void onTimer(long timestamp, OnTimerContext ctx, Collector<TopStoreRankingData> out) throws Exception {
        // 타이머가 실행되면 현재 매장 정보를 확인하지만, 
        // 변경 사항이 없으면 출력하지 않음 (체크만 함)
        int franchiseId = ctx.getCurrentKey();
        checkAndEmitTopStores(franchiseId, out, -1, -1);
        
        // 다음 타이머 설정 (주기적 실행 지속)
        setPeriodicTimer(ctx);
    }
}
