package com.example.test1.service;

import com.example.test1.entity.MenuItem;
import com.example.test1.entity.ReceiptData;
import org.apache.poi.ss.usermodel.*;
import org.apache.poi.xssf.usermodel.XSSFWorkbook;
import org.springframework.stereotype.Service;

import java.io.FileInputStream;
import java.io.IOException;
import java.time.LocalDateTime;
import java.time.format.DateTimeFormatter;
import java.util.*;

@Service
public class ReceiptDataService {

    private final Random random = new Random();

    private static final int COL_FRANCHISE_ID = 0;
    private static final int COL_STORE_BRAND = 1;
    private static final int COL_STORE_ID = 2;
    private static final int COL_STORE_NAME = 3;
    private static final int COL_REGION = 4;
    private static final int COL_STORE_ADDRESS = 5;
    private static final int COL_MENU_ID = 6;
    private static final int COL_MENU_NAME = 7;
    private static final int COL_PRICE = 8;
    private static final int COL_USER_ID = 12;
    private static final int COL_USER_NAME = 13;
    private static final int COL_USER_GENDER = 14;
    private static final int COL_USER_AGE = 15;

    public List<ReceiptData> generateGroupedShuffledDataFromExcel(String filePath, int generateCount) throws IOException {
        // 시트별로 데이터를 저장하기 위한 리스트 맵 생성
        Map<Integer, List<StoreData>> sheetToStoreDataMap = new HashMap<>();
        Map<Integer, List<MenuData>> sheetToMenuDataMap = new HashMap<>(); // 중요: 시트별 메뉴 데이터 보관

        // 모든 시트에서 공통으로 사용할 사용자 데이터
        List<UserData> userDataList = new ArrayList<>();
        List<String> genderList = new ArrayList<>();
        List<Integer> ageList = new ArrayList<>();

        // 유효한 시트 인덱스를 저장할 리스트
        List<Integer> validSheetIndices = new ArrayList<>();

        try (FileInputStream fis = new FileInputStream(filePath);
             Workbook workbook = new XSSFWorkbook(fis)) {

            // 사용자 데이터, 성별, 나이는 모든 시트에서 공통으로 수집
            Set<Integer> userIds = new HashSet<>();

            for (int sheetIndex = 0; sheetIndex < workbook.getNumberOfSheets(); sheetIndex++) {
                Sheet sheet = workbook.getSheetAt(sheetIndex);

                // 각 시트별 데이터를 담을 리스트 초기화
                List<StoreData> storeDataList = new ArrayList<>();
                List<MenuData> menuDataList = new ArrayList<>(); // 시트별 메뉴 데이터 리스트

                Set<Integer> storeIds = new HashSet<>();
                Set<Integer> menuIds = new HashSet<>();

                for (int i = 1; i <= sheet.getLastRowNum(); i++) {
                    Row row = sheet.getRow(i);
                    if (row == null) continue;

                    // 가게 및 메뉴 데이터 수집 (시트별로 구분)
                    Integer franchiseId = getIntCell(row, COL_FRANCHISE_ID);
                    String storeBrand = getStringCell(row, COL_STORE_BRAND);
                    Integer storeId = getIntCell(row, COL_STORE_ID);
                    String storeName = getStringCell(row, COL_STORE_NAME);
                    String region = getStringCell(row, COL_REGION);
                    String storeAddress = getStringCell(row, COL_STORE_ADDRESS);

                    if (franchiseId != null && storeId != null && storeBrand != null && !storeBrand.trim().isEmpty() && storeName != null && !storeName.trim().isEmpty()) {
                        if (storeIds.add(storeId)) {
                            storeDataList.add(new StoreData(franchiseId, storeBrand, storeId, storeName, region, storeAddress));
                        }
                    }

                    Integer menuId = getIntCell(row, COL_MENU_ID);
                    String menuName = getStringCell(row, COL_MENU_NAME);
                    Integer unitPrice = getIntCell(row, COL_PRICE);

                    if (menuId != null && menuName != null && !menuName.trim().isEmpty() && unitPrice != null) {
                        if (menuIds.add(menuId)) {
                            menuDataList.add(new MenuData(menuId, menuName, unitPrice)); // 시트별 메뉴 데이터 추가
                        }
                    }

                    // 사용자 데이터, 성별, 나이 수집 (모든 시트에서 공통)
                    String userIdStr = getUserIdAsString(row, COL_USER_ID);
                    String userName = getStringCell(row, COL_USER_NAME);
                    String gender = getStringCell(row, COL_USER_GENDER);
                    Integer age = getIntCell(row, COL_USER_AGE);

                    if (gender != null && !gender.trim().isEmpty()) genderList.add(gender.trim());
                    if (age != null) ageList.add(age);

                    try {
                        Integer userId = Integer.parseInt(userIdStr);
                        if (userName != null && !userName.trim().isEmpty() && userIds.add(userId)) {
                            userDataList.add(new UserData(userId, userName.trim()));
                        }
                    } catch (NumberFormatException ignored) {}
                }

                // 유효한 가게 및 메뉴 데이터가 있는 시트만 저장
                if (!storeDataList.isEmpty() && !menuDataList.isEmpty()) {
                    sheetToStoreDataMap.put(sheetIndex, storeDataList);
                    sheetToMenuDataMap.put(sheetIndex, menuDataList);
                    validSheetIndices.add(sheetIndex);
                }
            }
        }

        // 데이터가 없으면 예외 처리 또는 빈 리스트 반환
        if (validSheetIndices.isEmpty() || userDataList.isEmpty() || genderList.isEmpty() || ageList.isEmpty()) {
            System.err.println("엑셀 파일에서 유효한 데이터를 충분히 읽어오지 못했습니다.");
            return Collections.emptyList();
        }

        // 각 시트의 데이터를 섞기
        for (Integer sheetIndex : validSheetIndices) {
            Collections.shuffle(sheetToStoreDataMap.get(sheetIndex));
            Collections.shuffle(sheetToMenuDataMap.get(sheetIndex)); // 시트별 메뉴 데이터 섞기
        }

        // 공통 데이터도 섞기
        Collections.shuffle(userDataList);
        Collections.shuffle(genderList);
        Collections.shuffle(ageList);

        DateTimeFormatter formatter = DateTimeFormatter.ofPattern("yyyy-MM-dd HH:mm:ss");
        List<ReceiptData> resultList = new ArrayList<>();

        // 전체 영수증 생성 루프
        for (int i = 0; i < generateCount; i++) {
            // 랜덤하게 시트(가게) 선택
            int randomSheetIndex = validSheetIndices.get(random.nextInt(validSheetIndices.size()));

            List<StoreData> storeDataList = sheetToStoreDataMap.get(randomSheetIndex);
            List<MenuData> menuDataList = sheetToMenuDataMap.get(randomSheetIndex); // 선택된 시트의 메뉴 데이터

            // 랜덤하게 가게 선택
            StoreData store = storeDataList.get(random.nextInt(storeDataList.size()));

            // 랜덤하게 사용자 선택 (공통 풀에서)
            UserData user = userDataList.get(random.nextInt(userDataList.size()));
            String gender = genderList.get(random.nextInt(genderList.size()));
            int age = ageList.get(random.nextInt(ageList.size()));

            // 랜덤하게 메뉴 선택 (시트별 메뉴 풀에서)
            int menuCount = random.nextInt(7) + 1; // 영수증당 메뉴 개수 랜덤 선택 (1~7개)
            List<MenuData> selectedMenus = new ArrayList<>(menuDataList);
            Collections.shuffle(selectedMenus);
            selectedMenus = selectedMenus.subList(0, Math.min(menuCount, selectedMenus.size()));

            List<MenuItem> menuItems = new ArrayList<>();
            int receiptTotal = 0;

            for (MenuData menu : selectedMenus) {
                int quantity = random.nextInt(4) + 1;
                int unitPrice = menu.unitPrice != null ? menu.unitPrice : 0;
                int itemTotal = unitPrice * quantity;
                receiptTotal += itemTotal;

                String menuName = (menu.menuName != null && !menu.menuName.trim().isEmpty()) ? menu.menuName.trim() : "메뉴 이름 없음";

                MenuItem item = MenuItem.newBuilder()
                        .setMenuId(menu.menuId != null ? menu.menuId : 0)
                        .setMenuName(menuName)
                        .setUnitPrice(unitPrice)
                        .setQuantity(quantity)
                        .build();

                menuItems.add(item);
            }

            ReceiptData receipt = ReceiptData.newBuilder()
                    .setFranchiseId(store.franchiseId != null ? store.franchiseId : 0)
                    .setStoreBrand((store.storeBrand != null && !store.storeBrand.trim().isEmpty()) ? store.storeBrand.trim() : "브랜드 없음")
                    .setStoreId(store.storeId != null ? store.storeId : 0)
                    .setStoreName((store.storeName != null && !store.storeName.trim().isEmpty()) ? store.storeName.trim() : "가게 이름 없음")
                    .setRegion((store.region != null && !store.region.trim().isEmpty()) ? store.region.trim() : "지역 정보 없음")
                    .setStoreAddress((store.storeAddress != null && !store.storeAddress.trim().isEmpty()) ? store.storeAddress.trim() : "주소 정보 없음")
                    .setMenuItems(menuItems)
                    .setTotalPrice(receiptTotal)
                    .setUserId(user.userId != null ? user.userId : 0)
                    .setUserName((user.userName != null && !user.userName.trim().isEmpty()) ? user.userName.trim() : "사용자 이름 없음")
                    .setUserGender((gender != null && !gender.trim().isEmpty()) ? gender.trim() : "성별 정보 없음")
                    .setUserAge(age)
                    .setTime(LocalDateTime.now().format(formatter))
                    .build();

            resultList.add(receipt);
        }

        return resultList;
    }

    // 나머지 헬퍼 메소드들은 기존과 동일하게 유지
    private String getUserIdAsString(Row row, int colIdx) {
        Cell cell = row.getCell(colIdx);
        if (cell == null) return "";
        return switch (cell.getCellType()) {
            case STRING -> cell.getStringCellValue().trim();
            case NUMERIC -> String.valueOf((long) cell.getNumericCellValue());
            default -> "";
        };
    }

    private String getStringCell(Row row, int colIdx) {
        Cell cell = row.getCell(colIdx);
        if (cell == null) return null;
        return switch (cell.getCellType()) {
            case STRING -> cell.getStringCellValue();
            case NUMERIC -> String.valueOf((int) cell.getNumericCellValue());
            case BOOLEAN -> String.valueOf(cell.getBooleanCellValue());
            case FORMULA -> {
                try {
                    yield cell.getStringCellValue();
                } catch (IllegalStateException e) {
                    yield String.valueOf(cell.getNumericCellValue());
                }
            }
            default -> null;
        };
    }

    private Integer getIntCell(Row row, int colIdx) {
        Cell cell = row.getCell(colIdx);
        if (cell == null) return null;
        return switch (cell.getCellType()) {
            case NUMERIC -> (int) cell.getNumericCellValue();
            case STRING -> {
                try {
                    yield Integer.parseInt(cell.getStringCellValue().trim());
                } catch (NumberFormatException e) {
                    yield null;
                }
            }
            default -> null;
        };
    }

    private static class StoreData {
        Integer franchiseId; String storeBrand; Integer storeId; String storeName; String region; String storeAddress;
        StoreData(Integer f, String b, Integer i, String n, String r, String a) {
            franchiseId = f; storeBrand = b; storeId = i; storeName = n; region = r; storeAddress = a;
        }
    }

    private static class MenuData {
        Integer menuId; String menuName; Integer unitPrice;
        MenuData(Integer id, String name, Integer price) {
            menuId = id; menuName = name; unitPrice = price;
        }
    }

    private static class UserData {
        Integer userId; String userName;
        UserData(Integer id, String name) {
            userId = id; userName = name;
        }
    }
}