# 🏪 Franchise Top Store Application

Apache Flink 기반의 프랜차이즈별 상위 매장 실시간 분석 스트리밍 애플리케이션입니다.

## 📋 프로젝트 개요

### 🎯 **주요 기능**
- **실시간 매장 랭킹**: Kafka에서 영수증 데이터를 실시간으로 처리하여 매장별 매출 순위 생성
- **프랜차이즈별 집계**: 프랜차이즈 단위로 상위 3개 매장 추출
- **오늘 날짜 필터링**: 당일 데이터만 선별하여 처리
- **상태 기반 처리**: Flink 상태를 이용한 실시간 매장별 매출 누적
- **동적 랭킹 업데이트**: 매출 변동 시에만 결과 전송으로 효율성 극대화

### 🔄 **데이터 플로우**
```
Kafka Topic (Avro)     →     Flink Processing     →     Kafka Topic (JSON)
     ↓                             ↓                           ↓
  영수증 데이터              매장별 매출 집계 &             상위 매장 랭킹
 (test-topic)               상위 3개 매장 추출         (franchise-top-stores)
```

---

## ⚙️ 설정 및 실행

### 🔧 **환경 설정**

#### **application.properties 설정**
```properties
# Kafka 연결 설정
kafka.bootstrap.servers=localhost:9092
kafka.source.topic=test-topic
kafka.sink.topic=franchise-top-stores
kafka.consumer.group=franchise-top-store-finder

# Flink 실행 설정
flink.parallelism=1
flink.checkpoint.interval=60000

# 애플리케이션 로직 설정
application.timezone=Asia/Seoul
application.top.store.count=3
```

### 🚀 **빌드 및 실행**

```bash
# 빌드
./gradlew clean shadowJar

# 실행
java --add-opens java.base/java.util=ALL-UNNAMED \
     --add-opens java.base/java.lang=ALL-UNNAMED \
     -jar build/libs/franchise-top-store.jar
```

---

## 📊 데이터 스키마

### 📥 **입력 데이터 (ReceiptData)**
```json
{
  "franchise_id": 101,
  "store_brand": "Starbucks",
  "store_id": 1001,
  "store_name": "강남점",
  "region": "서울",
  "total_price": 9000,
  "time": "2025-05-22 14:30:15"
}
```

### 📤 **출력 데이터 (TopStoreRankingData)**
```json
{
  "franchise_id": 101,
  "timestamp": "2025-05-22 14:30:15",
  "top_stores": [
    {
      "rank": 1,
      "store_id": 1001,
      "store_name": "강남점",
      "store_brand": "Starbucks",
      "total_sales": 1500000
    }
  ]
}
```

---

## 🎯 핵심 비즈니스 로직

### 🏆 **상위 매장 선정 알고리즘**

1. **📊 매출 집계**: 매장별로 당일 총 매출 누적
2. **📈 순위 계산**: 매출 기준 내림차순 정렬
3. **🔄 변화 감지**: 순위 변동 및 매출 변화 감지
4. **📤 효율적 출력**: 변화가 있을 때만 결과 전송

---

## 🛠️ 개발 가이드

### 🔄 **핵심 처리 로직**

```java
// 1. 데이터 수신 및 필터링
DataStream<ReceiptData> receiptStream = env.fromSource(source, ...)
    .filter(new TodayReceiptFilter())

// 2. 키 기반 분할
    .keyBy(new FranchiseKeySelector())

// 3. 상태 기반 집계
    .process(new DailyTopStoreProcessor())
```

---

## 🚨 트러블슈팅

| 문제 | 증상 | 해결방법 |
|------|------|----------|
| **메모리 부족** | OutOfMemoryError | 체크포인트 간격 단축 |
| **순위 중복** | 같은 순위의 매장들 | 매장 ID 기준 보조 정렬 |
| **지연 증가** | 결과 출력 지연 | 병렬도 증가 |

---

## 🎯 마무리

**Franchise Top Store Application**은 실시간 스트리밍 데이터 처리를 통해 프랜차이즈 운영에 핵심적인 인사이트를 제공합니다.

### ✨ **핵심 가치**
- **⚡ 실시간성**: 매출 발생 즉시 상위 매장 랭킹 업데이트
- **🎯 정확성**: 상태 기반 정확한 매출 집계 및 순위 계산
- **💡 효율성**: 변화가 있을 때만 결과 전송으로 리소스 최적화
- **🔧 확장성**: 프랜차이즈 수와 매장 수에 관계없이 확장 가능

### 🚀 **시작하기**
```bash
# 1. 빌드
./gradlew clean shadowJar

# 2. 실행
java -jar build/libs/franchise-top-store.jar
```

---

**Happy Streaming! 🎉**
