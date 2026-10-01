# Duplicate Payment Detector

## 📋 프로젝트 개요

실시간으로 중복 결제를 탐지하는 Apache Flink 애플리케이션입니다. 동일 사용자가 짧은 시간 내에 여러 매장에서 결제하는 패턴을 감지하여 알림을 생성합니다.

## 🏗️ 아키텍처

```
Kafka (test-topic)
      ↓ Avro 형식
Receipt Data Source
      ↓
Receipt Validation & Filtering
      ↓
Key by User (name_gender_age)
      ↓
Time Window (10초)
      ↓
Duplicate Detection
      ↓
Kafka (payment_same_user) JSON 형식
```

## 📂 프로젝트 구조

```
src/main/java/com/kafka/duplicatedetector/
├── DuplicatePaymentDetectorApp.java     # 메인 애플리케이션
├── functions/                           # 데이터 처리 함수들
│   ├── UserKeySelector.java            # 사용자 기반 키 선택자
│   ├── ReceiptToFilteredMapper.java    # 데이터 필터링 매퍼
│   └── DuplicateDetectionWindowFunction.java # 중복 탐지 윈도우 함수
├── model/                               # 데이터 모델들
│   ├── ReceiptData.java                 # 영수증 데이터
│   ├── FilteredReceiptData.java         # 필터링된 영수증 데이터
│   └── DuplicateAlert.java              # 중복 알림 데이터
└── utils/                               # 유틸리티 클래스들
    ├── AppProperties.java               # 설정 관리
    ├── SimpleAvroDeserializationSchema.java  # Avro 역직렬화
    └── DuplicateAlertJsonSerializationSchema.java # JSON 직렬화
```

## 🚨 중복 탐지 조건

1. **동일 사용자**: 이름, 성별, 나이가 동일한 사용자
2. **시간 윈도우**: 설정된 시간(기본 10초) 내에 발생
3. **서로 다른 매장**: 2개 이상의 서로 다른 매장에서 결제
4. **결제 건수**: 윈도우 내 2건 이상의 결제

## ⚙️ 설정

### application.properties
```properties
# Kafka 설정
kafka.bootstrap.servers=localhost:9092
kafka.source.topic=test-topic
kafka.sink.topic=payment_same_user
kafka.consumer.group=duplicate-payment-detector

# Flink 설정
flink.window.size.seconds=10
flink.checkpoint.interval=60000

# 애플리케이션 설정
app.name=duplicate-payment-detector
app.version=1.0.0
```

## 🔄 데이터 플로우

### 입력 데이터 (Avro)
- **토픽**: test-topic
- **형식**: Confluent Schema Registry Avro
- **구조**: 영수증 데이터 (사용자 정보, 매장 정보, 메뉴 정보, 결제 정보)

### 출력 데이터 (JSON)
- **토픽**: payment_same_user
- **형식**: JSON
- **구조**: 중복 알림 (사용자 정보, 중복 매장 리스트, 알림 메시지, 탐지 시간)

### 출력 예시
```json
{
  "userId": "12345",
  "userName": "김철수",
  "userGender": "남성",
  "userAge": 30,
  "duplicateStores": [
    "스타벅스 - 강남점",
    "이디야 - 역삼점"
  ],
  "alertMessage": "결제자: 김철수 (12345), 의심스러운 결제가 탐지되었습니다: 스타벅스 - 강남점, 이디야 - 역삼점",
  "detectionTime": "2025-05-22 15:30:45"
}
```

## 🚀 실행 방법

### 1. 빌드
```bash
./gradlew build
```

### 2. Shadow JAR 생성
```bash
./gradlew shadowJar
```

### 3. 실행
```bash
java -jar build/libs/duplicate-payment-detector.jar
```

### 4. Gradle을 통한 실행
```bash
./gradlew run
```

## 🔧 주요 기능

### 1. 실시간 스트리밍 처리
- Apache Flink를 사용한 실시간 데이터 처리
- 체크포인트를 통한 장애 복구
- At-Least-Once 의미론으로 데이터 손실 방지

### 2. 윈도우 기반 분석
- 시간 기반 윈도우 (Tumbling Window)
- 사용자별 그룹화
- 설정 가능한 윈도우 크기

### 3. 스마트 중복 탐지
- 동일 매장 내 여러 결제는 제외
- 서로 다른 매장에서의 결제만 중복으로 판단
- 사용자 정보 기반 정확한 식별

### 4. 설정 기반 운영
- properties 파일을 통한 중앙화된 설정 관리
- 런타임 설정 변경 없이 재시작만으로 적용
- 환경별 설정 분리 가능

## 📊 모니터링 및 로깅

### 로그 레벨
- **INFO**: 일반적인 처리 상황
- **DEBUG**: 상세한 처리 과정 (개발/디버깅용)
- **WARN**: 중복 탐지 알림
- **ERROR**: 오류 상황

### 주요 메트릭스
- 처리된 영수증 수
- 탐지된 중복 결제 수
- 윈도우 처리 시간
- Kafka 처리량

## 🛠️ 개발 환경

- **Java**: 17+
- **Apache Flink**: 1.18.0
- **Kafka**: Compatible with Confluent Platform
- **Build Tool**: Gradle 8.1.1
- **Serialization**: Avro, JSON

## 📝 변경 이력

### v1.0.0 (2025-05-22)
- 초기 버전 릴리스
- sales-total-realtime 구조 기반 리팩토링
- AppProperties를 통한 설정 관리 개선
- 패키지 구조 정리 (com.kafka.duplicatedetector)
- 상세한 로깅 및 문서화 추가

## 🔍 트러블슈팅

### 일반적인 문제들

1. **Kafka 연결 실패**
   - bootstrap.servers 설정 확인
   - 네트워크 연결 상태 확인

2. **Avro 역직렬화 실패**
   - Schema Registry 연결 상태 확인
   - 스키마 호환성 확인

3. **중복 탐지 안됨**
   - 윈도우 크기 설정 확인
   - 사용자 키 생성 로직 검증

4. **성능 이슈**
   - 병렬도 조정
   - 체크포인트 간격 조정
   - Kafka 파티션 수 확인
