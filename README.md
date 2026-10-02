# HANNOON - 실시간 프랜차이즈 매출 분석 파이프라인

> SK Shieldus 부트캠프 최종 프로젝트 | 팀 3인 | 2025.03 ~ 2025.06
> 담당: 아키텍처 설계 · 인프라 · Kafka · Flink

프랜차이즈 매장에서 발생하는 영수증 데이터를 **Kafka로 수집하고, Flink로 실시간 처리**해서
매출 집계, 매장 랭킹, 중복 결제 탐지 결과를 만들어내는 End-to-End 스트리밍 파이프라인입니다.

이 저장소는 팀 프로젝트 중 제가 담당한 **데이터 파이프라인 파트(Producer + Flink 3종)** 를 한곳에 모은 것입니다.
대시보드 등 전체 프로젝트는 팀 원본 조직 [KFC-KafkaFriedCoders](https://github.com/KFC-KafkaFriedCoders)에서 볼 수 있습니다.

---

## 아키텍처

```
┌────────────┐   Avro    ┌──────────────────────────────┐   JSON    ┌───────────────────────────┐
│  producer  │ ───────▶  │  Kafka (Confluent, KRaft)    │ ───────▶  │ 결과 토픽 (대시보드가 구독)  │
│ Spring Boot│ test-topic│  + Schema Registry           │           │                           │
└────────────┘           └──────────────┬───────────────┘           │ • sales_total_realtime    │
                                        │                           │ • franchise-top-stores    │
                           ┌────────────┼─────────────┐             │ • payment_same_user       │
                           ▼            ▼             ▼             └───────────────────────────┘
                   flink-sales-total  flink-top-store  flink-duplicate-detector
                   (누적 매출 집계)    (매장 TOP 3)     (중복 결제 탐지)
```

## 구성

| 폴더 | 역할 | 입력 → 출력 |
|---|---|---|
| [`producer`](./producer) | 더미 영수증 데이터를 주기적으로 생성해 Kafka로 전송 (Spring Boot) | Excel 더미 데이터 → `test-topic` |
| [`flink-sales-total`](./flink-sales-total) | 프랜차이즈별 당일 누적 매출과 매장 수를 실시간 집계 (상태 기반 처리) | `test-topic` → `sales_total_realtime` |
| [`flink-top-store`](./flink-top-store) | 프랜차이즈별 매출 상위 3개 매장 랭킹, 변동이 있을 때만 출력 | `test-topic` → `franchise-top-stores` |
| [`flink-duplicate-detector`](./flink-duplicate-detector) | 같은 사용자가 10초 안에 서로 다른 매장에서 결제하면 중복 결제로 탐지 (Tumbling Window) | `test-topic` → `payment_same_user` |

각 폴더는 독립된 프로그램이라 따로 빌드하고 실행합니다. 서로 직접 호출하지 않고 **Kafka 토픽으로만 연결**됩니다.

## 기술 스택

| 구분 | 사용 기술 |
|---|---|
| 메시지 큐 | Confluent Kafka (KRaft 모드, 11개 노드), Kafka Connect |
| 스트림 처리 | Apache Flink 1.18 (3개 병렬 Job), 체크포인트 기반 장애 복구 |
| 데이터 포맷 | Avro + Schema Registry, 출력은 JSON |
| 백엔드 | Spring Boot (Producer) |
| 빌드 / 실행 | Gradle, Java 17 |
| 인프라 | AWS EC2 (11개 인스턴스) |

## 주요 성과

- 22시간 이상 무중단 안정 가동 (체크포인트 기반 장애 복구)
- 영수증 발생 즉시 초 단위로 매출 집계 갱신
- Tumbling Window 기반 중복 결제 이상 거래 탐지
- 상태(State) 기반 TOP 3 매장 실시간 랭킹
- 분산 환경 트러블슈팅 5건 직접 해결

## 실행 방법

접속 주소는 모두 `localhost` 예시값으로 바꿔 두었습니다. 각 폴더의 `src/main/resources/application.properties`에서
본인의 Kafka 브로커와 Schema Registry 주소로 수정한 뒤 실행하세요.

```bash
# 1. 데이터 생성 (producer)
cd producer
./gradlew bootRun

# 2. Flink 애플리케이션 (원하는 것을 각각 실행)
cd flink-sales-total          # 또는 flink-top-store, flink-duplicate-detector
./gradlew clean shadowJar
java --add-opens java.base/java.util=ALL-UNNAMED \
     --add-opens java.base/java.lang=ALL-UNNAMED \
     -jar build/libs/*.jar
```

자세한 설정과 데이터 스키마는 각 폴더의 README를 참고하세요.
