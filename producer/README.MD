# Producer

`Dummy_data.xlsx`의 매장·메뉴 데이터를 랜덤으로 조합해 영수증 데이터(Avro)를 만들고, 일정 주기로 Kafka 토픽에 전송하는 프로듀서입니다.

## 구성

- Java 17, Spring Boot 3.2, Spring Kafka
- Avro 직렬화 + Confluent Schema Registry
- 전송 토픽: `test-topic`

## 실행 전 준비

1. Kafka 브로커와 Schema Registry를 먼저 띄웁니다.
2. `src/main/resources/application.properties`에서 주소를 본인 환경에 맞게 수정합니다.
   - `spring.kafka.bootstrap-servers` (기본 `localhost:9092`)
   - `kafka.schema-registry-url` (기본 `http://localhost:8081`)

## 실행

IntelliJ에서 `Test1Application`을 실행하거나, `producer` 폴더에서 아래 명령을 실행합니다.

```bash
./gradlew bootRun
```

`Dummy_data.xlsx`는 실행 위치(`producer` 폴더) 기준 상대 경로로 읽습니다.

## 설정

`application.properties`에서 수정합니다.

| 설정 | 설명 | 기본값 |
|---|---|---|
| `app.scheduler.fixed-rate` | 전송 주기 (ms) | 1000 |
| `app.data.generate-count` | 한 번에 보낼 건수 | 1 |
| `app.data.excel-path` | 원본 엑셀 경로 | `./Dummy_data.xlsx` |

## 참고

원래 AWS EC2 서버에서 Kafka 클러스터를 구성해 운영했고, 저장소에는 로컬 주소로 바꿔 올렸습니다.
