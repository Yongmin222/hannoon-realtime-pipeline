# Kafka Purchase Data Producer

간단한 Kafka 구매 데이터 프로듀서입니다.

## 🚀 사용법

### 실행
```bash
docker compose -f docker-compose.noport.yml up -d --build
```

### 설정 변경
`docker-compose.noport.yml` 파일에서 이 두 값만 수정:
```yaml
- APP_SCHEDULER_FIXED_RATE=1000    # 전송 주기 (밀리초)
- APP_DATA_GENERATE_COUNT=1        # 한 번에 보낼 개수
```

### 재시작
```bash
docker compose -f docker-compose.noport.yml down
docker compose -f docker-compose.noport.yml up -d --build
```

### 로그 확인
```bash
docker logs -f kafka-purchase-producer
```

## 📋 설정 예시
- `1000, 1` = 1초마다 1개 (실시간)
- `5000, 10` = 5초마다 10개 (개발용)  
- `1000, 100` = 1초마다 100개 (부하테스트)
