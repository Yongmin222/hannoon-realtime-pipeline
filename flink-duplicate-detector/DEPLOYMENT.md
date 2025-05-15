# Flink 실행 가이드

## 개발 환경에서 테스트
```bash
# 1. 로컬에서 직접 실행
./gradlew run

# 2. IntelliJ에서 실행
# DuplicatePaymentDetectorApp.java 파일을 열고 main 메서드 실행
```

## 프로덕션 환경 배포

### 1. JAR 빌드
```bash
# Fat JAR 생성 (모든 의존성 포함)
./gradlew shadowJar

# 생성된 JAR 확인
ls -la build/libs/
# duplicate-payment-detector-1.0.0-all.jar
```

### 2. Flink 클러스터에 제출

#### Standalone 클러스터
```bash
# Flink 클러스터 시작
$FLINK_HOME/bin/start-cluster.sh

# Job 제출
$FLINK_HOME/bin/flink run \
    -c com.kafka.detector.DuplicatePaymentDetectorApp \
    build/libs/duplicate-payment-detector-1.0.0-all.jar
```

#### Flink on YARN
```bash
# YARN 세션 시작
$FLINK_HOME/bin/yarn-session.sh -n 2 -jm 1024m -tm 2048m

# Job 제출
$FLINK_HOME/bin/flink run \
    -c com.kafka.detector.DuplicatePaymentDetectorApp \
    build/libs/duplicate-payment-detector-1.0.0-all.jar
```

### 3. Docker로 실행
```bash
# Docker Compose 실행
docker-compose up -d

# Flink Web UI 접속
# http://localhost:8081

# Job 제출 (Web UI 또는 CLI)
docker exec duplicate-payment-detector_jobmanager_1 \
    flink run \
    -c com.kafka.detector.DuplicatePaymentDetectorApp \
    /opt/flink/jars/duplicate-payment-detector-1.0.0-all.jar
```

### 4. Kubernetes 배포
```bash
# Flink Operator 설치
kubectl create -f https://github.com/apache/flink-kubernetes-operator/releases/download/release-1.5.0/flink-kubernetes-operator-1.5.0.yaml

# 애플리케이션 배포
kubectl apply -f k8s-deployment.yaml

# 상태 확인
kubectl get flinkapp
kubectl logs -f deployment/duplicate-payment-detector
```

## 모니터링

- Flink Web UI: http://localhost:8081
- 로그 확인: `duplicate-payment-detector.log`
- Metrics: Flink metrics 시스템 사용

## 주의사항

1. Kafka가 실행 중이어야 함
2. `test-topic`에 데이터가 들어와야 함
3. 메모리 설정은 처리량에 따라 조정 필요
4. 체크포인트 설정으로 장애 복구 가능
