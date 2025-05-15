# Duplicate Payment Detector 프로젝트 정리

## 삭제된 파일들 (사용되지 않는 스키마 파일들)
- AvroSchemaRegistryDeserializationSchema.java
- BasicAvroDeserializationSchema.java
- ConfluentRegistryAvroDeserializationSchema.java
- DuplicateAlertSerializationSchema.java
- ReceiptAvroDeserializationSchema.java
- ReceiptJsonDeserializationSchema.java
- SimpleAvroDeserializationSchema.java

## 남은 파일들 (현재 사용 중)
- SimpleSchemaRegistryAvroDeserializationSchema.java (Avro 데이터 읽기)
- DuplicateAlertJsonSerializationSchema.java (JSON 데이터 쓰기)

## 프로젝트 구조
```
duplicate-payment-detector
├── src/main/java/com/kafka/detector
│   ├── DuplicatePaymentDetectorApp.java (메인 앱)
│   ├── functions/
│   │   ├── DuplicateDetectionWindowFunction.java
│   │   ├── ReceiptToFilteredMapper.java
│   │   └── UserKeySelector.java
│   ├── model/
│   │   ├── DuplicateAlert.java
│   │   ├── FilteredReceiptData.java
│   │   └── ReceiptData.java
│   └── utils/
│       ├── SimpleSchemaRegistryAvroDeserializationSchema.java
│       └── DuplicateAlertJsonSerializationSchema.java
```
