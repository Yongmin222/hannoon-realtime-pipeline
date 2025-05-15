package com.kafka.detector;

import com.kafka.detector.functions.DuplicateDetectionWindowFunction;
import com.kafka.detector.functions.ReceiptToFilteredMapper;
import com.kafka.detector.functions.UserKeySelector;
import com.kafka.detector.model.DuplicateAlert;
import com.kafka.detector.model.FilteredReceiptData;
import com.kafka.detector.model.ReceiptData;
import com.kafka.detector.utils.DuplicateAlertJsonSerializationSchema;
import com.kafka.detector.utils.SimpleSchemaRegistryAvroDeserializationSchema;
import org.apache.flink.api.common.eventtime.WatermarkStrategy;
import org.apache.flink.connector.kafka.sink.KafkaRecordSerializationSchema;
import org.apache.flink.connector.kafka.sink.KafkaSink;
import org.apache.flink.connector.kafka.source.KafkaSource;
import org.apache.flink.connector.kafka.source.enumerator.initializer.OffsetsInitializer;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.windowing.assigners.TumblingProcessingTimeWindows;
import org.apache.flink.streaming.api.windowing.time.Time;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.Properties;

/**
 * Duplicate Payment Detection Flink Application
 */
public class DuplicatePaymentDetectorApp {
    
    private static final Logger logger = LoggerFactory.getLogger(DuplicatePaymentDetectorApp.class);
    
    // Kafka configuration
    private static final String KAFKA_BOOTSTRAP_SERVERS = "localhost:9092";
    private static final String SOURCE_TOPIC = "test-topic";
    private static final String SINK_TOPIC = "payment-same-user";
    private static final String CONSUMER_GROUP = "duplicate-payment-detector-test";
    
    // Window configuration
    private static final int WINDOW_SIZE_SECONDS = 10;
    
    public static void main(String[] args) throws Exception {
        logger.info("Starting Duplicate Payment Detector Application");

        // Set up Flink execution environment
        final StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        
        // Enable checkpointing for fault tolerance
        env.enableCheckpointing(60000); // Checkpoint every minute
        
        // Create Kafka source
        KafkaSource<ReceiptData> kafkaSource = createKafkaSource();
        
        // Data stream processing pipeline
        DataStream<ReceiptData> receiptStream = env.fromSource(
                kafkaSource, 
                WatermarkStrategy.noWatermarks(), 
                "Kafka Source"
            )
            .filter(receipt -> {
                if (receipt == null) {
                    logger.warn("Received null receipt");
                    return false;
                }
                logger.info("Received receipt from user: {}", receipt.getUserId());
                return true;
            })
            .name("Filter Null Receipts");
        
        // Print raw data for debugging
        receiptStream.print("Raw Receipt");
        
        DataStream<FilteredReceiptData> filteredStream = receiptStream
                .map(new ReceiptToFilteredMapper())
                .name("Filter Receipt Data");
        
        // Print filtered data for debugging
        filteredStream.print("Filtered Receipt");
        
        DataStream<DuplicateAlert> alertStream = filteredStream
                .keyBy(new UserKeySelector())
                .window(TumblingProcessingTimeWindows.of(Time.seconds(WINDOW_SIZE_SECONDS)))
                .process(new DuplicateDetectionWindowFunction())
                .name("Detect Duplicates");
        
        // Send alerts to Kafka
        KafkaSink<DuplicateAlert> kafkaSink = createKafkaSink();
        alertStream.sinkTo(kafkaSink)
                .name("Kafka Sink");
        
        // Print to console for debugging
        alertStream.print("Duplicate Alert");
        
        // Execute the Flink job
        env.execute("Duplicate Payment Detection Application");
    }
    
    /**
     * Create Kafka source using the new KafkaSource API
     */
    private static KafkaSource<ReceiptData> createKafkaSource() {
        return KafkaSource.<ReceiptData>builder()
                .setBootstrapServers(KAFKA_BOOTSTRAP_SERVERS)
                .setTopics(SOURCE_TOPIC)
                .setGroupId(CONSUMER_GROUP)
                .setStartingOffsets(OffsetsInitializer.earliest())
                .setValueOnlyDeserializer(new SimpleSchemaRegistryAvroDeserializationSchema())
                .build();
    }
    
    /**
     * Create Kafka sink using the new KafkaSink API
     */
    private static KafkaSink<DuplicateAlert> createKafkaSink() {
        Properties producerConfig = new Properties();
        producerConfig.put("compression.type", "snappy");
        producerConfig.put("batch.size", 16384);
        producerConfig.put("linger.ms", 5);
        
        KafkaRecordSerializationSchema<DuplicateAlert> serializationSchema =
                KafkaRecordSerializationSchema.<DuplicateAlert>builder()
                        .setTopic(SINK_TOPIC)
                        .setValueSerializationSchema(new DuplicateAlertJsonSerializationSchema())
                        .build();
        
        return KafkaSink.<DuplicateAlert>builder()
                .setBootstrapServers(KAFKA_BOOTSTRAP_SERVERS)
                .setRecordSerializer(serializationSchema)
                .setKafkaProducerConfig(producerConfig)
                .build();
    }
}
