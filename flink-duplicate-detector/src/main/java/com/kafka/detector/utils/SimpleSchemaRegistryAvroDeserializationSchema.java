package com.kafka.detector.utils;

import com.kafka.detector.model.ReceiptData;
import org.apache.avro.Schema;
import org.apache.avro.generic.GenericDatumReader;
import org.apache.avro.generic.GenericRecord;
import org.apache.avro.io.DatumReader;
import org.apache.avro.io.Decoder;
import org.apache.avro.io.DecoderFactory;
import org.apache.flink.api.common.serialization.DeserializationSchema;
import org.apache.flink.api.common.typeinfo.TypeInformation;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.nio.ByteBuffer;
import java.util.ArrayList;
import java.util.List;

/**
 * Simple schema registry Avro deserialization
 */
public class SimpleSchemaRegistryAvroDeserializationSchema implements DeserializationSchema<ReceiptData> {
    
    private static final Logger logger = LoggerFactory.getLogger(SimpleSchemaRegistryAvroDeserializationSchema.class);
    
    // Magic byte and schema ID are at the beginning of Confluent Avro messages
    private static final byte MAGIC_BYTE = 0x0;
    private static final int SCHEMA_ID_SIZE = 4;
    
    private static final String SCHEMA_STRING = """
        {
          "type": "record",
          "name": "ReceiptData",
          "namespace": "com.example.test1.entity",
          "fields": [
            { "name": "franchise_id", "type": "int" },
            { "name": "store_brand", "type": "string" },
            { "name": "store_id", "type": "int" },
            { "name": "store_name", "type": "string" },
            { "name": "region", "type": "string" },
            { "name": "store_address", "type": "string" },
            {
              "name": "menu_items",
              "type": {
                "type": "array",
                "items": {
                  "type": "record",
                  "name": "MenuItem",
                  "fields": [
                    { "name": "menu_id", "type": "int" },
                    { "name": "menu_name", "type": "string" },
                    { "name": "unit_price", "type": "int" },
                    { "name": "quantity", "type": "int" }
                  ]
                }
              }
            },
            { "name": "total_price", "type": "int" },
            { "name": "user_id", "type": "int" },
            { "name": "time", "type": "string" },
            { "name": "user_name", "type": "string" },
            { "name": "user_gender", "type": "string" },
            { "name": "user_age", "type": "int" }
          ]
        }
        """;
    
    @Override
    public ReceiptData deserialize(byte[] message) {
        if (message == null || message.length == 0) {
            return null;
        }
        
        try {
            // Skip Confluent wire format bytes (1 magic byte + 4 schema ID bytes)
            if (message.length < 5 || message[0] != MAGIC_BYTE) {
                logger.error("Invalid Confluent Avro message format");
                return null;
            }
            
            // Skip the magic byte and schema ID
            ByteBuffer buffer = ByteBuffer.wrap(message, 1, 4);
            int schemaId = buffer.getInt();
            logger.debug("Received message with schema ID: {}", schemaId);
            
            // Decode actual Avro data (after the 5-byte header)
            Schema schema = new Schema.Parser().parse(SCHEMA_STRING);
            DatumReader<GenericRecord> reader = new GenericDatumReader<>(schema);
            Decoder decoder = DecoderFactory.get().binaryDecoder(message, 5, message.length - 5, null);
            GenericRecord record = reader.read(null, decoder);
            
            return convertToReceiptData(record);
            
        } catch (Exception e) {
            logger.error("Failed to deserialize Avro message", e);
            return null;
        }
    }
    
    private ReceiptData convertToReceiptData(GenericRecord record) {
        ReceiptData receiptData = new ReceiptData();
        
        receiptData.setFranchiseId((Integer) record.get("franchise_id"));
        receiptData.setStoreBrand(record.get("store_brand").toString());
        receiptData.setStoreId((Integer) record.get("store_id"));
        receiptData.setStoreName(record.get("store_name").toString());
        receiptData.setRegion(record.get("region").toString());
        receiptData.setStoreAddress(record.get("store_address").toString());
        
        // Handle menu items
        @SuppressWarnings("unchecked")
        List<GenericRecord> menuItemRecords = (List<GenericRecord>) record.get("menu_items");
        List<ReceiptData.MenuItem> menuItems = new ArrayList<>();
        
        if (menuItemRecords != null) {
            for (GenericRecord menuItemRecord : menuItemRecords) {
                ReceiptData.MenuItem menuItem = new ReceiptData.MenuItem();
                menuItem.setMenuId((Integer) menuItemRecord.get("menu_id"));
                menuItem.setMenuName(menuItemRecord.get("menu_name").toString());
                menuItem.setUnitPrice((Integer) menuItemRecord.get("unit_price"));
                menuItem.setQuantity((Integer) menuItemRecord.get("quantity"));
                menuItems.add(menuItem);
            }
        }
        
        receiptData.setMenuItems(menuItems);
        receiptData.setTotalPrice((Integer) record.get("total_price"));
        receiptData.setUserId((Integer) record.get("user_id"));
        receiptData.setTime(record.get("time").toString());
        receiptData.setUserName(record.get("user_name").toString());
        receiptData.setUserGender(record.get("user_gender").toString());
        receiptData.setUserAge((Integer) record.get("user_age"));
        
        return receiptData;
    }
    
    @Override
    public boolean isEndOfStream(ReceiptData nextElement) {
        return false;
    }
    
    @Override
    public TypeInformation<ReceiptData> getProducedType() {
        return TypeInformation.of(ReceiptData.class);
    }
}
