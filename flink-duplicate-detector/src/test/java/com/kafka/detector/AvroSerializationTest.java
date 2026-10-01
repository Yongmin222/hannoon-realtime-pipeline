package com.kafka.detector;

import com.kafka.detector.model.ReceiptData;
import org.apache.avro.Schema;
import org.apache.avro.generic.GenericData;
import org.apache.avro.generic.GenericRecord;
import org.apache.avro.io.DatumWriter;
import org.apache.avro.io.EncoderFactory;
import org.apache.avro.io.BinaryEncoder;
import org.apache.avro.generic.GenericDatumWriter;

import java.io.ByteArrayOutputStream;
import java.util.ArrayList;
import java.util.List;

public class AvroSerializationTest {
    
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
    
    public static void main(String[] args) throws Exception {
        Schema schema = new Schema.Parser().parse(SCHEMA_STRING);
        
        // Create test data
        GenericRecord record = new GenericData.Record(schema);
        record.put("franchise_id", 1);
        record.put("store_brand", "Brand A");
        record.put("store_id", 100);
        record.put("store_name", "Store 1");
        record.put("region", "Seoul");
        record.put("store_address", "123 Street");
        
        // Create menu items
        Schema menuItemSchema = schema.getField("menu_items").schema().getElementType();
        List<GenericRecord> menuItems = new ArrayList<>();
        
        GenericRecord menuItem = new GenericData.Record(menuItemSchema);
        menuItem.put("menu_id", 1);
        menuItem.put("menu_name", "Coffee");
        menuItem.put("unit_price", 5000);
        menuItem.put("quantity", 2);
        menuItems.add(menuItem);
        
        record.put("menu_items", menuItems);
        record.put("total_price", 10000);
        record.put("user_id", 123);
        record.put("time", "2024-01-01 10:00:00");
        record.put("user_name", "John Doe");
        record.put("user_gender", "M");
        record.put("user_age", 30);
        
        // Serialize
        ByteArrayOutputStream outputStream = new ByteArrayOutputStream();
        BinaryEncoder encoder = EncoderFactory.get().binaryEncoder(outputStream, null);
        DatumWriter<GenericRecord> writer = new GenericDatumWriter<>(schema);
        writer.write(record, encoder);
        encoder.flush();
        byte[] serializedData = outputStream.toByteArray();
        
        System.out.println("Serialized data length: " + serializedData.length);
        
        // Test deserialization
        ReceiptAvroDeserializationSchema deserializer = new ReceiptAvroDeserializationSchema();
        ReceiptData receipt = deserializer.deserialize(serializedData);
        
        if (receipt != null) {
            System.out.println("Deserialization successful!");
            System.out.println("User ID: " + receipt.getUserId());
            System.out.println("Total Price: " + receipt.getTotalPrice());
        } else {
            System.out.println("Deserialization failed!");
        }
    }
}
