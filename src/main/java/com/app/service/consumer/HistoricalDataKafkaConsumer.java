package com.app.service.consumer;

import com.app.service.MinioService;
import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.ObjectMapper;
import io.minio.PutObjectArgs;
import jakarta.annotation.PostConstruct;
import jakarta.annotation.PreDestroy;
import org.apache.avro.Schema;
import org.apache.avro.generic.GenericData;
import org.apache.avro.generic.GenericRecord;
import org.apache.avro.generic.GenericRecordBuilder;
import org.apache.hadoop.fs.Path;
import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.kafka.clients.consumer.ConsumerRecords;
import org.apache.kafka.clients.consumer.KafkaConsumer;
import org.apache.parquet.avro.AvroParquetWriter;
import org.apache.parquet.hadoop.ParquetWriter;
import org.apache.parquet.hadoop.metadata.CompressionCodecName;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.stereotype.Service;

import java.io.File;
import java.io.FileInputStream;
import java.io.IOException;
import java.io.InputStream;
import java.nio.file.Files;
import java.nio.file.Paths;
import java.time.Duration;
import java.util.Collections;
import java.util.Properties;
import java.util.UUID;

@Service
public class HistoricalDataKafkaConsumer {

    private KafkaConsumer<String, String> consumer;
    private final String historicalTopicName;
    private volatile boolean running = true;

    private Schema schema; // Avro schema

    @Autowired
    private MinioService minioService;

    public HistoricalDataKafkaConsumer(@Value("${kafka.historical.topic}") String historicalTopicName) {
        this.historicalTopicName = historicalTopicName;
        this.schema = parseSchema("avro/csvStockData.avsc"); // Load Avro schema from file
    }

    @Value("${spring.kafka.bootstrap-servers}")
    private String bootstrapServers;

    @PostConstruct
    public void start() {
        Properties props = new Properties();
        props.put("bootstrap.servers", bootstrapServers);
        props.put("group.id", "csv-consumer-group");
        props.put("key.deserializer", "org.apache.kafka.common.serialization.StringDeserializer");
        props.put("value.deserializer", "org.apache.kafka.common.serialization.StringDeserializer");

        this.consumer = new KafkaConsumer<>(props);
        this.consumer.subscribe(Collections.singletonList(historicalTopicName));

        Thread consumerThread = new Thread(this::runConsumer);
        consumerThread.start();
    }

    private void runConsumer() {
        try {
            while (running) {
                ConsumerRecords<String, String> records = consumer.poll(Duration.ofMillis(100));
                for (ConsumerRecord<String, String> record : records) {
                    String objectName = "historical-data-" + record.topic() + "-" + record.partition() + "-" + record.offset() + ".parquet";
                    File tempFile = new File("/tmp/" + objectName);

                    // Check if the file already exists in the local file system and delete it if necessary
                    if (tempFile.exists()) {
                        tempFile.delete();
                    }

                    try (ParquetWriter<GenericRecord> writer = AvroParquetWriter.<GenericRecord>builder(new Path(tempFile.getAbsolutePath()))
                            .withSchema(schema)
                            .withCompressionCodec(CompressionCodecName.SNAPPY)
                            .build()) {
                        GenericRecord avroRecord = new GenericData.Record(schema);
                        populateAvroRecord(avroRecord, record);
                        writer.write(avroRecord);
                    }
                    System.out.println("consumed csv message "+objectName);
                    // Upload the Parquet file to MinIO
                    minioService.uploadFile("windows-csv-data", objectName, tempFile);
                }
            }
        } catch (IOException e) {
            e.printStackTrace();
        } finally {
            consumer.close();
        }
    }




    private void populateAvroRecord(GenericRecord avroRecord, ConsumerRecord<String, String> record) {
        try {
            ObjectMapper objectMapper = new ObjectMapper();
            JsonNode jsonNode = objectMapper.readTree(record.value());

            avroRecord.put("volume", jsonNode.get("volume").asText());
            avroRecord.put("high", jsonNode.get("high").asText());
            avroRecord.put("low", jsonNode.get("low").asText());
            avroRecord.put("close", jsonNode.get("close").asText());
            avroRecord.put("open", jsonNode.get("open").asText());
            avroRecord.put("timestamp", jsonNode.get("timestamp").asText());

        } catch (Exception e) {
            throw new RuntimeException("Error parsing JSON from Kafka record", e);
        }
    }


    private Schema parseSchema(String name) {
        try (InputStream schemaStream = getClass().getClassLoader().getResourceAsStream(name)) {
            return new Schema.Parser().parse(schemaStream);
        } catch (IOException e) {
            throw new RuntimeException("Failed to parse Avro schema file.", e);
        }
    }


    @PreDestroy
    public void shutdown() {
        running = false;
    }
}

