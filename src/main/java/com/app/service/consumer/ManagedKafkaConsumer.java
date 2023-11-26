package com.app.service.consumer;

import com.app.service.MinioService;
import jakarta.annotation.PostConstruct;
import jakarta.annotation.PreDestroy;
import org.apache.kafka.clients.consumer.KafkaConsumer;
import org.apache.kafka.clients.consumer.ConsumerRecords;
import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.stereotype.Service;

import java.time.Duration;
import java.util.Collections;
import java.util.Properties;
import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.ObjectMapper;
import org.apache.avro.Schema;
import org.apache.avro.generic.GenericData;
import org.apache.avro.generic.GenericRecord;
import org.apache.hadoop.fs.Path;
import org.apache.parquet.avro.AvroParquetWriter;
import org.apache.parquet.hadoop.ParquetWriter;
import org.apache.parquet.hadoop.metadata.CompressionCodecName;
import java.io.File;
import java.io.IOException;
import java.io.InputStream;


@Service
public class ManagedKafkaConsumer {

    private KafkaConsumer<String, String> consumer;
    private final String topicName;
    private volatile boolean running = true;

    @Autowired
    private MinioService minioService;

    private Schema schema; // Avro schema

    @Value("${spring.kafka.bootstrap-servers}")
    private String bootstrapServers;

    public ManagedKafkaConsumer(@Value("${kafka.realtime.topic}") String topicName) {
        this.topicName = topicName;
        this.schema = parseSchema("avro/stockData.avsc"); // Load Avro schema from file
    }

    @PostConstruct
    public void start() {
        Properties props = new Properties();
        props.put("bootstrap.servers", bootstrapServers);
        props.put("group.id", "movie-consumer-group");
        props.put("key.deserializer", "org.apache.kafka.common.serialization.StringDeserializer");
        props.put("value.deserializer", "org.apache.kafka.common.serialization.StringDeserializer");

        this.consumer = new KafkaConsumer<>(props);
        this.consumer.subscribe(Collections.singletonList(topicName));

        Thread consumerThread = new Thread(this::runConsumer);
        consumerThread.start();
    }

    private void runConsumer() {
        try {
            while (running) {
                ConsumerRecords<String, String> records = consumer.poll(Duration.ofMillis(100));
                for (ConsumerRecord<String, String> record : records) {
                    ObjectMapper objectMapper = new ObjectMapper();
                    JsonNode rootNode = objectMapper.readTree(record.value());
                    JsonNode timeSeriesNode = rootNode.path("Time Series (60min)");

                    // Construct object name based on Kafka record metadata
                    String objectName = "stock-data-" + record.topic() + "-" + record.partition() + "-" + record.offset() + ".parquet";
                    File tempFile = new File("/tmp/" + objectName);

                    if (tempFile.exists()) {
                        tempFile.delete();
                    }

                    try (ParquetWriter<GenericRecord> writer = AvroParquetWriter.<GenericRecord>builder(new Path(tempFile.getAbsolutePath()))
                            .withSchema(schema)
                            .withCompressionCodec(CompressionCodecName.SNAPPY)
                            .build()) {
                        timeSeriesNode.fields().forEachRemaining(entry -> {
                            String date = entry.getKey();
                            JsonNode dataNode = entry.getValue();
                            GenericRecord avroRecord = new GenericData.Record(schema);
                            populateAvroRecord(avroRecord, date, dataNode);
                            try {
                                writer.write(avroRecord);
                            } catch (IOException e) {
                                throw new RuntimeException("Error writing to Parquet file", e);
                            }
                        });
                    } catch (IOException e) {
                        throw new RuntimeException("Error creating Parquet file", e);
                    }
                    System.out.println("consumed realtime message "+objectName);
                    // Upload the Parquet file to MinIO
                    minioService.uploadFile("windows-realtime-data", objectName, tempFile);
                }
            }
        } catch (IOException e) {
            e.printStackTrace();
        } finally {
            consumer.close();
        }
    }



    private void populateAvroRecord(GenericRecord avroRecord, String date, JsonNode dataNode) {
        avroRecord.put("date", date);
        avroRecord.put("open", dataNode.get("1. open").asText());
        avroRecord.put("high", dataNode.get("2. high").asText());
        avroRecord.put("low", dataNode.get("3. low").asText());
        avroRecord.put("close", dataNode.get("4. close").asText());
        avroRecord.put("volume", dataNode.get("5. volume").asText());
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

