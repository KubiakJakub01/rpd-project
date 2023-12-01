package com.app.service;

import io.minio.MinioClient;
import io.minio.PutObjectArgs;
import io.minio.errors.MinioException;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.stereotype.Service;

import java.io.*;
import java.nio.charset.StandardCharsets;
import java.security.InvalidKeyException;
import java.security.NoSuchAlgorithmException;

@Service
public class MinioService {

    private MinioClient minioClient;

    public MinioService(@Value("${minio.endpoint}") String endpoint,
                        @Value("${minio.access-key}") String accessKey,
                        @Value("${minio.secret-key}") String secretKey) {
        minioClient = MinioClient.builder()
                .endpoint(endpoint)
                .credentials(accessKey, secretKey)
                .build();
    }

    public void uploadString(String bucketName, String objectName, String content) {
        try {
            // Create a ByteArrayInputStream from the string content
            ByteArrayInputStream bais = new ByteArrayInputStream(content.getBytes(StandardCharsets.UTF_8));

            // Create PutObjectArgs object
            PutObjectArgs putObjectArgs = PutObjectArgs.builder()
                    .bucket(bucketName)
                    .object(objectName)
                    .stream(bais, bais.available(), -1) // -1 indicates unknown size (streaming)
                    .contentType("application/octet-stream")
                    .build();

            // Upload the ByteArrayInputStream to the bucket using PutObjectArgs
            minioClient.putObject(putObjectArgs);
        } catch (MinioException | InvalidKeyException | IOException | NoSuchAlgorithmException e) {
            System.out.println("Error occurred: " + e);
        }
    }

    public void uploadFile(String bucketName, String objectName, File file) {
        // Logic to upload a file to MinIO
        try (InputStream is = new FileInputStream(file)) {
            PutObjectArgs args = PutObjectArgs.builder()
                    .bucket(bucketName)
                    .object(objectName)
                    .stream(is, file.length(), -1)
                    .contentType("application/octet-stream")
                    .build();
            minioClient.putObject(args);
        } catch (Exception e) {
            throw new RuntimeException("Error uploading file to MinIO", e);
        }
    }


}

