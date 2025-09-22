package com.poc.azure.function;

import com.fasterxml.jackson.core.type.TypeReference;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.microsoft.azure.functions.*;
import com.microsoft.azure.functions.annotation.*;
import com.azure.storage.blob.*;

import java.io.*;
import java.net.HttpURLConnection;
import java.net.URL;
import java.nio.charset.StandardCharsets;
import java.util.*;

public class TimerTriggerProcessorFunction {

    private static final String POST_URL = System.getenv("POST_URL");
    private static final String AZURE_CONNECTION_STRING = System.getenv("AZURE_STORAGE_CONNECTION_STRING");
    private static final String CONTAINER_NAME = System.getenv("TIMER_BLOB_CONTAINER_NAME");
    private static final String BLOB_NAME = System.getenv("BLOB_NAME");

    @FunctionName("processTimerTrigger")
    public void run(
            @TimerTrigger(name = "timerInfo", schedule = "0 */1 * * * *") String timerInfo,
            final ExecutionContext context) {

        context.getLogger().info("HTTP trigger received a request to process blob JSON.");

        try {
            // Step 1: Read the JSON from Blob
            String json = readBlobJsonFromSdk(context);

            if (json == null || json.trim().isEmpty() || json.trim().equals("[]")) {
                context.getLogger().info("Blob is empty, execution skipped.");
                return;
            }

            // Step 2: Transform
            List<Map<String, Object>> simplifiedData = transformJson(json, context);

            // Step 3: Send to endpoint
            sendToEndpoint(simplifiedData, context);

            // Step 4: Delete the blob
            deleteBlobUsingSdk(context);

            context.getLogger().info("Data successfully sent and blob deleted.");
            return;

        } catch (Exception e) {
            context.getLogger().severe("Errore durante l’elaborazione: " + e.getMessage());
            return;
        }
    }

    private String readBlobJsonFromSdk(ExecutionContext context) throws IOException {

        context.getLogger().info("Reading JSON from Blob using Azure SDK...");

        BlobClient blobClient = new BlobClientBuilder()
                .connectionString(AZURE_CONNECTION_STRING)
                .containerName(CONTAINER_NAME)
                .blobName(BLOB_NAME)
                .buildClient();

        if (!blobClient.exists()) {
            throw new FileNotFoundException("Blob not found: " + BLOB_NAME);
        }

        ByteArrayOutputStream outputStream = new ByteArrayOutputStream();
        blobClient.downloadStream(outputStream);

        return outputStream.toString(StandardCharsets.UTF_8);
    }

    private List<Map<String, Object>> transformJson(String json, ExecutionContext context) throws IOException {

        context.getLogger().info("Transforming JSON...");

        ObjectMapper mapper = new ObjectMapper();

        List<Map<String, Object>> original = mapper.readValue(json, new TypeReference<>() {
        });
        return original.stream()
                .map(item -> Map.of(
                        "orderId", item.get("orderId"),
                        "customerId", item.get("customerId"),
                        "totalAmount", item.get("totalAmount"),
                        "status", item.get("status")))
                .toList();
    }

    private void sendToEndpoint(List<Map<String, Object>> data, ExecutionContext context) throws IOException {

        context.getLogger().info("Sending data to webhook...");

        ObjectMapper mapper = new ObjectMapper();
        String json = mapper.writeValueAsString(data);

        HttpURLConnection conn = (HttpURLConnection) new URL(POST_URL).openConnection();
        conn.setRequestMethod("POST");
        conn.setConnectTimeout(5000);
        conn.setReadTimeout(5000);
        conn.setRequestProperty("Content-Type", "application/json");
        conn.setDoOutput(true);

        try (OutputStream os = conn.getOutputStream()) {
            os.write(json.getBytes(StandardCharsets.UTF_8));
        }

        int responseCode = conn.getResponseCode();
        if (responseCode >= 400) {
            throw new IOException("POST failed with HTTP code: " + responseCode);
        }

        context.getLogger().info("Webhook POST completed successfully.");
    }

    private void deleteBlobUsingSdk(ExecutionContext context) {

        context.getLogger().info("Deleting blob using Azure SDK...");

        BlobClient blobClient = new BlobClientBuilder()
                .connectionString(AZURE_CONNECTION_STRING)
                .containerName(CONTAINER_NAME)
                .blobName(BLOB_NAME)
                .buildClient();

        if (blobClient.exists()) {
            blobClient.delete();
            context.getLogger().info("Blob deleted successfully.");
        } else {
            context.getLogger().warning("Blob not found, skipping delete.");
        }
    }
}
