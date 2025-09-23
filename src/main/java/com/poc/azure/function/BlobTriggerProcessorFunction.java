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

public class BlobTriggerProcessorFunction {

    private static final String POST_URL = System.getenv("POST_URL");
    private static final String AZURE_CONNECTION_STRING = System.getenv("AZURE_STORAGE_CONNECTION_STRING");

    @FunctionName("processBlobTrigger")
    public void run(
            @BlobTrigger(name = "inputBlob", path = "%BLOB_CONTAINER_NAME%/{name}", dataType = "binary", connection = "AZURE_STORAGE_CONNECTION_STRING") byte[] content,
            @BindingName("name") String blobName,
            final ExecutionContext context) {

        context.getLogger().info("Blob trigger received: " + blobName);

        try {
            if (content == null || content.length == 0) {
                context.getLogger().info("Blob is empty, execution skipped.");
                return;
            }

            // Step 1: Convert blob content to string
            String json = new String(content, StandardCharsets.UTF_8);

            // Step 2: Transform
            List<Map<String, Object>> simplifiedData = transformJson(json, context);

            // Step 3: Send to endpoint
            sendToEndpoint(simplifiedData, context);

            // Step 4: Delete blob
            deleteBlob(blobName, context);

            context.getLogger().info("Data successfully sent and blob deleted.");

        } catch (Exception e) {
            context.getLogger().severe("Error: " + e.getMessage());
        }
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

    private void deleteBlob(String blobName, ExecutionContext context) {

        context.getLogger().info("Deleting blob");
        
        BlobClient blobClient = new BlobClientBuilder()
                .connectionString(AZURE_CONNECTION_STRING)
                .containerName(System.getenv("BLOB_CONTAINER_NAME"))
                .blobName(blobName)
                .buildClient();

        if (blobClient.exists()) {
            blobClient.delete();
            context.getLogger().info("Blob deleted successfully.");
        } else {
            context.getLogger().warning("Blob not found, skipping delete.");
        }
    }
}
