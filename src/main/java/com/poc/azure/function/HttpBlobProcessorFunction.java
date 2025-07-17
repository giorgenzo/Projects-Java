package com.poc.azure.function;

import com.fasterxml.jackson.core.type.TypeReference;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.microsoft.azure.functions.*;
import com.microsoft.azure.functions.annotation.*;

import java.io.BufferedReader;
import java.io.InputStreamReader;
import java.net.HttpURLConnection;
import java.net.URL;
import java.nio.charset.StandardCharsets;
import java.util.*;

/**
 * Azure Function that processes a JSON file from Blob Storage on HTTP trigger.
 */
public class HttpBlobProcessorFunction {

    private static final String BLOB_URL = "https://pocstblob.blob.core.windows.net/poc-json-file/trigger-file/sample-trigger-file.json?sp=r&st=2025-07-09T10:39:20Z&se=2025-12-31T22:59:59Z&spr=https&sv=2024-11-04&sr=b&sig=hApfwPsJAGIXPeaVhZS7e%2F4ZN8ZJH18LnyT5aN1Rljo%3D";
    private static final String POST_URL = "https://webhook.site/50b2aa03-7eaa-4618-ad4c-8e7dc94d2f6c";

    @FunctionName("processBlobHttpTrigger")
    public HttpResponseMessage run(
        @HttpTrigger(
            name = "req",
            methods = {HttpMethod.GET, HttpMethod.POST},
            authLevel = AuthorizationLevel.ANONYMOUS)
        HttpRequestMessage<Optional<String>> request,
        final ExecutionContext context) {

        context.getLogger().info("HTTP trigger received a request to process blob JSON.");

        try {
            // 1. Read blob
            String json = readBlobJson(BLOB_URL);

            // 2. Execute trasformation
            List<Map<String, Object>> simplifiedData = transformJson(json);

            // 3. Send webhook
            sendToEndpoint(simplifiedData);

            return request
                .createResponseBuilder(HttpStatus.OK)
                .body("Blob JSON processed and sent successfully.")
                .build();

        } catch (Exception e) {
            context.getLogger().severe("Errore: " + e.getMessage());
            return request
                .createResponseBuilder(HttpStatus.INTERNAL_SERVER_ERROR)
                .body("Errore durante l’elaborazione: " + e.getMessage())
                .build();
        }
    }

    private String readBlobJson(String url) throws Exception {
        HttpURLConnection conn = (HttpURLConnection) new URL(url).openConnection();
        conn.setRequestMethod("GET");

        try (BufferedReader reader = new BufferedReader(
                new InputStreamReader(conn.getInputStream(), StandardCharsets.UTF_8))) {
            StringBuilder result = new StringBuilder();
            String line;
            while ((line = reader.readLine()) != null) {
                result.append(line);
            }
            return result.toString();
        }
    }

    private List<Map<String, Object>> transformJson(String json) throws Exception {
        ObjectMapper mapper = new ObjectMapper();

        List<Map<String, Object>> original = mapper.readValue(json, new TypeReference<>() {});
        return original.stream()
                .map(item -> Map.of(
                        "orderId", item.get("orderId"),
                        "customerId", item.get("customerId"),
                        "totalAmount", item.get("totalAmount"),
                        "status", item.get("status")
                ))
                .toList();
    }

    private void sendToEndpoint(List<Map<String, Object>> data) throws Exception {
        ObjectMapper mapper = new ObjectMapper();
        String json = mapper.writeValueAsString(data);

        HttpURLConnection conn = (HttpURLConnection) new URL(POST_URL).openConnection();
        conn.setRequestMethod("POST");
        conn.setRequestProperty("Content-Type", "application/json");
        conn.setDoOutput(true);

        try (var os = conn.getOutputStream()) {
            os.write(json.getBytes(StandardCharsets.UTF_8));
        }

        int responseCode = conn.getResponseCode();
        if (responseCode >= 400) {
            throw new RuntimeException("POST failed with code: " + responseCode);
        }
    }
}
