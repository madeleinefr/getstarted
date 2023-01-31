package org.example;

import io.undertow.Undertow;
import io.undertow.server.BlockingHttpExchange;
import io.undertow.server.HttpHandler;
import io.undertow.server.HttpServerExchange;

import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.io.TempDir;
import software.amazon.awssdk.http.urlconnection.UrlConnectionHttpClient;
import software.amazon.awssdk.services.s3.S3Client;
import software.amazon.awssdk.services.s3.model.GetObjectRequest;

import java.io.BufferedWriter;
import java.io.OutputStream;
import java.io.OutputStreamWriter;
import java.net.URI;
import java.nio.file.Files;
import java.nio.file.Path;

import static org.junit.jupiter.api.Assertions.assertEquals;


public class HandlerTest {


    public void setup(@TempDir Path tempDir) throws Exception {
        final int S3_MOCK_SERVER_PORT = 9999;

        Undertow s3Server = Undertow.builder()
                .addHttpListener(S3_MOCK_SERVER_PORT, "0.0.0.0")
                .setHandler(new S3HttpHandler())
                .build();

        s3Server.start();

        String serviceEndpoint = String.format("http://localhost:%d", S3_MOCK_SERVER_PORT);

        try (S3Client s3Client = S3Client.builder()
                .endpointOverride(URI.create(serviceEndpoint))
                .httpClient(UrlConnectionHttpClient.builder().build())
                .build()) {

            GetObjectRequest request = GetObjectRequest.builder()
                    .bucket("my-bucket")
                    .key("my-object")
                    .build();

            Path targetPath = tempDir.resolve("result");

            s3Client.getObject(request, targetPath);

            String objectContent = Files.readString(targetPath);
            System.out.printf("Object content: %s%n", objectContent);
            assertEquals("Hello world", objectContent);
        }

        s3Server.stop();
    }

    @Test
    void listObjectsOfS3Bucket() {

    }


    static class S3HttpHandler implements HttpHandler {

        @Override
        public void handleRequest(HttpServerExchange exchange) throws Exception {
            if (exchange.isInIoThread()) {
                exchange.dispatch(this);
                return;
            }

            System.out.printf("Request method: %s%n", exchange.getRequestMethod());
            System.out.printf("Request path: %s%n", exchange.getRequestPath());

            try (BlockingHttpExchange blockingExchange = exchange.startBlocking();
                 OutputStream outputStream = exchange.getOutputStream();
                 OutputStreamWriter outputStreamWriter = new OutputStreamWriter(outputStream);
                 BufferedWriter writer = new BufferedWriter(outputStreamWriter)) {

                writer.write("Hello world");
            }
        }

    }
}
