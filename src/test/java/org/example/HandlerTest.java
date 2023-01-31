package org.example;

import io.undertow.Undertow;
import io.undertow.server.BlockingHttpExchange;
import io.undertow.server.HttpHandler;
import io.undertow.server.HttpServerExchange;

import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.io.TempDir;
import software.amazon.awssdk.http.urlconnection.UrlConnectionHttpClient;
import com.amazonaws.services.s3.AmazonS3;
import com.amazonaws.services.s3.AmazonS3ClientBuilder;
import com.amazonaws.services.s3.model.AmazonS3Exception;
import com.amazonaws.services.s3.model.Bucket;
import software.amazon.awssdk.services.s3.S3Client;
import software.amazon.awssdk.services.s3.model.*;

import java.io.BufferedWriter;
import java.io.OutputStream;
import java.io.OutputStreamWriter;
import java.net.URI;
import java.nio.file.Files;
import java.nio.file.Path;
import java.util.List;

import static org.junit.jupiter.api.Assertions.assertEquals;


public class HandlerTest {
    Handler handler;

    S3Client s3Client;

    @BeforeAll
    public void setup(@TempDir Path tempDir) throws Exception {
        final int S3_MOCK_SERVER_PORT = 9999;

        Undertow s3Server = Undertow.builder()
                .addHttpListener(S3_MOCK_SERVER_PORT, "0.0.0.0")
                .setHandler(new S3HttpHandler())
                .build();

        s3Server.start();

        String serviceEndpoint = String.format("http://localhost:%d", S3_MOCK_SERVER_PORT);

        S3Client s3Client = S3Client.builder()
                .endpointOverride(URI.create(serviceEndpoint))
                .httpClient(UrlConnectionHttpClient.builder().build())
                .build();

        //s3Server.stop();
    }

    @Test
    void setupS3BucketForTutorial(){
        Handler.setupS3BucketForTutorial(s3Client,"bucket08101919" );

        HeadBucketRequest headBucketRequest = HeadBucketRequest.builder()
                .bucket("bucket08101919")
                .build();

        assertEquals(true, s3Client.headBucket(headBucketRequest));
    }
    @Test
    void createS3BucketUsingS3Waiter() {
        Handler.createS3BucketUsingS3Waiter(s3Client, "bucket08101919");

        HeadBucketRequest headBucketRequest = HeadBucketRequest.builder()
                .bucket("bucket08101919")
                .build();

        assertEquals(true, s3Client.headBucket(headBucketRequest));
    }


    /*@Test
    static void listObjectsOfS3Bucket() {
        //given(welche testvoraussetzung habe ich)
        ListObjectsRequest listObjects = ListObjectsRequest
                .builder()
                .bucket("bucketname")
                .build();

        Handler.listObjectsOfS3Bucket();

        ListObjectsResponse res = s3Client.listObjects(listObjects);
        List<S3Object> objects = res.contents();
        for (S3Object myValue : objects) {
            logger.info("\n The name of the object is " + myValue.key());
            logger.info("\n The owner is " + myValue.owner());

    }
*/

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

