package org.example;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import software.amazon.awssdk.auth.credentials.ProfileCredentialsProvider;
import software.amazon.awssdk.core.sync.RequestBody;
import software.amazon.awssdk.core.waiters.WaiterResponse;
import software.amazon.awssdk.regions.Region;
import software.amazon.awssdk.services.s3.S3Client;
import software.amazon.awssdk.services.s3.model.*;
import software.amazon.awssdk.services.s3.waiters.S3Waiter;

import java.util.List;


public class Handler {
    private final S3Client s3Client;
    private static final Logger logger = LoggerFactory.getLogger(App.class);

    public Handler() {
        s3Client = DependencyFactory.s3Client();
    }

    public void sendRequest() {
        String bucket = "bucket" + System.currentTimeMillis();
        String key = "key";

        tutorialSetup(s3Client, bucket);

        logger.info("Uploading object...");

        // Put a single object in the bucket, in this case the key of the Bucket retreived from the PutObjectRequest
        s3Client.putObject(PutObjectRequest.builder().bucket(bucket).key(key)
                        .build(),
                RequestBody.fromString("Testing with the {sdk-java}"));

        logger.info("Upload complete");
        logger.info(" ");

        listBuckets(s3Client);
        logger.info("Listed the buckets successfully");
        listBucketObjects(s3Client, bucket);
        logger.info("Listed the objects and their owners of the created bucket successfully");

        cleanUp(s3Client, bucket, key);


        logger.info("Closing the connection to {S3}");
        s3Client.close();
        logger.info("Connection closed");
        logger.info("Exiting...");
    }

    public static void createS3Client(S3Client s3Client) {

        ProfileCredentialsProvider credentialsProvider = ProfileCredentialsProvider.create();
        Region region = Region.US_EAST_1;

        s3Client = S3Client.builder()
                .region(region)
                .credentialsProvider(credentialsProvider)
                .build();

    }

    public static void tutorialSetup(S3Client s3Client, String bucketName) {
        try {
            s3Client.createBucket(CreateBucketRequest
                    .builder()
                    .bucket(bucketName)
                    .build());
            logger.info("Creating bucket: " + bucketName);
            s3Client.waiter().waitUntilBucketExists(HeadBucketRequest.builder()
                    .bucket(bucketName)
                    .build());
            logger.info(bucketName + " is ready.");
            logger.info(" ");
        } catch (S3Exception e) {
            logger.info(e.awsErrorDetails().errorMessage());
            System.exit(1);
        }
    }

    // Create a bucket by using a S3Waiter object
    public static void createBucket(S3Client s3Client, String bucketName) {

        try {
            S3Waiter s3Waiter = s3Client.waiter();
            CreateBucketRequest bucketRequest = CreateBucketRequest.builder()
                    .bucket(bucketName)
                    .build();

            s3Client.createBucket(bucketRequest);
            HeadBucketRequest bucketRequestWait = HeadBucketRequest.builder()
                    .bucket(bucketName)
                    .build();

            // Wait until the bucket is created and print out the response.
            WaiterResponse<HeadBucketResponse> waiterResponse = s3Waiter.waitUntilBucketExists(bucketRequestWait);
            waiterResponse.matched().response().ifPresent(System.out::println);
            System.out.println(bucketName + " is ready");

        } catch (S3Exception e) {
            System.err.println(e.awsErrorDetails().errorMessage());
            System.exit(1);
        }
    }

    // snippet-start:[s3.java2.s3_bucket_deletion.delete_objects]
    public static void deleteObjectsInBucket(S3Client s3, String bucket) {

        try {
            // To delete a bucket, all the objects in the bucket must be deleted first.
            ListObjectsV2Request listObjectsV2Request = ListObjectsV2Request.builder()
                    .bucket(bucket)
                    .build();
            ListObjectsV2Response listObjectsV2Response;

            do {
                listObjectsV2Response = s3.listObjectsV2(listObjectsV2Request);
                for (S3Object s3Object : listObjectsV2Response.contents()) {
                    DeleteObjectRequest request = DeleteObjectRequest.builder()
                            .bucket(bucket)
                            .key(s3Object.key())
                            .build();
                    s3.deleteObject(request);
                }
            } while (listObjectsV2Response.isTruncated());
            // snippet-end:[s3.java2.s3_bucket_deletion.delete_objects]

            DeleteBucketRequest deleteBucketRequest = DeleteBucketRequest.builder().bucket(bucket).build();
            s3.deleteBucket(deleteBucketRequest);

        } catch (S3Exception e) {
            System.err.println(e.awsErrorDetails().errorMessage());
            System.exit(1);
        }
    }
    // snippet-end:[s3.java2.bucket_deletion.main]


    //Deletes the created bucket and objects from the aws account
    public static void cleanUp(S3Client s3Client, String bucketName, String keyName) {
        logger.info("Cleaning up...");
        try {
            logger.info("Deleting object: " + keyName);
            DeleteObjectRequest deleteObjectRequest = DeleteObjectRequest.builder().bucket(bucketName).key(keyName).build();
            s3Client.deleteObject(deleteObjectRequest);
            logger.info(keyName + " has been deleted.");
            logger.info("Deleting bucket: " + bucketName);
            DeleteBucketRequest deleteBucketRequest = DeleteBucketRequest.builder().bucket(bucketName).build();
            s3Client.deleteBucket(deleteBucketRequest);
            logger.info(bucketName + " has been deleted.");
            logger.info(" ");
        } catch (S3Exception e) {
            System.err.println(e.awsErrorDetails().errorMessage());
            System.exit(1);
        }
        logger.info("Cleanup complete");
        logger.info(" ");

    }

    /*
    Suggested feature: lists the objects of a bucket and their owner
     */
    public static void listBucketObjects(S3Client s3Client, String bucketName) {
        try {
            ListObjectsRequest listObjects = ListObjectsRequest
                    .builder()
                    .bucket(bucketName)
                    .build();


            ListObjectsResponse res = s3Client.listObjects(listObjects);
            List<S3Object> objects = res.contents();
            for (S3Object myValue : objects) {
                System.out.print("\n The name of the object is " + myValue.key());
                System.out.print("\n The owner is " + myValue.owner());
            }
        } catch (S3Exception exception) {
            System.err.println(exception.awsErrorDetails().errorMessage());
            System.exit(1);
        }

    }

    /*
    Another suggested feauture:
    Listing all the buckets of this account
     */
    public static void listBuckets(S3Client s3Client) {

        try {
            ListBucketsRequest listBucketsRequest = ListBucketsRequest
                    .builder()
                    .build();
            ListBucketsResponse listBucketsResponse = s3Client.listBuckets(listBucketsRequest);
            listBucketsResponse.buckets().stream().forEach(x -> System.out.println("The name of the bucket is " + x.name()));
        } catch (S3Exception exception) {
            System.err.println(exception.awsErrorDetails().errorMessage());
            System.exit(1);
        }
    }
}



