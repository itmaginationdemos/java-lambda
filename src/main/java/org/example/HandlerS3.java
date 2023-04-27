package org.example;

import com.amazonaws.services.lambda.runtime.Context;
import com.amazonaws.services.lambda.runtime.LambdaLogger;
import com.amazonaws.services.lambda.runtime.RequestHandler;
import com.amazonaws.services.lambda.runtime.events.S3Event;
import com.amazonaws.services.lambda.runtime.events.models.s3.S3EventNotification;
import org.apache.pdfbox.pdmodel.PDDocument;
import org.apache.pdfbox.text.PDFTextStripper;
import software.amazon.awssdk.core.sync.RequestBody;
import software.amazon.awssdk.services.s3.S3Client;
import software.amazon.awssdk.services.s3.model.GetObjectRequest;
import software.amazon.awssdk.services.s3.model.PutObjectRequest;
import software.amazon.awssdk.services.s3.model.PutObjectResponse;

import java.io.IOException;
import java.io.InputStream;
import java.util.HashMap;

public class HandlerS3 implements RequestHandler<S3Event, String> {

    @Override
    public String handleRequest(S3Event event, Context context) {
        LambdaLogger logger = context.getLogger();
        logger.log("HandlerS3::handleRequest START ______________________________");
        S3EventNotification.S3EventNotificationRecord record = event.getRecords().get(0);
        if (record == null) {
            logger.log("Record not found in event. Exiting.");
            return null;
        }

        String srcBucket = record.getS3().getBucket().getName();
        String srcKey = record.getS3().getObject().getUrlDecodedKey();

        logger.log("CONTEXT: " + context);
        logger.log("EVENT: " + event);
        logger.log("RECORD: " + record);
        logger.log("BUCKET: " + srcBucket);
        logger.log("KEY: " + srcKey);

        logger.log("1. Get PDF from S3");
        S3Client s3Client = S3Client.builder().build();
        InputStream inputStream = getObject(s3Client, srcBucket, srcKey);

        logger.log("2. Convert PDF to text");
        String text;
        try {
            text = new PDFTextStripper().getText(PDDocument.load(inputStream));
            logger.log("PDF text: " + text);
        } catch (IOException e) {
            logger.log("Error: " + e.getMessage());
            throw new RuntimeException(e);
        }

        logger.log("3. Save converted text file on S3");
        String dstBucket = srcBucket;
        String dstKey = "output/converted-" + srcKey + ".txt";
        PutObjectResponse putObjectResponse = putS3Object(s3Client, dstBucket, dstKey, text);
        logger.log("putObjectResponse:" + putObjectResponse);

        logger.log("HandlerS3::handleRequest END ______________________________");
        return text;
    }

    private InputStream getObject(S3Client s3Client, String bucket, String key) {
        GetObjectRequest getObjectRequest = GetObjectRequest.builder()
                .bucket(bucket)
                .key(key)
                .build();
        return s3Client.getObject(getObjectRequest);
    }

    public static PutObjectResponse putS3Object(S3Client s3, String bucketName, String objectKey, String text) {
        PutObjectRequest putOb = PutObjectRequest.builder()
                .bucket(bucketName)
                .key(objectKey)
                .metadata(new HashMap<>())
                .build();

        PutObjectResponse response = s3.putObject(putOb, RequestBody.fromString(text));
        return response;
    }
}