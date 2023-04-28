package org.example.itm;

import com.amazonaws.services.lambda.runtime.Context;
import com.amazonaws.services.lambda.runtime.LambdaLogger;
import com.amazonaws.services.lambda.runtime.RequestHandler;
import com.amazonaws.services.lambda.runtime.events.S3Event;
import com.amazonaws.services.lambda.runtime.events.models.s3.S3EventNotification;
import org.apache.pdfbox.pdmodel.PDDocument;
import org.apache.pdfbox.text.PDFTextStripper;
import software.amazon.awssdk.auth.credentials.EnvironmentVariableCredentialsProvider;
import software.amazon.awssdk.core.SdkSystemSetting;
import software.amazon.awssdk.core.sync.RequestBody;
import software.amazon.awssdk.regions.Region;
import software.amazon.awssdk.services.s3.S3Client;
import software.amazon.awssdk.services.s3.model.GetObjectRequest;
import software.amazon.awssdk.services.s3.model.PutObjectRequest;
import software.amazon.awssdk.services.s3.model.PutObjectResponse;

import java.io.IOException;
import java.io.InputStream;
import java.util.HashMap;
import java.util.Optional;

public class HandlerS3 implements RequestHandler<S3Event, String> {


    private static S3Client createS3Client() {
        return S3Client.builder()
                .region(Region.of(System.getenv(SdkSystemSetting.AWS_REGION.environmentVariable())))
                .credentialsProvider(EnvironmentVariableCredentialsProvider.create())
                .build();
    }

    public static PutObjectResponse putS3Object(S3Client s3, String bucketName, String objectKey, String text) {

        PutObjectRequest putOb = PutObjectRequest.builder()
                .bucket(bucketName)
                .key(objectKey)
                .metadata(new HashMap<>())
                .build();

        return s3.putObject(putOb, RequestBody.fromString(text));
    }

    @Override
    public String handleRequest(S3Event event, Context context) {
        LambdaLogger logger = context.getLogger();
        logger.log("HandlerS3::handleRequest START ______________________________");
        Optional<S3EventNotification.S3EventNotificationRecord> eventRecordOpt = event.getRecords().stream().findFirst();
        if (eventRecordOpt.isEmpty()) {
            logger.log("Record not found in event. Exiting.");
            return null;
        }
        S3EventNotification.S3EventNotificationRecord eventRecord = eventRecordOpt.get();
        String bucketName = eventRecord.getS3().getBucket().getName();
        String srcKey = eventRecord.getS3().getObject().getUrlDecodedKey();

        logVariables(logger, event, context, eventRecord, bucketName, srcKey);

        S3Client s3Client = createS3Client();

        logger.log("2. Convert PDF to text");
        String text = getPdfAsText(logger, bucketName, srcKey, s3Client);

        logger.log("3. Save converted text file on S3");
        String dstKey = "output/converted-" + srcKey + ".txt";
        PutObjectResponse putObjectResponse = putS3Object(s3Client, bucketName, dstKey, text);
        logger.log("putObjectResponse:" + putObjectResponse);

        logger.log("HandlerS3::handleRequest END ______________________________");
        return text;
    }

    private void logVariables(LambdaLogger logger, S3Event event, Context context, S3EventNotification.S3EventNotificationRecord eventRecord, String bucketName, String srcKey) {
        logger.log("CONTEXT: " + context);
        logger.log("EVENT: " + event);
        logger.log("RECORD: " + eventRecord);
        logger.log("BUCKET: " + bucketName);
        logger.log("KEY: " + srcKey);
    }

    private String getPdfAsText(LambdaLogger logger, String bucketName, String srcKey, S3Client s3Client) {
        try (InputStream inputStream = getObject(s3Client, bucketName, srcKey)) {
            PDDocument pdf = PDDocument.load(inputStream);
            if (pdf.isEncrypted()) {
                String errorMsg = String.format("The %s/%s file is encrypted or not a valid PDF", bucketName, srcKey);
                logger.log(errorMsg);
                throw new IllegalArgumentException(errorMsg);
            }
            String text = new PDFTextStripper().getText(pdf);
            logger.log("PDF text: " + text);
            return text;
        } catch (IOException e) {
            logger.log("Error: " + e.getMessage());
            throw new IllegalStateException(e);
        }
    }

    private InputStream getObject(S3Client s3Client, String bucket, String key) {
        GetObjectRequest getObjectRequest = GetObjectRequest.builder()
                .bucket(bucket)
                .key(key)
                .build();
        return s3Client.getObject(getObjectRequest);
    }
}