package ru.element.lab;

import com.amazonaws.auth.AWSStaticCredentialsProvider;
import com.amazonaws.auth.BasicAWSCredentials;
import com.amazonaws.client.builder.AwsClientBuilder;
import com.amazonaws.services.s3.AmazonS3;
import com.amazonaws.services.s3.AmazonS3ClientBuilder;
import com.amazonaws.services.s3.model.S3Object;
import lombok.extern.slf4j.Slf4j;
import org.apache.commons.lang3.exception.ExceptionUtils;

import java.io.InputStream;
import java.io.Serializable;

/**
 * Сервис для работы с S3 файлохранилищем.
 */
@Slf4j
public class S3Service implements Serializable {
    private final AmazonS3 s3client;

    public S3Service(String endpoint, String accessKey, String secretKey) {
        s3client = AmazonS3ClientBuilder
                .standard()
                .withCredentials(new AWSStaticCredentialsProvider(new BasicAWSCredentials(accessKey, secretKey)))
                .withEndpointConfiguration(new AwsClientBuilder.EndpointConfiguration(endpoint, null))
                .withPathStyleAccessEnabled(true)
                .build();
    }

    public InputStream getFile(String bucket, String fileName) {
        try {
            final S3Object object = s3client.getObject(bucket, fileName);

            return object.getObjectContent();
        } catch (Exception e) {
            log.error(String.format("%s", ExceptionUtils.getStackTrace(e)));
            log.error("On get from S3: ", e);
            return null;
        }
    }
}
