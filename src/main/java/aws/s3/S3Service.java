package aws.s3;

import aws.sqs.util.SQSUtil;
import com.amazonaws.services.lambda.runtime.events.SQSEvent;
import com.amazonaws.services.s3.AmazonS3;
import com.amazonaws.services.s3.AmazonS3ClientBuilder;
import com.amazonaws.services.s3.model.CopyObjectRequest;
import com.amazonaws.services.s3.model.DeleteObjectRequest;
import exception.AWSException;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class S3Service {

    private Logger LOGGER = LoggerFactory.getLogger(S3Service.class);

    private AmazonS3 s3Client = initS3Client();

    public void moveFile(SQSEvent sqsEvent) throws AWSException{
        if (sqsEvent == null) {
            LOGGER.error("Can't upload file to S3 Bucket - SQS Event is null");
            throw new AWSException("Can't upload file to S3 Bucket - SQS Event is null");
        }

        String bucketName = SQSUtil.getBucketName(sqsEvent);
        String sourceKey = SQSUtil.getSourceKey(sqsEvent);
        String destinationKey = SQSUtil.getDestinationKey(sqsEvent);

        try {
            LOGGER.debug("Copying file to S3Bucket...");
            CopyObjectRequest copyObjRequest = new CopyObjectRequest(bucketName, sourceKey, bucketName, destinationKey);
            s3Client.copyObject(copyObjRequest);
            LOGGER.info("File [{}] successfully copied to [moved] directory", sourceKey, bucketName);
        }catch (Exception e) {
            LOGGER.error("Can't copy file " + sourceKey + ": " + e.getMessage());
            throw new AWSException("Can't copy file " + sourceKey + ": " + e.getMessage());
        }

        try {
            LOGGER.debug("Deleting file from S3Bucket [{}]...", bucketName);
            s3Client.deleteObject(new DeleteObjectRequest(bucketName, sourceKey));
            LOGGER.info("File [{}] successfully deleted", sourceKey);
        }catch (Exception e) {
            LOGGER.error("Can't delete file " + sourceKey + ": " + e.getMessage());
            throw new AWSException("Can't delete file " + sourceKey + ": " + e.getMessage());
        }

        LOGGER.info("File successfully moved from [{}] to [{}]", sourceKey.split("/")[0], destinationKey.split("/")[0]);
    }

    private AmazonS3 initS3Client() {
        AmazonS3 amazonS3 = null;
        try {
            amazonS3 = AmazonS3ClientBuilder.defaultClient();
        }catch (Exception e) {
            LOGGER.error("Error occurred while initializing S3 Client: " + e.getMessage());
        }
        return amazonS3;
    }

}
