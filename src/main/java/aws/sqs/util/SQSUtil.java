package aws.sqs.util;

import com.amazonaws.services.lambda.runtime.events.SQSEvent;
import exception.AWSException;
import org.json.JSONException;
import org.json.JSONObject;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class SQSUtil {

    private static Logger LOGGER = LoggerFactory.getLogger(SQSUtil.class);
    private static String sourceFolderName = "uploaded";
    private static String destinationFolderName = "moved";

    public static String getDestinationKey(SQSEvent sqsEvent) throws AWSException{
        return destinationFolderName + "/" + getFileName(sqsEvent);
    }

    public static String getSourceKey(SQSEvent sqsEvent) throws AWSException{
        return sourceFolderName + "/" + getFileName(sqsEvent);
    }

    public static String getBucketName(SQSEvent sqsEvent) throws AWSException{
        return getMessageBody(sqsEvent).getString("bucketName");
    }

    public static String getFileName(SQSEvent sqsEvent) throws AWSException{
        return getMessageBody(sqsEvent).getString("fileName");
    }

    public static String getDate(SQSEvent sqsEvent) throws AWSException {
        return getMessageBody(sqsEvent).getString("date");
    }

    private static JSONObject getMessageBody(SQSEvent sqsEvent)  throws AWSException{
        try {
            String body = SQSUtil.getMessageFromSQS(sqsEvent);
            return new JSONObject(body);
        } catch (JSONException e) {
            LOGGER.error("Error while reading SQS JSON message: " + e.getMessage());
            throw new AWSException("Error while reading SQS JSON message: " + e.getMessage());
        }
    }
    private static String getMessageFromSQS(SQSEvent sqsEvent) throws AWSException {
        String body = "";
        try {
            for (SQSEvent.SQSMessage msg : sqsEvent.getRecords()) {
                body = msg.getBody();
            }
        }catch (Exception e) {
            LOGGER.error("Error while getting message body from SQS: " + e);
            throw new AWSException("Error while getting message body from SQS: " + e);
        }
        return body;
    }

}
