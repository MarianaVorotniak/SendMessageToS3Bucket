import com.amazonaws.services.lambda.runtime.Context;
import com.amazonaws.services.lambda.runtime.RequestHandler;
import com.amazonaws.services.lambda.runtime.events.SQSEvent;
import exception.AWSException;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import aws.dynamo.DynamoService;
import aws.s3.S3Service;

public class Main implements RequestHandler<SQSEvent, String> {

    private static Logger LOGGER = LoggerFactory.getLogger(Main.class);

    private S3Service s3Service = new S3Service();

    private DynamoService dynamoService = new DynamoService();

    public String handleRequest(SQSEvent sqsEvent, Context context) {
        LOGGER.info("Event received: " + sqsEvent.toString());
        try {
            s3Service.moveFile(sqsEvent);
            dynamoService.updateStatusInDynamoDB(sqsEvent);

            LOGGER.info("Success");
            return "Success!";
        } catch (AWSException e) {
            return e.getMessage();
        }
    }

}
