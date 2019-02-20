package aws.dynamo;

import aws.sqs.util.SQSUtil;
import com.amazonaws.regions.Regions;
import com.amazonaws.services.dynamodbv2.AmazonDynamoDB;
import com.amazonaws.services.dynamodbv2.AmazonDynamoDBClientBuilder;
import com.amazonaws.services.dynamodbv2.document.DynamoDB;
import com.amazonaws.services.dynamodbv2.document.Table;
import com.amazonaws.services.dynamodbv2.document.UpdateItemOutcome;
import com.amazonaws.services.dynamodbv2.document.spec.UpdateItemSpec;
import com.amazonaws.services.dynamodbv2.document.utils.ValueMap;
import com.amazonaws.services.dynamodbv2.model.ReturnValue;
import com.amazonaws.services.lambda.runtime.events.SQSEvent;
import exception.AWSException;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class DynamoService {

    private Logger LOGGER = LoggerFactory.getLogger(DynamoService.class);

    private String STATUS = "COPIED";
    private Regions REGION = Regions.EU_WEST_1;

    private DynamoDB dynamoDB = initDynamoDbClient();

    public void updateStatusInDynamoDB(SQSEvent sqsEvent) throws AWSException{
        if (sqsEvent == null) {
            LOGGER.error("Error updating filed in DynamoDB - SQS Event is null");
        }
        
        Table table;
        UpdateItemSpec updateItemSpec;
        
        try {
        table = dynamoDB.getTable(System.getenv("DYNAMODB_TABLE_NAME"));

        updateItemSpec = new UpdateItemSpec().withReturnValues(ReturnValue.ALL_NEW)
                .withPrimaryKey("fileName", SQSUtil.getFileName(sqsEvent), "date", SQSUtil.getDate(sqsEvent))
                .withUpdateExpression("set file_status = :file_status")
                .withValueMap(new ValueMap()
                        .withString(":file_status", STATUS));
        
            LOGGER.debug("Updating the item...");
            UpdateItemOutcome outcome = table.updateItem(updateItemSpec);
            LOGGER.info("UpdateItem succeeded:\n" + outcome.getItem().toJSONPretty());
        }
        catch (Exception e) {
            LOGGER.error("Unable to update item [{}]", SQSUtil.getFileName(sqsEvent));
            LOGGER.error(e.getMessage());
        }
    }

    private DynamoDB initDynamoDbClient() {
        DynamoDB dynamoDB = null;
        try {
            AmazonDynamoDB client = AmazonDynamoDBClientBuilder.standard().withRegion(REGION).build();
            dynamoDB = new DynamoDB(client);
        } catch (Exception e) {
            LOGGER.error("Error while initializing DynamoDBClient: " + e.getMessage());
        }
        return dynamoDB;
    }
}
