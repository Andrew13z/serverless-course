package com.task06;

import com.amazonaws.regions.Regions;
import com.amazonaws.services.dynamodbv2.AmazonDynamoDB;
import com.amazonaws.services.dynamodbv2.AmazonDynamoDBClientBuilder;
import com.amazonaws.services.lambda.runtime.Context;
import com.amazonaws.services.lambda.runtime.RequestHandler;
import com.amazonaws.services.lambda.runtime.events.DynamodbEvent;
import com.amazonaws.services.lambda.runtime.events.models.dynamodb.AttributeValue;
import com.syndicate.deployment.annotations.environment.EnvironmentVariable;
import com.syndicate.deployment.annotations.environment.EnvironmentVariables;
import com.syndicate.deployment.annotations.events.DynamoDbTriggerEventSource;
import com.syndicate.deployment.annotations.lambda.LambdaHandler;

import java.time.ZoneId;
import java.time.format.DateTimeFormatter;
import java.util.LinkedHashMap;
import java.util.Map;
import java.util.UUID;

import static com.amazonaws.services.dynamodbv2.document.ItemUtils.fromSimpleMap;

@LambdaHandler(lambdaName = "audit_producer",
		roleName = "audit_producer-role",
		isPublishVersion = true,
		aliasName = "${lambdas_alias_name}"
)
@DynamoDbTriggerEventSource(targetTable = "cmtr-9909348c-ConfigurationStream-test", batchSize = 1)
@EnvironmentVariables(value = {
		@EnvironmentVariable(key = "target_table", value = "${target_table}"),
})
public class AuditProducer implements RequestHandler<DynamodbEvent, Void> {

	private AmazonDynamoDB amazonDynamoDB;


	@Override
	public Void handleRequest(DynamodbEvent event, Context context) {
		initDynamoDbClient();
		DynamodbEvent.DynamodbStreamRecord record = event.getRecords().get(0);
		if ("INSERT".equals(record.getEventName())) {
			processCreateItem(record, context);
		}
		if ("MODIFY".equals(record.getEventName())) {
			processUpdateItem(record, context);
		}
		return null;
	}

	private void processCreateItem(DynamodbEvent.DynamodbStreamRecord record, Context context) {
		Map<String, AttributeValue> newImage = record.getDynamodb().getNewImage();
		context.getLogger().log("Processing new item");

		String key = newImage.get("key").getS();
		String value = newImage.get("value").getS();
		String modificationTime =
				record.getDynamodb()
						.getApproximateCreationDateTime()
						.toInstant()
						.atZone(ZoneId.systemDefault())
						.toLocalDateTime()
						.format(DateTimeFormatter.ofPattern("yyyy-MM-dd'T'HH:mm:ss.SSS'Z'"));

		Map<String, Object> newValue = new LinkedHashMap<>();
		newValue.put("key", key);
		newValue.put("value", value);

		context.getLogger().log("Saving new item with values: key = " + key + ", value = " + value);

		Map<String, com.amazonaws.services.dynamodbv2.model.AttributeValue> auditItem = new LinkedHashMap<>();
		auditItem.put("id",
					  new com.amazonaws.services.dynamodbv2.model.AttributeValue(UUID.randomUUID().toString()));
		auditItem.put("itemKey", new com.amazonaws.services.dynamodbv2.model.AttributeValue(key));
		auditItem.put("modificationTime",
					  new com.amazonaws.services.dynamodbv2.model.AttributeValue(modificationTime));
		auditItem.put("newValue", new com.amazonaws.services.dynamodbv2.model.AttributeValue().withM(
				fromSimpleMap(newValue)));

		amazonDynamoDB.putItem(System.getenv("target_table"), auditItem);
	}

	private void processUpdateItem(DynamodbEvent.DynamodbStreamRecord record, Context context) {
		Map<String, AttributeValue> oldImage = record.getDynamodb().getOldImage();
		Map<String, AttributeValue> newImage = record.getDynamodb().getNewImage();
		context.getLogger().log("Processing updated item");

		String key = newImage.get("key").getS();
		String oldValue = oldImage.get("value").getS();
		String newValue = newImage.get("value").getS();
		String modificationTime =
				record.getDynamodb()
						.getApproximateCreationDateTime()
						.toInstant()
						.atZone(ZoneId.systemDefault())
						.toLocalDateTime()
						.format(DateTimeFormatter.ofPattern("yyyy-MM-dd'T'HH:mm:ss.SSS'Z'"));

		context.getLogger().log("Saving updated item with values: key = " + key + ", oldValue = " + oldValue +
				", newValue = " + newValue);

		Map<String, com.amazonaws.services.dynamodbv2.model.AttributeValue> auditItem = new LinkedHashMap<>();
		auditItem.put("id",
					  new com.amazonaws.services.dynamodbv2.model.AttributeValue(UUID.randomUUID().toString()));
		auditItem.put("itemKey", new com.amazonaws.services.dynamodbv2.model.AttributeValue(key));
		auditItem.put("modificationTime",
					  new com.amazonaws.services.dynamodbv2.model.AttributeValue(modificationTime));
		auditItem.put("updatedAttribute", new com.amazonaws.services.dynamodbv2.model.AttributeValue("value"));
		auditItem.put("oldValue", new com.amazonaws.services.dynamodbv2.model.AttributeValue(oldValue));
		auditItem.put("newValue", new com.amazonaws.services.dynamodbv2.model.AttributeValue(newValue));
		
		amazonDynamoDB.putItem(System.getenv("target_table"), auditItem);
	}

	private void initDynamoDbClient() {
		this.amazonDynamoDB = AmazonDynamoDBClientBuilder.standard()
				.withRegion(Regions.EU_CENTRAL_1)
				.build();
	}

}
