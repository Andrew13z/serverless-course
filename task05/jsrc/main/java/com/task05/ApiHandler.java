package com.task05;

import com.amazonaws.regions.Regions;
import com.amazonaws.services.dynamodbv2.AmazonDynamoDB;
import com.amazonaws.services.dynamodbv2.AmazonDynamoDBClientBuilder;
import com.amazonaws.services.dynamodbv2.model.AttributeValue;
import com.amazonaws.services.lambda.runtime.Context;
import com.amazonaws.services.lambda.runtime.RequestHandler;
import com.amazonaws.services.lambda.runtime.events.APIGatewayProxyResponseEvent;
import com.google.gson.Gson;
import com.syndicate.deployment.annotations.environment.EnvironmentVariable;
import com.syndicate.deployment.annotations.environment.EnvironmentVariables;
import com.syndicate.deployment.annotations.lambda.LambdaHandler;
import com.syndicate.deployment.annotations.resources.DependsOn;
import com.syndicate.deployment.model.ResourceType;

import java.time.LocalDateTime;
import java.time.format.DateTimeFormatter;
import java.util.Collections;
import java.util.HashMap;
import java.util.Map;
import java.util.UUID;

@LambdaHandler(lambdaName = "api_handler",
		roleName = "api_handler-role"
)
@DependsOn(name = "Events", resourceType = ResourceType.DYNAMODB_TABLE)
@EnvironmentVariables(value = {
		@EnvironmentVariable(key = "target_table", value = "${target_table}"),
})
public class ApiHandler implements RequestHandler<ApiRequest, APIGatewayProxyResponseEvent> {

	private static final int SC_OK = 200;
	private static final Regions REGION = Regions.EU_CENTRAL_1;

	private AmazonDynamoDB amazonDynamoDB;
	private final Gson gson = new Gson();

	@Override
	public APIGatewayProxyResponseEvent handleRequest(ApiRequest apiRequest,
													  Context context) {
		context.getLogger().log("mapped: " + apiRequest.toString());

		Event event = toEvent(apiRequest);

		initDynamoDbClient();

		persist(event);

		return new APIGatewayProxyResponseEvent()
				.withStatusCode(SC_OK)
				.withBody(gson.toJson(new ApiResponse(SC_OK, event)));
	}

	private Event toEvent(ApiRequest apiRequest) {
		Event event = new Event();

		event.setId(UUID.randomUUID().toString());
		event.setPrincipalId(apiRequest.getPrincipalId());
		event.setCreatedAt(LocalDateTime.now().format(
				DateTimeFormatter.ofPattern("yyyy-MM-dd'T'HH:mm:ss.ss'Z'")));
		event.setBody(apiRequest.getContent());

		return event;
	}

	private void persist(Event event) {
		Map<String, AttributeValue> attributesMap = new HashMap<>();

		attributesMap.put("id", new AttributeValue(event.getId()));
		attributesMap.put("principalId", new AttributeValue().withN(String.valueOf(event.getPrincipalId())));
		attributesMap.put("createdAt", new AttributeValue(event.getCreatedAt()));
		attributesMap.put("body", new AttributeValue(mapToString(event.getBody())));

		amazonDynamoDB.putItem(System.getenv("target_table"), attributesMap);
	}

	private void initDynamoDbClient() {
		this.amazonDynamoDB = AmazonDynamoDBClientBuilder.standard()
				.withRegion(REGION)
				.build();
	}

	private String mapToString(Map<String, String> map) {
		StringBuilder sb = new StringBuilder();
		sb.append("{");
		map.forEach((key, value) -> {
			sb.append(key).append("=").append(value).append(", ");
		});
		sb.setLength(sb.length() - 2);
		sb.append("}");
		return sb.toString();
	}
}
