package com.task05;

import com.amazonaws.regions.Regions;
import com.amazonaws.services.dynamodbv2.AmazonDynamoDB;
import com.amazonaws.services.dynamodbv2.AmazonDynamoDBClientBuilder;
import com.amazonaws.services.dynamodbv2.model.AttributeValue;
import com.amazonaws.services.lambda.runtime.Context;
import com.amazonaws.services.lambda.runtime.RequestHandler;
import com.amazonaws.services.lambda.runtime.events.APIGatewayProxyRequestEvent;
import com.amazonaws.services.lambda.runtime.events.APIGatewayProxyResponseEvent;
import com.amazonaws.services.lambda.runtime.events.APIGatewayV2HTTPEvent;
import com.amazonaws.services.lambda.runtime.events.APIGatewayV2HTTPResponse;
import com.google.gson.Gson;
import com.syndicate.deployment.annotations.lambda.LambdaHandler;

import java.time.LocalDateTime;
import java.time.format.DateTimeFormatter;
import java.util.HashMap;
import java.util.Map;
import java.util.UUID;

@LambdaHandler(lambdaName = "api_handler",
		roleName = "api_handler-role"
)
public class ApiHandler implements RequestHandler<APIGatewayV2HTTPEvent, APIGatewayV2HTTPResponse> {

	private static final int SC_OK = 200;
	private static final String TABLE_NAME = "Events";
	private static final Regions REGION = Regions.EU_CENTRAL_1;

	private AmazonDynamoDB amazonDynamoDB;
	private final Gson gson = new Gson();

	@Override
	public APIGatewayV2HTTPResponse handleRequest(APIGatewayV2HTTPEvent apiGatewayProxyRequestEvent,
													  Context context) {
		context.getLogger().log("reqeust event: " + apiGatewayProxyRequestEvent.toString());
		
		String json = apiGatewayProxyRequestEvent.getBody();
		context.getLogger().log("json: " + json);
		ApiRequest apiRequest = gson.fromJson(json, ApiRequest.class);
		context.getLogger().log("mapped: " + apiRequest.toString());

		Event event = toEvent(apiRequest);

		initDynamoDbClient();

		persist(event);

		return APIGatewayV2HTTPResponse.builder()
				.withStatusCode(SC_OK)
				.withBody(gson.toJson(new ApiResponse(SC_OK, event)))
				.build();
	}

	private Event toEvent(ApiRequest apiRequest) {
		Event event = new Event();

		event.setId(UUID.randomUUID().toString());
		event.setPrincipalId(apiRequest.getPrincipalId());
		event.setCreatedAt(LocalDateTime.now().format(
				DateTimeFormatter.ofPattern("yyyy-MM-dd'T'HH:mm:ss'Z'")));
		event.setBody(apiRequest.getContent());

		return event;
	}

	private void persist(Event event) {
		Map<String, AttributeValue> attributesMap = new HashMap<>();

		attributesMap.put("id", new AttributeValue(event.getId()));
		attributesMap.put("principalId", new AttributeValue(String.valueOf(event.getPrincipalId())));
		attributesMap.put("createdAt", new AttributeValue(event.getCreatedAt()));
		attributesMap.put("body", new AttributeValue(gson.toJson(event.getBody())));

		amazonDynamoDB.putItem(TABLE_NAME, attributesMap);
	}

	private void initDynamoDbClient() {
		this.amazonDynamoDB = AmazonDynamoDBClientBuilder.standard()
				.withRegion(REGION)
				.build();
	}
}
