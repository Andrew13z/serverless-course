package com.task02;

import com.amazonaws.services.lambda.runtime.Context;
import com.amazonaws.services.lambda.runtime.RequestHandler;
import com.amazonaws.services.lambda.runtime.events.APIGatewayProxyRequestEvent;
import com.amazonaws.services.lambda.runtime.events.APIGatewayProxyResponseEvent;
import com.syndicate.deployment.annotations.LambdaUrlConfig;
import com.syndicate.deployment.annotations.lambda.LambdaHandler;
import com.syndicate.deployment.model.lambda.url.AuthType;
import com.syndicate.deployment.model.lambda.url.InvokeMode;

@LambdaHandler(lambdaName = "hello_world",
		roleName = "hello_world-role",
		isPublishVersion = true,
		aliasName = "${lambdas_alias_name}"
)
@LambdaUrlConfig(
		authType = AuthType.NONE,
		invokeMode = InvokeMode.BUFFERED
)
public class HelloWorld implements RequestHandler<APIGatewayProxyRequestEvent, APIGatewayProxyResponseEvent> {

	@Override
	public APIGatewayProxyResponseEvent handleRequest(APIGatewayProxyRequestEvent request,
													  Context context) {
		if ("GET".equals(request.getHttpMethod()) && request.getPath().endsWith("/hello")) {

			return new APIGatewayProxyResponseEvent()
					.withStatusCode(200)
					.withBody("{\n" +
									  "    \"statusCode\": 200,\n" +
									  "    \"message\": \"Hello from Lambda\"\n" +
									  "}");
		}

		return new APIGatewayProxyResponseEvent().withStatusCode(400);
	}
}
