package com.task04;

import com.amazonaws.services.lambda.runtime.Context;
import com.amazonaws.services.lambda.runtime.LambdaLogger;
import com.amazonaws.services.lambda.runtime.RequestHandler;
import com.amazonaws.services.lambda.runtime.events.SNSEvent;
import com.syndicate.deployment.annotations.events.SnsEventSource;
import com.syndicate.deployment.annotations.lambda.LambdaHandler;
import com.syndicate.deployment.model.events.SnsTriggerEventSourceItem;

import java.util.HashMap;
import java.util.Map;

@LambdaHandler(lambdaName = "sns_handler",
	roleName = "sns_handler-role",
	isPublishVersion = true
)
@SnsEventSource(targetTopic = "lambda_topic")
public class SnsHandler implements RequestHandler<SNSEvent, Void> {

	@Override
	public Void handleRequest(SNSEvent snsEvent, Context context) {
		LambdaLogger logger = context.getLogger();
		logger.log(snsEvent.toString());
		return null;
	}
}
