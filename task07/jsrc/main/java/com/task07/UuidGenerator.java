package com.task07;

import com.amazonaws.regions.Regions;
import com.amazonaws.services.lambda.runtime.Context;
import com.amazonaws.services.lambda.runtime.RequestHandler;
import com.amazonaws.services.s3.AmazonS3;
import com.amazonaws.services.s3.AmazonS3ClientBuilder;
import com.amazonaws.services.s3.model.PutObjectRequest;
import com.syndicate.deployment.annotations.environment.EnvironmentVariable;
import com.syndicate.deployment.annotations.environment.EnvironmentVariables;
import com.syndicate.deployment.annotations.events.RuleEventSource;
import com.syndicate.deployment.annotations.lambda.LambdaHandler;

import java.io.File;
import java.io.FileOutputStream;
import java.io.IOException;
import java.io.PrintWriter;
import java.time.LocalDateTime;
import java.time.format.DateTimeFormatter;
import java.util.List;
import java.util.UUID;
import java.util.stream.Collectors;
import java.util.stream.IntStream;

@LambdaHandler(lambdaName = "uuid_generator",
		roleName = "uuid_generator-role"
)
@RuleEventSource(targetRule = "uuid_trigger")
@EnvironmentVariables(value = {
		@EnvironmentVariable(key = "target_bucket", value = "${target_bucket}"),
})
public class UuidGenerator implements RequestHandler<Object, Void> {

	private AmazonS3 amazonS3;


	public Void handleRequest(Object request, Context context) {
		String time = LocalDateTime.now().format(DateTimeFormatter.ofPattern("yyyy-MM-dd'T'HH:mm:ss.SSS'Z'"));

		PutObjectRequest putOb = new PutObjectRequest(
				System.getenv("target_bucket"),
				time,
				generateFile(context));

		amazonS3.putObject(putOb);

		return null;
	}

	private File generateFile(Context context) {
		List<String> uuids = IntStream.range(0, 10)
				.mapToObj(i -> UUID.randomUUID().toString())
				.collect(Collectors.toList());

		FileOutputStream fos = null;
		PrintWriter pw = null;
		try {
			File tempFile = File.createTempFile("prefix", "suffix");
			fos = new FileOutputStream(tempFile);
			pw = new PrintWriter(fos);
			uuids.forEach(pw::println);
			
			return tempFile;
		} catch (IOException e) {
			throw new RuntimeException(e);
		} finally {
			try {
				if (fos != null) {
					fos.close();
				}
			} catch (Exception e) {
				context.getLogger().log("Error when closing fos: " + e.getMessage());
			}
			if (pw != null) {
				pw.close();
			}
		}
	}

	private void initDynamoDbClient() {
		this.amazonS3 = AmazonS3ClientBuilder.standard()
				.withRegion(Regions.EU_CENTRAL_1)
				.build();
	}
}
