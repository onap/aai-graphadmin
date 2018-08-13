package org.onap.aai.util;

import com.beust.jcommander.JCommander;
import com.beust.jcommander.Parameter;
import org.onap.aai.dbmap.AAIGraph;
import org.onap.aai.introspection.LoaderFactory;
import org.onap.aai.logging.LoggingContext;
import org.onap.aai.migration.EventAction;
import org.onap.aai.setup.SchemaVersions;
import org.springframework.context.annotation.AnnotationConfigApplicationContext;

import java.util.*;

public class SendDeleteMigrationNotificationsMain {

	public static void main(String[] args) {

		Arrays.asList(args).stream().forEach(System.out::println);

		String requestId = UUID.randomUUID().toString();
		LoggingContext.init();
		LoggingContext.partnerName("Migration");
		LoggingContext.serviceName(AAIConstants.AAI_RESOURCES_MS);
		LoggingContext.component("SendMigrationNotifications");
		LoggingContext.targetEntity(AAIConstants.AAI_RESOURCES_MS);
		LoggingContext.targetServiceName("main");
		LoggingContext.requestId(requestId);
		LoggingContext.statusCode(LoggingContext.StatusCode.COMPLETE);
		LoggingContext.responseCode(LoggingContext.SUCCESS);

		AnnotationConfigApplicationContext ctx = new AnnotationConfigApplicationContext(
				"org.onap.aai.config",
				"org.onap.aai.setup"
		);

		LoaderFactory loaderFactory = ctx.getBean(LoaderFactory.class);
		SchemaVersions schemaVersions = ctx.getBean(SchemaVersions.class);
		String basePath = ctx.getEnvironment().getProperty("schema.uri.base.path");

		CommandLineDeleteArgs cArgs = new CommandLineDeleteArgs();

		JCommander jCommander = new JCommander(cArgs, args);
		jCommander.setProgramName(SendDeleteMigrationNotificationsMain.class.getSimpleName());

		EventAction action = EventAction.valueOf(cArgs.eventAction.toUpperCase());

		SendDeleteMigrationNotifications internal = new SendDeleteMigrationNotifications(loaderFactory, schemaVersions, cArgs.config, cArgs.file, new HashSet<>(cArgs.notifyOn), cArgs.sleepInMilliSecs, cArgs.numToBatch, requestId, action, cArgs.eventSource);

		try {
			internal.process(basePath);
		} catch (Exception e) {
			e.printStackTrace();
		}
		AAIGraph.getInstance().graphShutdown();
		System.exit(0);
	}
}

class CommandLineDeleteArgs {

	@Parameter(names = "--help", help = true)
	public boolean help;

	@Parameter(names = "-c", description = "location of configuration file", required = true)
	public String config;

	@Parameter(names = "--inputFile", description = "path to input file", required = true)
	public String file;

	@Parameter (names = "--notifyOn", description = "path to input file")
	public List<String> notifyOn = new ArrayList<>();

	@Parameter (names = "--sleepInMilliSecs", description = "how long to sleep between sending in seconds", validateWith = PositiveNumValidator.class)
	public Integer sleepInMilliSecs = 0;

	@Parameter (names = "--numToBatch", description = "how many to batch before sending", validateWith = PositiveNumValidator.class)
	public Integer numToBatch = 1;

	@Parameter (names = "-a", description = "event action type for dmaap event: CREATE, UPDATE, or DELETE")
	public String eventAction = EventAction.DELETE.toString();

	@Parameter (names = "--eventSource", description = "source of truth for notification, defaults to DMAAP-LOAD")
	public String eventSource = "DMAAP-LOAD";
}


