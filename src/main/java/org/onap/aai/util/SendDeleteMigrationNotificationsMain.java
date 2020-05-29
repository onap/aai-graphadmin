/**
 * ============LICENSE_START=======================================================
 * org.onap.aai
 * ================================================================================
 * Copyright © 2017-2018 AT&T Intellectual Property. All rights reserved.
 * ================================================================================
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *    http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 * ============LICENSE_END=========================================================
 */
package org.onap.aai.util;

import com.beust.jcommander.JCommander;
import com.beust.jcommander.Parameter;
import org.onap.aai.restclient.PropertyPasswordConfiguration;
import org.onap.aai.dbmap.AAIGraph;
import org.onap.aai.exceptions.AAIException;
import org.onap.aai.introspection.LoaderFactory;
import org.onap.aai.logging.ErrorLogHelper;
import org.onap.aai.migration.EventAction;
import org.onap.aai.setup.SchemaVersions;
import org.springframework.context.annotation.AnnotationConfigApplicationContext;

import java.util.*;

public class SendDeleteMigrationNotificationsMain {

	public static void main(String[] args) throws AAIException {

		Arrays.asList(args).stream().forEach(System.out::println);

		String requestId = UUID.randomUUID().toString();

		AnnotationConfigApplicationContext ctx = new AnnotationConfigApplicationContext();
		PropertyPasswordConfiguration initializer = new PropertyPasswordConfiguration();
		initializer.initialize(ctx);
		try {
			ctx.scan(
					"org.onap.aai.config",
					"org.onap.aai.setup"
			);
			ctx.refresh();
		} catch (Exception e) {
			AAIException aai = ExceptionTranslator.schemaServiceExceptionTranslator(e);
			ErrorLogHelper.logError(aai.getCode(), e.getMessage() + ", resolve and retry");
			throw aai;
		}
		LoaderFactory loaderFactory = ctx.getBean(LoaderFactory.class);
		SchemaVersions schemaVersions = (SchemaVersions) ctx.getBean("schemaVersions");
		String basePath = ctx.getEnvironment().getProperty("schema.uri.base.path");

		CommandLineDeleteArgs cArgs = new CommandLineDeleteArgs();

		JCommander jCommander = new JCommander(cArgs, args);
		jCommander.setProgramName(SendDeleteMigrationNotificationsMain.class.getSimpleName());

		EventAction action = EventAction.valueOf(cArgs.eventAction.toUpperCase());

		SendDeleteMigrationNotifications internal = new SendDeleteMigrationNotifications(loaderFactory, schemaVersions, cArgs.config, cArgs.file, cArgs.sleepInMilliSecs, cArgs.numToBatch, requestId, action, cArgs.eventSource);

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


