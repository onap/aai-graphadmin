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
package org.onap.aai.db.schema;

import com.beust.jcommander.JCommander;
import com.beust.jcommander.Parameter;
import com.fasterxml.jackson.databind.ObjectMapper;
import org.apache.commons.configuration.ConfigurationException;
import org.apache.commons.configuration.PropertiesConfiguration;
import org.codehaus.jackson.JsonGenerationException;
import org.janusgraph.core.JanusGraph;
import org.janusgraph.core.JanusGraphFactory;
import org.onap.aai.config.PropertyPasswordConfiguration;
import org.onap.aai.dbmap.AAIGraphConfig;
import org.onap.aai.edges.EdgeIngestor;
import org.onap.aai.exceptions.AAIException;
import org.onap.aai.logging.ErrorLogHelper;
import org.onap.aai.logging.ErrorObjectFormatException;
import org.onap.aai.logging.LoggingContext;
import org.onap.aai.logging.LoggingContext.StatusCode;
import org.onap.aai.setup.SchemaVersions;
import org.onap.aai.util.AAIConfig;
import org.onap.aai.util.ExceptionTranslator;
import org.springframework.context.annotation.AnnotationConfigApplicationContext;

import java.io.IOException;
import java.util.UUID;

public class ScriptDriver {

	/**
	 * The main method.
	 *
	 * @param args the arguments
	 * @throws AAIException the AAI exception
	 * @throws JsonGenerationException the json generation exception
	 * @throws IOException Signals that an I/O exception has occurred.
	 */
	public static void main (String[] args) throws AAIException, IOException, ConfigurationException, ErrorObjectFormatException {
		CommandLineArgs cArgs = new CommandLineArgs();
		
		LoggingContext.init();
		LoggingContext.component("DBSchemaScriptDriver");
		LoggingContext.partnerName("NA");
		LoggingContext.targetEntity("AAI");
		LoggingContext.requestId(UUID.randomUUID().toString());
		LoggingContext.serviceName("AAI");
		LoggingContext.targetServiceName("main");
		LoggingContext.statusCode(StatusCode.COMPLETE);
		LoggingContext.responseCode(LoggingContext.SUCCESS);
		ErrorLogHelper.loadProperties();
		new JCommander(cArgs, args);
		
		if (cArgs.help) {
			System.out.println("-c [path to graph configuration] -type [what you want to audit - oxm or graph]");
		}

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
			LoggingContext.statusCode(LoggingContext.StatusCode.ERROR);
			LoggingContext.responseCode(LoggingContext.DATA_ERROR);
			ErrorLogHelper.logError(aai.getCode(), e.getMessage() + ", resolve and retry");
			throw aai;
		}
		AuditorFactory auditorFactory = ctx.getBean(AuditorFactory.class);
		SchemaVersions schemaVersions = ctx.getBean(SchemaVersions.class);
		EdgeIngestor edgeIngestor     = ctx.getBean(EdgeIngestor.class);

		String config = cArgs.config;
		AAIConfig.init();

		PropertiesConfiguration graphConfiguration = new AAIGraphConfig
			.Builder(config)
			.forService(ScriptDriver.class.getSimpleName())
			.withGraphType("NA")
			.buildConfiguration();

		try (JanusGraph graph = JanusGraphFactory.open(graphConfiguration)) {
			if (!("oxm".equals(cArgs.type) || "graph".equals(cArgs.type))) {
				System.out.println("type: " + cArgs.type + " not recognized.");
				System.exit(1);
			}

			AuditDoc doc = null;
			if ("oxm".equals(cArgs.type)) {
				doc = auditorFactory.getOXMAuditor(schemaVersions.getDefaultVersion(), edgeIngestor).getAuditDoc();
			} else if ("graph".equals(cArgs.type)) {
				doc = auditorFactory.getGraphAuditor(graph).getAuditDoc();
			}

			ObjectMapper mapper = new ObjectMapper();

			String json = mapper.writerWithDefaultPrettyPrinter().writeValueAsString(doc);
			System.out.println(json);
		}
	}
	
}

class CommandLineArgs {
	
	@Parameter(names = "--help", description = "Help")
	public boolean help = false;
	
	@Parameter(names = "-c", description = "Configuration", required=true)
	public String config;
	
	@Parameter(names = "-type", description = "Type", required=true)
	public String type = "graph";
	

}