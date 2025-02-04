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
import org.apache.commons.configuration2.PropertiesConfiguration;
import org.janusgraph.core.JanusGraph;
import org.janusgraph.core.JanusGraphFactory;
import org.onap.aai.dbmap.AAIGraphConfig;
import org.onap.aai.edges.EdgeIngestor;
import org.onap.aai.exceptions.AAIException;
import org.onap.aai.logging.ErrorLogHelper;
import org.onap.aai.logging.ErrorObjectFormatException;
import org.onap.aai.setup.SchemaVersions;
import org.onap.aai.util.AAIConfig;
import org.onap.aai.util.ExceptionTranslator;
import org.springframework.context.annotation.AnnotationConfigApplicationContext;

import java.io.IOException;
public class ScriptDriver {

	/**
	 * The main method.
	 *
	 * @param args the arguments
	 * @throws AAIException the AAI exception
	 * @throws JsonGenerationException the json generation exception
	 * @throws IOException Signals that an I/O exception has occurred.
	 * @throws org.apache.commons.configuration2.ex.ConfigurationException
	 */
	public static void main (String[] args) throws AAIException, IOException, ErrorObjectFormatException, org.apache.commons.configuration2.ex.ConfigurationException {
		CommandLineArgs cArgs = new CommandLineArgs();

		ErrorLogHelper.loadProperties();
		new JCommander(cArgs, args);

		if (cArgs.help) {
			System.out.println("-c [path to graph configuration] -type [what you want to audit - oxm or graph]");
		}

		AnnotationConfigApplicationContext ctx = new AnnotationConfigApplicationContext();
		try {
			ctx.scan(
					"org.onap.aai.config",
					"org.onap.aai.introspection"
			);
			ctx.refresh();

		} catch (Exception e) {
			AAIException aai = ExceptionTranslator.schemaServiceExceptionTranslator(e);
			ErrorLogHelper.logError(aai.getCode(), e.getMessage() + ", resolve and retry");
			throw aai;
		}
		AuditorFactory auditorFactory = ctx.getBean(AuditorFactory.class);
		SchemaVersions schemaVersions = (SchemaVersions) ctx.getBean("schemaVersions");
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
