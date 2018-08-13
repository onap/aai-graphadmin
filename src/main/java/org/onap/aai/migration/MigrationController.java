/**
 * ============LICENSE_START=======================================================
 * org.onap.aai
 * ================================================================================
 * Copyright © 2017 AT&T Intellectual Property. All rights reserved.
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
 *
 * ECOMP is a trademark and service mark of AT&T Intellectual Property.
 */
package org.onap.aai.migration;

import java.util.UUID;

import org.onap.aai.dbmap.AAIGraph;
import org.onap.aai.edges.EdgeIngestor;
import org.onap.aai.introspection.LoaderFactory;
import org.onap.aai.logging.LoggingContext;
import org.onap.aai.logging.LoggingContext.StatusCode;
import org.onap.aai.serialization.db.EdgeSerializer;
import org.onap.aai.setup.SchemaVersions;
import org.onap.aai.util.AAIConstants;
import org.springframework.context.annotation.AnnotationConfigApplicationContext;

/**
 * Wrapper class to allow {@link org.onap.aai.migration.MigrationControllerInternal MigrationControllerInternal}
 * to be run from a shell script
 */
public class MigrationController {

	/**
	 * The main method.
	 *
	 * @param args
	 *            the arguments
	 */
	public static void main(String[] args) {

		LoggingContext.init();
		LoggingContext.partnerName("Migration");
		LoggingContext.serviceName(AAIConstants.AAI_RESOURCES_MS);
		LoggingContext.component("MigrationController");
		LoggingContext.targetEntity(AAIConstants.AAI_RESOURCES_MS);
		LoggingContext.targetServiceName("main");
		LoggingContext.requestId(UUID.randomUUID().toString());
		LoggingContext.statusCode(StatusCode.COMPLETE);
		LoggingContext.responseCode(LoggingContext.SUCCESS);

		AnnotationConfigApplicationContext ctx = new AnnotationConfigApplicationContext(
				"org.onap.aai.config",
				"org.onap.aai.setup"
		);

		LoaderFactory loaderFactory   = ctx.getBean(LoaderFactory.class);
		EdgeIngestor   edgeIngestor   = ctx.getBean(EdgeIngestor.class);
		EdgeSerializer edgeSerializer = ctx.getBean(EdgeSerializer.class);
		SchemaVersions schemaVersions = ctx.getBean(SchemaVersions.class);

		MigrationControllerInternal internal = new MigrationControllerInternal(loaderFactory, edgeIngestor, edgeSerializer, schemaVersions);

		try {
			internal.run(args);
		} catch (Exception e) {
			e.printStackTrace();
		}
		AAIGraph.getInstance().graphShutdown();
		System.exit(0);
	}
}
