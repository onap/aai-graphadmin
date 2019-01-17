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
package org.onap.aai.migration;

import org.onap.aai.config.PropertyPasswordConfiguration;
import org.onap.aai.dbmap.AAIGraph;
import org.onap.aai.edges.EdgeIngestor;
import org.onap.aai.exceptions.AAIException;
import org.onap.aai.introspection.LoaderFactory;
import org.onap.aai.logging.ErrorLogHelper;
import org.onap.aai.logging.LoggingContext;
import org.onap.aai.logging.LoggingContext.StatusCode;
import org.onap.aai.serialization.db.EdgeSerializer;
import org.onap.aai.setup.SchemaVersions;
import org.onap.aai.util.AAIConstants;
import org.onap.aai.util.ExceptionTranslator;
import org.springframework.context.annotation.AnnotationConfigApplicationContext;

import java.util.UUID;

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
	public static void main(String[] args) throws AAIException {

		LoggingContext.init();
		LoggingContext.partnerName("Migration");
		LoggingContext.serviceName(AAIConstants.AAI_RESOURCES_MS);
		LoggingContext.component("MigrationController");
		LoggingContext.targetEntity(AAIConstants.AAI_RESOURCES_MS);
		LoggingContext.targetServiceName("main");
		LoggingContext.requestId(UUID.randomUUID().toString());
		LoggingContext.statusCode(StatusCode.COMPLETE);
		LoggingContext.responseCode(LoggingContext.SUCCESS);

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
			System.out.println("Problems running tool "+aai.getMessage());
			LoggingContext.statusCode(LoggingContext.StatusCode.ERROR);
			LoggingContext.responseCode(LoggingContext.DATA_ERROR);
			ErrorLogHelper.logError(aai.getCode(), e.getMessage() + ", resolve and retry");
			throw aai;
		}
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
