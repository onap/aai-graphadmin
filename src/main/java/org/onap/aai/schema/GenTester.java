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
package org.onap.aai.schema;

import java.util.List;
import java.util.Optional;
import java.util.Set;

import org.apache.tinkerpop.gremlin.process.traversal.dsl.graph.__;
import org.apache.tinkerpop.gremlin.structure.Vertex;
import org.janusgraph.core.JanusGraph;
import org.janusgraph.core.PropertyKey;
import org.janusgraph.core.schema.JanusGraphManagement;
import org.janusgraph.graphdb.database.StandardJanusGraph;
import org.janusgraph.graphdb.database.management.ManagementSystem;
import org.onap.aai.dbgen.SchemaGenerator;
import org.onap.aai.dbmap.AAIGraph;
import org.onap.aai.exceptions.AAIException;
import org.onap.aai.logging.ErrorLogHelper;
import org.onap.aai.util.AAIConfig;
import org.onap.aai.util.ExceptionTranslator;
import org.onap.aai.util.GraphAdminDBUtils;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.context.annotation.AnnotationConfigApplicationContext;

public class GenTester {

	private static Logger LOGGER;
	private static boolean historyEnabled;
	private static final String SCHEMA_INITIALIZED  = "schema-initialized";

	/**
	 * The main method.
	 *
	 * @param args the arguments
	 */
	public static void main(String[] args) throws AAIException {

		JanusGraph graph = null;
		System.setProperty("aai.service.name", GenTester.class.getSimpleName());

		LOGGER = LoggerFactory.getLogger(GenTester.class);
		boolean addDefaultCR = true;

		AnnotationConfigApplicationContext ctx = new AnnotationConfigApplicationContext();
		try {
			ctx.scan(
					"org.onap.aai");
			ctx.refresh();
		} catch (Exception e) {
			AAIException aai = ExceptionTranslator.schemaServiceExceptionTranslator(e);
			LOGGER.error("Problems running the tool " + aai.getMessage());
			ErrorLogHelper.logError(aai.getCode(), e.getMessage() + ", resolve and retry");
			throw aai;
		}
		historyEnabled = Boolean.parseBoolean(ctx.getEnvironment().getProperty("history.enabled", "false"));
		if (historyEnabled) {
			String amsg = "GenTester may only be used when history.enabled=false. ";
			System.out.println(amsg);
			LOGGER.debug(amsg);
			return;
		}
		try {
			LOGGER.debug("GenTester uses either cql jar or Cassandra jar");

			AAIConfig.init();
			if (args != null && args.length > 0) {
				if ("genDbRulesOnly".equals(args[0])) {
					ErrorLogHelper.logError("AAI_3100",
							" This option is no longer supported. What was in DbRules is now derived from the OXM files. ");
					return;
				} else if ("GEN_DB_WITH_NO_SCHEMA".equals(args[0])) {
					// Note this is done to create an empty DB with no Schema so that
					// an HBase copyTable can be used to set up a copy of the db.
					String imsg = "    ---- NOTE --- about to load a graph without doing any schema processing (takes a little while) --------   ";
					System.out.println(imsg);
					LOGGER.debug(imsg);
					graph = AAIGraph.getInstance().getGraph();

					if (graph == null) {
						ErrorLogHelper.logError("AAI_5102", "Error creating JanusGraph graph.");
						return;
					} else {
						LOGGER.debug("Successfully loaded a JanusGraph graph without doing any schema work.");
						return;
					}
				} else if ("GEN_DB_WITH_NO_DEFAULT_CR".equals(args[0])) {
					addDefaultCR = false;
				} else {
					ErrorLogHelper.logError("AAI_3000",
							"Unrecognized argument passed to GenTester.java: [" + args[0] + "]. ");

					String emsg = "Unrecognized argument passed to GenTester.java: [" + args[0] + "]. ";
					System.out.println(emsg);
					LOGGER.error(emsg);

					emsg = "Either pass no argument for normal processing, or use 'GEN_DB_WITH_NO_SCHEMA'.";
					System.out.println(emsg);
					LOGGER.error(emsg);

					return;
				}
			}

			// AAIConfig.init();
			ErrorLogHelper.loadProperties();

			LOGGER.debug("about to open graph (takes a little while)");
			graph = AAIGraph.getInstance().getGraph();

			if (graph == null) {
				ErrorLogHelper.logError("AAI_5102", "Error creating JanusGraph graph. ");
				return;
			}

			Optional<Vertex> schemaInitializedVertex = graph.traversal().V()
				.or(
					__.has(SCHEMA_INITIALIZED, true),
					__.has(SCHEMA_INITIALIZED, false)
				)
				.tryNext();

			if (schemaInitializedVertex.isPresent()) {
				//Set schema-initialized vertex to false if such vertex is present in db
				setSchemaInitializedToFalse(graph, schemaInitializedVertex);
			} else {
				// Creating a new vertex as the vertex is not yet present in db
				createNewSchemaInitializedVertex(graph);
			}

			GraphAdminDBUtils.logConfigs(graph.configuration());

			LOGGER.debug("-- Loading new schema elements into JanusGraph --");

			boolean dbNotEmpty = (graph.traversal().V().limit(1).hasNext());
			LOGGER.info("DB is not empty. Newly created indexes will also be reindexed.");
			List<String> vertexesToReindex = SchemaGenerator.loadSchemaIntoJanusGraph(graph, null, dbNotEmpty);
			LOGGER.debug("-- committing transaction ");
			graph.tx().commit();

			boolean reindexingEnabled = false; // disable reindexing for now, since it's not working correctly
			if (reindexingEnabled && !vertexesToReindex.isEmpty()) {
				killTransactionsAndInstances(graph);
				LOGGER.info("Number of edge indexes to reindex: " + vertexesToReindex.size());
				SchemaGenerator.reindexEdgeIndexes(graph, vertexesToReindex);
			} else {
				if (vertexesToReindex.isEmpty()) {
					LOGGER.info("Nothing to reindex.");
				}
			}

			// Setting property schema-initialized to true
			LOGGER.debug("-- Updating vertex with property schema-initialized to true ");
			graph.traversal().V().has(SCHEMA_INITIALIZED , false).property(SCHEMA_INITIALIZED , true).next();
			LOGGER.debug("-- committing transaction ");
			graph.tx().commit();

			graph.close();
			LOGGER.info("Closed the graph");

		} catch (Exception ex) {
			ErrorLogHelper.logError("AAI_4000", ex.getMessage());
			System.exit(1);
		}

		LOGGER.debug("-- all done, if program does not exit, please kill.");
		System.exit(0);
	}

	private static void setSchemaInitializedToFalse(JanusGraph graph, Optional<Vertex> schemaInitializedVertex) {
		Vertex vertex = schemaInitializedVertex.get();
		Object schemaInitializedValueObj = vertex.property(SCHEMA_INITIALIZED).value();
		Boolean schemaInitializedValue = schemaInitializedValueObj instanceof Boolean b ? b : Boolean.FALSE;

		//Setting schema-initialized vertex to False
		if (Boolean.TRUE.equals(schemaInitializedValue)) {
			// Update the property from true to false
			LOGGER.debug("-- Vertex with property 'schema-initialized' present in db and is true. Updating it to false");
			graph.traversal().V()
				.has(SCHEMA_INITIALIZED, true)
				.property(SCHEMA_INITIALIZED, false)
				.next();
		} else {
			// Property already false, no action needed
			LOGGER.debug("-- Vertex with property 'schema-initialized' present in db and is false. Keeping it false. Do Nothing");
		}
	}

	private static void createNewSchemaInitializedVertex(JanusGraph graph) throws Exception {
		LOGGER.debug("-- Adding a new vertex with property schema-initialized as false");
		JanusGraphManagement mgmt = graph.openManagement();
		try {
			// Creating an index
			createSchemaInitializedIndex(graph, mgmt);
		} catch (Exception e) {
			mgmt.rollback();
			LOGGER.error("Problems creating an index for schema-initialized vertex " + e.getMessage());
			throw e;
		}
		try {
			Vertex newVertex = graph.addVertex(SCHEMA_INITIALIZED , false);
			LOGGER.info("Created a new vertex with property '{}' set to '{}'", SCHEMA_INITIALIZED ,
				newVertex.property(SCHEMA_INITIALIZED ).value());
		} catch (Exception e) {
			LOGGER.error("Error creating a new vertex: {}", e.getMessage(), e);
			throw e;
		}
	}

	private static void createSchemaInitializedIndex(JanusGraph graph, JanusGraphManagement mgmt) throws InterruptedException {
		// creating a composite index
		LOGGER.debug("-- Building an index on property schema-initialized");
		PropertyKey schemaInitialized = mgmt.makePropertyKey(SCHEMA_INITIALIZED).dataType(Boolean.class).make();
		mgmt.buildIndex(SCHEMA_INITIALIZED, Vertex.class)
				.addKey(schemaInitialized)
				.buildCompositeIndex();
		mgmt.commit();

		// Wait for the index to become available
		ManagementSystem.awaitGraphIndexStatus(graph, SCHEMA_INITIALIZED).call();
	}

	/**
	 * Radical approach to avoiding index update failures.
	 * Indexes can get stuck in INSTALLED state, when there are stale transactions
	 * or JanusGraph instances.
	 * This is because a state change needs to be acknowledged by all instances
	 * before transitioning.
	 *
	 * @param graph
	 * @return
	 */
	private static void killTransactionsAndInstances(JanusGraph graph) {
		graph.tx().rollback();
		final StandardJanusGraph janusGraph = (StandardJanusGraph) graph;
		janusGraph.getOpenTransactions().stream().forEach(transaction -> {
			LOGGER.debug("Closing open transaction [{}] before schema generation", transaction.toString());
			transaction.rollback();
		});

		final JanusGraphManagement graphMgtForClosing = graph.openManagement();

		Set<String> instances = graphMgtForClosing.getOpenInstances();
		LOGGER.info("Number of open instances: {}", instances.size());
		LOGGER.info("Currently open instances: [{}]", instances);
		instances.stream()
				.filter(instance -> !instance.contains("graphadmin")) // Potentially comment this out, should there be
				// issues with the schema creation job
				.filter(instance -> !instance.contains("(current)"))
				.forEach(instance -> {
					LOGGER.debug("Closing open JanusGraph instance [{}] before reindexing procedure", instance);
					graphMgtForClosing.forceCloseInstance(instance);
				});
		graphMgtForClosing.commit();
	}

}
