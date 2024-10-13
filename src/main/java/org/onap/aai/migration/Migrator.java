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

import java.io.File;
import java.io.IOException;
import java.nio.file.Files;
import java.nio.file.Paths;
import java.nio.file.StandardOpenOption;
import java.util.Collections;
import java.util.Iterator;
import java.util.List;
import java.util.Optional;
import java.util.UUID;
import org.apache.tinkerpop.gremlin.process.traversal.dsl.graph.__;
import org.apache.tinkerpop.gremlin.structure.Direction;
import org.apache.tinkerpop.gremlin.structure.Edge;
import org.apache.tinkerpop.gremlin.structure.Property;
import org.apache.tinkerpop.gremlin.structure.Vertex;
import org.apache.tinkerpop.gremlin.structure.VertexProperty;
import org.json.JSONException;
import org.json.JSONObject;
import org.onap.aai.aailog.logs.AaiDebugLog;
import org.onap.aai.config.SpringContextAware;
import org.onap.aai.edges.EdgeIngestor;
import org.onap.aai.edges.enums.EdgeType;
import org.onap.aai.edges.exceptions.AmbiguousRuleChoiceException;
import org.onap.aai.edges.exceptions.EdgeRuleNotFoundException;
import org.onap.aai.exceptions.AAIException;
import org.onap.aai.introspection.Introspector;
import org.onap.aai.introspection.Loader;
import org.onap.aai.introspection.LoaderFactory;
import org.onap.aai.introspection.ModelType;
import org.onap.aai.kafka.NotificationProducer;
import org.onap.aai.serialization.db.DBSerializer;
import org.onap.aai.serialization.db.EdgeSerializer;
import org.onap.aai.serialization.db.exceptions.NoEdgeRuleFoundException;
import org.onap.aai.serialization.engines.TransactionalGraphEngine;
import org.onap.aai.setup.SchemaVersion;
import org.onap.aai.setup.SchemaVersions;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.slf4j.MDC;

/**
 * This class defines an A&AI Migration
 */
@MigrationPriority(0)
@MigrationDangerRating(0)
public abstract class Migrator implements Runnable {

	protected Logger logger = null;

	protected DBSerializer serializer = null;
	protected Loader loader = null;

	protected TransactionalGraphEngine engine;
	protected NotificationHelper notificationHelper;

	protected EdgeSerializer edgeSerializer;
	protected EdgeIngestor edgeIngestor;

	protected LoaderFactory loaderFactory;
	protected SchemaVersions schemaVersions;

	protected static final String MIGRATION_ERROR = "Migration Error: ";
	protected static final String MIGRATION_SUMMARY_COUNT = "Migration Summary Count: ";

	private static AaiDebugLog debugLog = new AaiDebugLog();
	static {
		debugLog.setupMDC();
	}


	/**
	 * Instantiates a new migrator.
	 *
	 * @param g the g
	 * @param schemaVersions
	 */
	public Migrator(TransactionalGraphEngine engine, LoaderFactory loaderFactory, EdgeIngestor edgeIngestor, EdgeSerializer edgeSerializer, SchemaVersions schemaVersions){
        this.engine = engine;
		this.loaderFactory  = loaderFactory;
		this.edgeIngestor = edgeIngestor;
		this.edgeSerializer = edgeSerializer;
		this.schemaVersions = schemaVersions;
		initDBSerializer();
		NotificationProducer notificationProducer = SpringContextAware.getBean(NotificationProducer.class);
		this.notificationHelper = new NotificationHelper(notificationProducer, loader, serializer, loaderFactory, schemaVersions, engine, "AAI-MIGRATION", this.getMigrationName());
		MDC.put("logFilenameAppender", this.getClass().getSimpleName());
        logger = LoggerFactory.getLogger(this.getClass().getSimpleName());
		logAndPrint(logger,"\tInitilization of " + this.getClass().getSimpleName() + " migration script complete.");
	}

	/**
	 * Gets the status.
	 *
	 * @return the status
	 */
	public abstract Status getStatus();

	/**
	 * Rollback.
	 */
	public void rollback() {
        engine.rollback();
	}

	/**
	 * Commit.
	 */
	public void commit() {
        engine.commit();
	}

	/**
	 * Create files containing vertices for dmaap Event Generation
	 * @param dmaapMsgList
	 */
	public void createDmaapFiles(List<String> dmaapMsgList) {
		String fileName = getMigrationName() + "-" + UUID.randomUUID();
		String logDirectory = System.getProperty("AJSC_HOME") + "/logs/data/dmaapEvents";

		File f = new File(logDirectory);
		f.mkdirs();

		if (dmaapMsgList.size() > 0) {
			try {
				Files.write(Paths.get(logDirectory+"/"+fileName), (Iterable<String>)dmaapMsgList.stream()::iterator);
			} catch (IOException e) {
				System.out.println("Unable to generate file with dmaap msgs for " + getMigrationName() +
						" Exception is: " + e.getMessage());
				logger.error("Unable to generate file with dmaap msgs for " + getMigrationName(), e);
			}
		} else {
			logAndPrint(logger,"No dmaap msgs detected for " + getMigrationName());
		}
	}

	/**
	 * Create files containing data for dmaap delete Event Generation
	 * @param dmaapDeleteIntrospectorList
	 */
	public void createDmaapFilesForDelete(List<Introspector> dmaapDeleteIntrospectorList) {
		try {
			System.out.println("dmaapDeleteIntrospectorList :: " + dmaapDeleteIntrospectorList.size());
			String fileName = "DELETE-" + getMigrationName() + "-" + UUID.randomUUID();
			String logDirectory = System.getProperty("AJSC_HOME") + "/logs/data/dmaapEvents/";
			File f = new File(logDirectory);
			f.mkdirs();

			try {
				Files.createFile(Paths.get(logDirectory + "/" + fileName));
			} catch (Exception e) {
				logger.error("Unable to create file", e);
			}

			if (dmaapDeleteIntrospectorList.size() > 0) {
				dmaapDeleteIntrospectorList.stream().forEach(svIntr -> {
					try {
						String str = svIntr.marshal(false);
						String finalStr = "";
						try {
							finalStr =
									svIntr.getName() + "#@#" + svIntr.getURI() + "#@#" + str + "\n";
							Files.write(Paths.get(logDirectory + "/" + fileName),
									finalStr.getBytes(), StandardOpenOption.APPEND);
						} catch (IOException e) {
							System.out.println("Unable to generate file with dmaap msgs for " +
									getMigrationName() +
									" Exception is: " + e.getMessage());
							logger.error("Unable to generate file with dmaap msgs for " +
									getMigrationName(), e);
						}

					} catch (Exception e) {
						// TODO Auto-generated catch block
						System.out.println("Exception : " + e.getMessage());
					}
				});

				//Files.write(Paths.get(logDirectory+"/"+fileName), (Iterable<Vertex>)dmaapVertexList.stream()::iterator);
			}
		} catch (Exception e) {
			logger.error("Unable to generate file with dmaap msgs for " + getMigrationName(), e);
		}
	}

	/**
	 * As string.
	 *
	 * @param v the v
	 * @return the string
	 */
	protected String asString(Vertex v) {
		final JSONObject result = new JSONObject();
		Iterator<VertexProperty<Object>> properties = v.properties();
		Property<Object> pk = null;
		try {
			while (properties.hasNext()) {
				pk = properties.next();
				result.put(pk.key(), pk.value());
			}
		} catch (JSONException e) {
			System.out.println("Warning error reading vertex: " + e.getMessage());
			logger.error("Warning error reading vertex: " + e);
		}

		return result.toString();
	}

	/**
	 * As string.
	 *
	 * @param edge the edge
	 * @return the string
	 */
	protected String asString(Edge edge) {
		final JSONObject result = new JSONObject();
		Iterator<Property<Object>> properties = edge.properties();
		Property<Object> pk = null;
		try {
			while (properties.hasNext()) {
				pk = properties.next();
				result.put(pk.key(), pk.value());
			}
		} catch (JSONException e) {
			System.out.println("Warning error reading edge: " + e.getMessage());
			logger.error("Warning error reading edge: " + e);
		}

		return result.toString();
	}

	/**
	 *
	 * @param v
	 * @param numLeadingTabs number of leading \t char's
	 * @return
	 */
	protected String toStringForPrinting(Vertex v, int numLeadingTabs) {
		String prefix = String.join("", Collections.nCopies(numLeadingTabs, "\t"));
		if (v == null) {
			return "";
		}
		final StringBuilder sb = new StringBuilder();
		sb.append(prefix + v + "\n");
		v.properties().forEachRemaining(prop -> sb.append(prefix + prop + "\n"));
		return sb.toString();
	}

	/**
	 *
	 * @param e
	 * @param numLeadingTabs number of leading \t char's
	 * @return
	 */
	protected String toStringForPrinting(Edge e, int numLeadingTabs) {
		String prefix = String.join("", Collections.nCopies(numLeadingTabs, "\t"));
		if (e == null) {
			return "";
		}
		final StringBuilder sb = new StringBuilder();
		sb.append(prefix + e + "\n");
		sb.append(prefix + e.label() + "\n");
		e.properties().forEachRemaining(prop -> sb.append(prefix + "\t" + prop + "\n"));
		return sb.toString();
	}

	/**
	 * Checks for edge between.
	 *
	 * @param a a
	 * @param b b
	 * @param d d
	 * @param edgeLabel the edge label
	 * @return true, if successful
	 */
	protected boolean hasEdgeBetween(Vertex a, Vertex b, Direction d, String edgeLabel) {

		if (d.equals(Direction.OUT)) {
			return engine.asAdmin().getReadOnlyTraversalSource().V(a).out(edgeLabel).where(__.otherV().hasId(b)).hasNext();
		} else {
			return engine.asAdmin().getReadOnlyTraversalSource().V(a).in(edgeLabel).where(__.otherV().hasId(b)).hasNext();
		}

	}

	/**
	 * Creates the edge
	 *
	 * @param type the edge type - COUSIN or TREE
	 * @param out the out
	 * @param in the in
	 * @return the edge
	 */
	protected Edge createEdge(EdgeType type, Vertex out, Vertex in) throws AAIException {
		Edge newEdge = null;
		try {
			if (type.equals(EdgeType.COUSIN)){
				newEdge = edgeSerializer.addEdge(this.engine.asAdmin().getTraversalSource(), out, in);
			} else {
				newEdge = edgeSerializer.addTreeEdge(this.engine.asAdmin().getTraversalSource(), out, in);
			}
		} catch (NoEdgeRuleFoundException e) {
			throw new AAIException("AAI_6129", e);
		}
		return newEdge;
	}

	/**
	 * Creates the edge
	 *
	 * @param type the edge type - COUSIN or TREE
	 * @param out the out
	 * @param in the in
	 * @return the edge
	 */
	protected Edge createEdgeIfPossible(EdgeType type, Vertex out, Vertex in) throws AAIException {
		Edge newEdge = null;
		try {
			if (type.equals(EdgeType.COUSIN)){
				newEdge = edgeSerializer.addEdgeIfPossible(this.engine.asAdmin().getTraversalSource(), out, in);
			} else {
				newEdge = edgeSerializer.addTreeEdgeIfPossible(this.engine.asAdmin().getTraversalSource(), out, in);
			}
		} catch (NoEdgeRuleFoundException e) {
			throw new AAIException("AAI_6129", e);
		}
		return newEdge;
	}

	/**
	 * Creates the edge
	 *
	 * @param type the edge type - COUSIN or TREE
	 * @param out the out
	 * @param in the in
	 * @return the edge
	 */
	protected Edge createPrivateEdge(Vertex out, Vertex in) throws AAIException {
		Edge newEdge = null;
		try {
			newEdge = edgeSerializer.addPrivateEdge(this.engine.asAdmin().getTraversalSource(), out, in, null);
		} catch (EdgeRuleNotFoundException | AmbiguousRuleChoiceException e) {
			throw new AAIException("AAI_6129", e);
		}
		return newEdge;
	}

	/**
	 * Creates the TREE edge
	 *
	 * @param out the out
	 * @param in the in
	 * @return the edge
	 */
	protected Edge createTreeEdge(Vertex out, Vertex in) throws AAIException {
		Edge newEdge = createEdge(EdgeType.TREE, out, in);
		return newEdge;
	}

	/**
	 * Creates the COUSIN edge
	 *
	 * @param out the out
	 * @param in the in
	 * @return the edge
	 */
	protected Edge createCousinEdge(Vertex out, Vertex in) throws AAIException {
		Edge newEdge = createEdge(EdgeType.COUSIN, out, in);
		return newEdge;
	}

	private void initDBSerializer() {
		SchemaVersion version = schemaVersions.getDefaultVersion();
		ModelType introspectorFactoryType = ModelType.MOXY;
		loader = loaderFactory.createLoaderForVersion(introspectorFactoryType, version);
		try {
			this.serializer = new DBSerializer(version, this.engine, introspectorFactoryType, this.getMigrationName());
		} catch (AAIException e) {
			throw new RuntimeException("could not create seralizer", e);
		}
	}

	/**
	 * These are the node types you would like your traversal to process
	 * @return
	 */
	public abstract Optional<String[]> getAffectedNodeTypes();

	/**
	 * used as the "fromAppId" when modifying vertices
	 * @return
	 */
	public abstract String getMigrationName();

	/**
	 * updates all internal vertex properties
	 * @param v
	 * @param isNewVertex
	 */
	protected void touchVertexProperties(Vertex v, boolean isNewVertex) {
		this.serializer.touchStandardVertexProperties(v, isNewVertex);
	}

	public NotificationHelper getNotificationHelper() {
		return this.notificationHelper;
	}

	/**
	 * Log and print.
	 *
	 * @param logger the logger
	 * @param msg the msg
	 */
	protected void logAndPrint(Logger logger, String msg) {
		System.out.println(msg);
		logger.info(msg);
	}
}
