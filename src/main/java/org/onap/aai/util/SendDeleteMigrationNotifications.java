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

import com.att.eelf.configuration.Configuration;
import com.att.eelf.configuration.EELFLogger;
import com.att.eelf.configuration.EELFManager;
import org.apache.tinkerpop.gremlin.process.traversal.dsl.graph.GraphTraversalSource;
import org.apache.tinkerpop.gremlin.structure.Vertex;
import org.onap.aai.db.props.AAIProperties;
import org.onap.aai.dbmap.AAIGraph;
import org.onap.aai.dbmap.DBConnectionType;
import org.onap.aai.exceptions.AAIException;
import org.onap.aai.introspection.*;
import org.onap.aai.migration.EventAction;
import org.onap.aai.migration.NotificationHelper;
import org.onap.aai.rest.ueb.UEBNotification;
import org.onap.aai.serialization.db.DBSerializer;
import org.onap.aai.serialization.engines.QueryStyle;
import org.onap.aai.serialization.engines.JanusGraphDBEngine;
import org.onap.aai.serialization.engines.TransactionalGraphEngine;
import org.onap.aai.setup.SchemaVersions;
import org.onap.aai.setup.SchemaVersion;
import org.slf4j.MDC;

import java.io.IOException;
import java.net.URI;
import java.nio.file.Files;
import java.nio.file.Paths;
import java.util.*;
import java.util.concurrent.atomic.AtomicInteger;

import javax.ws.rs.core.Response.Status;

public class SendDeleteMigrationNotifications {

	protected EELFLogger logger = EELFManager.getInstance().getLogger(SendDeleteMigrationNotifications.class.getSimpleName());

	private String config;
	private String path;
	private Set<String> notifyOn;
	long sleepInMilliSecs;
	int numToBatch;
	private String requestId;
	private EventAction eventAction;
	private String eventSource;

	protected QueryStyle queryStyle = QueryStyle.TRAVERSAL;
	protected ModelType introspectorFactoryType = ModelType.MOXY;
	protected Loader loader = null;
	protected TransactionalGraphEngine engine = null;
	protected NotificationHelper notificationHelper = null;
	protected DBSerializer serializer = null;
	protected final LoaderFactory loaderFactory;
	protected final SchemaVersions schemaVersions;
	protected final SchemaVersion version;
	
	public SendDeleteMigrationNotifications(LoaderFactory loaderFactory, SchemaVersions schemaVersions, String config, String path, Set<String> notifyOn, int sleepInMilliSecs, int numToBatch, String requestId, EventAction eventAction, String eventSource) {
		System.setProperty("aai.service.name", SendDeleteMigrationNotifications.class.getSimpleName());
		Properties props = System.getProperties();
		props.setProperty(Configuration.PROPERTY_LOGGING_FILE_NAME, "migration-logback.xml");
		props.setProperty(Configuration.PROPERTY_LOGGING_FILE_PATH, AAIConstants.AAI_HOME_ETC_APP_PROPERTIES);

		MDC.put("logFilenameAppender", SendDeleteMigrationNotifications.class.getSimpleName());

		this.config = config;
		this.path = path;
		this.notifyOn = notifyOn;
		this.sleepInMilliSecs = sleepInMilliSecs;
		this.numToBatch = numToBatch;
		this.requestId = requestId;
		this.eventAction = eventAction;
		this.eventSource = eventSource;
		this.loaderFactory = loaderFactory;
		this.schemaVersions = schemaVersions;
		this.version  = schemaVersions.getDefaultVersion();
		initGraph();

		initFields();
		

	}

	public void process(String basePath) throws Exception {

		try {
			Map<Integer, String> deleteDataMap = processFile();
			int count = 0;
			for (Map.Entry<Integer, String> entry : deleteDataMap.entrySet()) {
				logger.info("Processing " + entry.getKey() + " :: Data :: " + entry.getValue());
				String data = entry.getValue();
				Introspector obj = null;
				if (data.contains("#@#")) {
					String[] splitLine = data.split("#@#");
					if (splitLine.length == 3) {
						obj = loader.unmarshal(splitLine[0], splitLine[2]);
						this.notificationHelper.addDeleteEvent(UUID.randomUUID().toString(), splitLine[0], eventAction,
								URI.create(splitLine[1]), obj, new HashMap(), basePath);
					}
				}
				count++;
				if (count >= this.numToBatch) {
					trigger();
					logger.info("Triggered " + entry.getKey());
					count = 0;
					Thread.sleep(this.sleepInMilliSecs);
				}
			}
			if (count > 0) {
				trigger();
			}
			cleanup();
		} catch (Exception e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		}
	}

	protected void trigger() throws AAIException {
		this.notificationHelper.triggerEvents();
	}

	private Map<Integer,String> processFile() throws IOException {
		List<String> lines = Files.readAllLines(Paths.get(path));
		final Map<Integer,String> data = new LinkedHashMap<>();
		AtomicInteger counter = new AtomicInteger(0);
		lines.stream().forEach(line -> {
			if (line.contains("#@#")) {
				data.put(counter.incrementAndGet(), line);
			}
		});
		return data;
	}

	protected void cleanup() {
		logAndPrint("Events sent, closing graph connections");
		engine.rollback();
		AAIGraph.getInstance().graphShutdown();
		logAndPrint("---------- Done ----------");
	}

	private void initFields() {
		this.loader = loaderFactory.createLoaderForVersion(introspectorFactoryType, version);
		this.engine = new JanusGraphDBEngine(queryStyle, DBConnectionType.REALTIME, loader);
		try {
			this.serializer = new DBSerializer(version, this.engine, introspectorFactoryType, this.eventSource);
		} catch (AAIException e) {
			throw new RuntimeException("could not create serializer", e);
		}
		this.notificationHelper = new NotificationHelper(loader, serializer, loaderFactory, schemaVersions, engine, requestId, this.eventSource);
	}

	protected void initGraph() {
		System.setProperty("realtime.db.config", this.config);
		logAndPrint("\n\n---------- Connecting to Graph ----------");
		AAIGraph.getInstance();
		logAndPrint("---------- Connection Established ----------");
	}

	protected void logAndPrint(String msg) {
		System.out.println(msg);
		logger.info(msg);
	}


}