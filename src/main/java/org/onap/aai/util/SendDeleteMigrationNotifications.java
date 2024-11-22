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


import org.onap.aai.config.SpringContextAware;
import org.onap.aai.dbmap.AAIGraph;
import org.onap.aai.exceptions.AAIException;
import org.onap.aai.introspection.Introspector;
import org.onap.aai.introspection.Loader;
import org.onap.aai.introspection.LoaderFactory;
import org.onap.aai.introspection.ModelType;
import org.onap.aai.kafka.NotificationProducer;
import org.onap.aai.migration.EventAction;
import org.onap.aai.migration.NotificationHelper;
import org.onap.aai.serialization.db.DBSerializer;
import org.onap.aai.serialization.engines.JanusGraphDBEngine;
import org.onap.aai.serialization.engines.QueryStyle;
import org.onap.aai.serialization.engines.TransactionalGraphEngine;
import org.onap.aai.setup.SchemaVersion;
import org.onap.aai.setup.SchemaVersions;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.slf4j.MDC;

import java.io.IOException;
import java.net.URI;
import java.nio.file.Files;
import java.nio.file.Paths;
import java.util.*;
import java.util.concurrent.atomic.AtomicInteger;

public class SendDeleteMigrationNotifications {

	protected Logger logger = LoggerFactory.getLogger(SendDeleteMigrationNotifications.class.getSimpleName());

	private String config;
	private String path;
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

	public SendDeleteMigrationNotifications(LoaderFactory loaderFactory, SchemaVersions schemaVersions, String config, String path, int sleepInMilliSecs, int numToBatch, String requestId, EventAction eventAction, String eventSource) {
		System.setProperty("aai.service.name", SendDeleteMigrationNotifications.class.getSimpleName());

		MDC.put("logFilenameAppender", SendDeleteMigrationNotifications.class.getSimpleName());

		this.config = config;
		this.path = path;
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

	public void process(String basePath) {

		try {
			Map<Integer, String> deleteDataMap = processFile();
			int count = 0;
			for (Map.Entry<Integer, String> entry : deleteDataMap.entrySet()) {
				logger.debug("Processing " + entry.getKey() + " :: Data :: " + entry.getValue());
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
					logger.debug("Triggered " + entry.getKey());
					count = 0;
					Thread.sleep(this.sleepInMilliSecs);
				}
			}
			if (count > 0) {
				trigger();
			}
			cleanup();
		} catch (Exception e) {
			logger.warn("Exception caught during SendDeleteMigrationNotifications.process()", e);
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
		this.engine = new JanusGraphDBEngine(queryStyle, loader);
		try {
			this.serializer = new DBSerializer(version, this.engine, introspectorFactoryType, this.eventSource);
		} catch (AAIException e) {
			throw new RuntimeException("could not create serializer", e);
		}
		NotificationProducer notificationProducer = SpringContextAware.getBean(NotificationProducer.class);
		this.notificationHelper = new NotificationHelper(notificationProducer, loader, serializer, loaderFactory, schemaVersions, engine, requestId, this.eventSource);
	}

	protected void initGraph() {
		System.setProperty("realtime.db.config", this.config);
		logAndPrint("\n\n---------- Connecting to Graph ----------");
		AAIGraph.getInstance();
		logAndPrint("---------- Connection Established ----------");
	}

	protected void logAndPrint(String msg) {
		System.out.println(msg);
		logger.debug(msg);
	}


}
