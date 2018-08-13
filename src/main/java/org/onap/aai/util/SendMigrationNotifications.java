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

public class SendMigrationNotifications {

	protected EELFLogger logger = EELFManager.getInstance().getLogger(SendMigrationNotifications.class.getSimpleName());

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

	public SendMigrationNotifications(LoaderFactory loaderFactory, SchemaVersions schemaVersions, String config, String path, Set<String> notifyOn, int sleepInMilliSecs, int numToBatch, String requestId, EventAction eventAction, String eventSource) {
		System.setProperty("aai.service.name", SendMigrationNotifications.class.getSimpleName());
		Properties props = System.getProperties();
		props.setProperty(Configuration.PROPERTY_LOGGING_FILE_NAME, "migration-logback.xml");
		props.setProperty(Configuration.PROPERTY_LOGGING_FILE_PATH, AAIConstants.AAI_HOME_ETC_APP_PROPERTIES);

		MDC.put("logFilenameAppender", SendMigrationNotifications.class.getSimpleName());

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

		Map<String, String> vertexIds = processFile();
		engine.startTransaction();
		GraphTraversalSource g = engine.asAdmin().getReadOnlyTraversalSource();
		List<Vertex> vertexes;
		URI uri;
		Vertex v;
		int count = 0;
		for (Map.Entry<String, String> entry : vertexIds.entrySet()) {
			vertexes = g.V(entry.getKey()).toList();
			if (vertexes == null || vertexes.isEmpty()) {
				logAndPrint("Vertex " + entry.getKey() + " no longer exists." );
				continue;
			} else if (vertexes.size() > 1) {
				logAndPrint("Vertex " + entry.getKey() + " query returned " + vertexes.size() + " vertexes." );
				continue;
			} else {
				logger.info("Processing " + entry.getKey() + "resource-version " + entry.getValue());
				v = vertexes.get(0);
				if (notifyOn.isEmpty() || notifyOn.contains(v.value(AAIProperties.NODE_TYPE).toString())) {
					if (entry.getValue().equals(v.value(AAIProperties.RESOURCE_VERSION).toString())) {
						Introspector introspector = serializer.getLatestVersionView(v);
						uri = this.serializer.getURIForVertex(v, false);
						this.notificationHelper.addEvent(v, introspector, eventAction, uri, basePath);
						count++;
						if (count >= this.numToBatch) {
							trigger();
							logger.info("Triggered " + entry.getKey());
							count = 0;
							Thread.sleep(this.sleepInMilliSecs);
						}
					}
				}
			}
		}

		if (count > 0) {
			trigger();
		}

		cleanup();
	}

	protected void trigger() throws AAIException {
		this.notificationHelper.triggerEvents();
	}

	private Map<String, String> processFile() throws IOException {
		List<String> lines = Files.readAllLines(Paths.get(path));
		final Map<String,String> vertexIds = new LinkedHashMap<>();
		lines.stream().forEach(line -> {
			if (line.contains("_")) {
				String[] splitLine = line.split("_");
				if (splitLine.length == 2) {
					vertexIds.put(splitLine[0], splitLine[1]);
				}
			}
		});
		return vertexIds;
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