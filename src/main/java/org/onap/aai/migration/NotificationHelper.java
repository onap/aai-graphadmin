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

import java.io.UnsupportedEncodingException;
import java.net.URI;
import java.util.HashMap;
import java.util.List;

import jakarta.ws.rs.core.Response.Status;

import org.apache.tinkerpop.gremlin.structure.Vertex;
import org.onap.aai.exceptions.AAIException;
import org.onap.aai.introspection.Introspector;
import org.onap.aai.introspection.Loader;
import org.onap.aai.introspection.LoaderFactory;
import org.onap.aai.introspection.exceptions.AAIUnknownObjectException;
import org.onap.aai.kafka.NotificationProducer;
import org.onap.aai.rest.notification.UEBNotification;
import org.onap.aai.serialization.db.DBSerializer;
import org.onap.aai.serialization.engines.TransactionalGraphEngine;
import org.onap.aai.serialization.engines.query.QueryEngine;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.slf4j.MDC;
import org.onap.aai.setup.SchemaVersions;

/**
 * Allows for DMaaP notifications from Migrations
 */
public class NotificationHelper {

	protected Logger LOGGER = null;
	protected final DBSerializer serializer;
	protected final Loader loader;
	protected final TransactionalGraphEngine engine;
	protected final String transactionId;
	protected final String sourceOfTruth;
	protected final UEBNotification	notification;
	protected final NotificationProducer notificationProducer;

	public NotificationHelper(NotificationProducer notificationProducer, Loader loader, DBSerializer serializer, LoaderFactory loaderFactory, SchemaVersions schemaVersions, TransactionalGraphEngine engine, String transactionId, String sourceOfTruth) {
		this.loader = loader;
		this.serializer = serializer;
		this.engine = engine;
		this.transactionId = transactionId;
		this.sourceOfTruth = sourceOfTruth;
		this.notification = new UEBNotification(loaderFactory, schemaVersions);
		this.notificationProducer = notificationProducer;
		MDC.put("logFilenameAppender", this.getClass().getSimpleName());
		LOGGER = LoggerFactory.getLogger(this.getClass().getSimpleName());

	}

	public void addEvent(Vertex v, Introspector obj, EventAction action, URI uri, String basePath) throws UnsupportedEncodingException, AAIException {
		HashMap<String, Introspector> relatedObjects = new HashMap<>();
		Status status = mapAction(action);

		if (!obj.isTopLevel()) {
			relatedObjects = this.getRelatedObjects(serializer, engine.getQueryEngine(), v);
		}
		notification.createNotificationEvent(transactionId, sourceOfTruth, status, uri, obj, relatedObjects, basePath);

	}

	public void addDeleteEvent(String transactionId, String sourceOfTruth, EventAction action, URI uri, Introspector obj, HashMap relatedObjects,String basePath) throws UnsupportedEncodingException, AAIException {
		Status status = mapAction(action);
		notification.createNotificationEvent(transactionId, sourceOfTruth, status, uri, obj, relatedObjects, basePath);

	}

	private HashMap<String, Introspector> getRelatedObjects(DBSerializer serializer, QueryEngine queryEngine, Vertex v) throws AAIException {
		HashMap<String, Introspector> relatedVertices = new HashMap<>();
		List<Vertex> vertexChain = queryEngine.findParents(v);
		for (Vertex vertex : vertexChain) {
			try {
				final Introspector vertexObj = serializer.getVertexProperties(vertex);
				relatedVertices.put(vertexObj.getObjectId(), vertexObj);
			} catch (AAIUnknownObjectException | UnsupportedEncodingException e) {
				LOGGER.warn("Unable to get vertex properties, partial list of related vertices returned");
			}

		}

		return relatedVertices;
	}

	private Status mapAction(EventAction action) {
		if (EventAction.CREATE.equals(action)) {
			return Status.CREATED;
		} else if (EventAction.UPDATE.equals(action)) {
			return Status.OK;
		} else if (EventAction.DELETE.equals(action)) {
			return Status.NO_CONTENT;
		} else {
			return Status.OK;
		}
	}

	public void triggerEvents() throws AAIException {
		notificationProducer.sendUEBNotification(notification);
	}

	public UEBNotification getNotifications() {
		return this.notification;
	}
}
