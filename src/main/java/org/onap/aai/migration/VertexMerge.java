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
import java.util.Collection;
import java.util.Collections;
import java.util.HashMap;
import java.util.HashSet;
import java.util.Iterator;
import java.util.Map;
import java.util.Optional;
import java.util.Set;

import org.apache.tinkerpop.gremlin.process.traversal.dsl.graph.GraphTraversalSource;
import org.apache.tinkerpop.gremlin.structure.Vertex;

import org.onap.aai.db.props.AAIProperties;
import org.onap.aai.exceptions.AAIException;
import org.onap.aai.introspection.Introspector;
import org.onap.aai.introspection.Loader;
import org.onap.aai.introspection.exceptions.AAIUnknownObjectException;
import org.onap.aai.serialization.db.DBSerializer;
import org.onap.aai.edges.enums.EdgeType;
import org.onap.aai.serialization.db.EdgeSerializer;
import org.onap.aai.serialization.engines.TransactionalGraphEngine;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * This class recursively merges two vertices passed in.
 * <br>
 * You can start with any two vertices, but after the vertices are merged based off the equality of their keys
 *
 */
public class VertexMerge {

	private final Logger logger = LoggerFactory.getLogger(this.getClass().getSimpleName());

	private final GraphTraversalSource g;
	private final TransactionalGraphEngine engine;
	private final DBSerializer serializer;
	private final EdgeSerializer edgeSerializer;
	private final Loader loader;
	private final NotificationHelper notificationHelper;
	private final boolean hasNotifications;

	private VertexMerge(Builder builder) {
		this.engine = builder.getEngine();
		this.serializer = builder.getSerializer();
		this.g = engine.asAdmin().getTraversalSource();
		this.edgeSerializer = builder.getEdgeSerializer();
		this.loader = builder.getLoader();
		this.notificationHelper = builder.getHelper();
		this.hasNotifications = builder.isHasNotifications();
	}
	
	/**
	 * Merges vertices. forceCopy is a map of the form [{aai-node-type}:{set of properties}]
	 * @param primary
	 * @param secondary
	 * @param forceCopy
	 * @throws AAIException
	 * @throws UnsupportedEncodingException
	 */
	public void performMerge(Vertex primary, Vertex secondary, Map<String, Set<String>> forceCopy, String basePath) throws AAIException, UnsupportedEncodingException {
		final Optional<Introspector> secondarySnapshot;
		if (this.hasNotifications) {
			secondarySnapshot = Optional.of(serializer.getLatestVersionView(secondary));
		} else {
			secondarySnapshot = Optional.empty();
		}
		mergeProperties(primary, secondary, forceCopy);
		
		Collection<Vertex> secondaryChildren = this.engine.getQueryEngine().findChildren(secondary);
		Collection<Vertex> primaryChildren = this.engine.getQueryEngine().findChildren(primary);
		
		mergeChildren(primary, secondary, primaryChildren, secondaryChildren, forceCopy);
		
		Collection<Vertex> secondaryCousins = this.engine.getQueryEngine().findCousinVertices(secondary);
		Collection<Vertex> primaryCousins = this.engine.getQueryEngine().findCousinVertices(primary);
		
		secondaryCousins.removeAll(primaryCousins);
		logger.debug("removing vertex after merge: " + secondary );
		if (this.hasNotifications && secondarySnapshot.isPresent()) {
			this.notificationHelper.addEvent(secondary, secondarySnapshot.get(), EventAction.DELETE, this.serializer.getURIForVertex(secondary, false), basePath);
		}
		secondary.remove();
		for (Vertex v : secondaryCousins) {
			this.edgeSerializer.addEdgeIfPossible(g, v, primary);
		}
		if (this.hasNotifications) {
			final Introspector primarySnapshot = serializer.getLatestVersionView(primary);
			this.notificationHelper.addEvent(primary, primarySnapshot, EventAction.UPDATE, this.serializer.getURIForVertex(primary, false), basePath);
		}
	}
	
	/**
	 * This method may go away if we choose to event on each modification performed
	 * @param primary
	 * @param secondary
	 * @param forceCopy
	 * @throws AAIException
	 * @throws UnsupportedEncodingException
	 */
	protected void performMergeHelper(Vertex primary, Vertex secondary, Map<String, Set<String>> forceCopy) throws AAIException, UnsupportedEncodingException {
		mergeProperties(primary, secondary, forceCopy);
		
		Collection<Vertex> secondaryChildren = this.engine.getQueryEngine().findChildren(secondary);
		Collection<Vertex> primaryChildren = this.engine.getQueryEngine().findChildren(primary);
		
		mergeChildren(primary, secondary, primaryChildren, secondaryChildren, forceCopy);
		
		Collection<Vertex> secondaryCousins = this.engine.getQueryEngine().findCousinVertices(secondary);
		Collection<Vertex> primaryCousins = this.engine.getQueryEngine().findCousinVertices(primary);
		
		secondaryCousins.removeAll(primaryCousins);
		secondary.remove();
		for (Vertex v : secondaryCousins) {
			this.edgeSerializer.addEdgeIfPossible(g, v, primary);
		}
	}
	
	private String getURI(Vertex v) throws UnsupportedEncodingException, AAIException {
		Introspector obj = loader.introspectorFromName(v.<String>property(AAIProperties.NODE_TYPE).orElse(""));
		this.serializer.dbToObject(Collections.singletonList(v), obj, 0, true, "false");
		return obj.getURI();

	}
	private void mergeChildren(Vertex primary, Vertex secondary, Collection<Vertex> primaryChildren, Collection<Vertex> secondaryChildren, Map<String, Set<String>> forceCopy) throws UnsupportedEncodingException, AAIException {
		Map<String, Vertex> primaryMap = uriMap(primaryChildren);
		Map<String, Vertex> secondaryMap = uriMap(secondaryChildren);
		Set<String> primaryKeys = new HashSet<>(primaryMap.keySet());
		Set<String> secondaryKeys = new HashSet<>(secondaryMap.keySet());
		primaryKeys.retainAll(secondaryKeys);
		final Set<String> mergeItems = new HashSet<>(primaryKeys);
		primaryKeys = new HashSet<>(primaryMap.keySet());
		secondaryKeys = new HashSet<>(secondaryMap.keySet());
		secondaryKeys.removeAll(primaryKeys);
		final Set<String> copyItems = new HashSet<>(secondaryKeys);
		
		for (String key : mergeItems) {
			this.performMergeHelper(primaryMap.get(key), secondaryMap.get(key), forceCopy);
		}
		
		for (String key : copyItems) {
			this.edgeSerializer.addTreeEdgeIfPossible(g, secondaryMap.get(key), primary);
			this.serializer.getEdgeBetween(EdgeType.TREE, secondary, secondaryMap.get(key)).remove();
		}

	}
	
	private Map<String, Vertex> uriMap(Collection<Vertex> vertices) throws UnsupportedEncodingException, AAIException {
		final Map<String, Vertex> result = new HashMap<>();
		for (Vertex v : vertices) {
			result.put(getURI(v), v);
		}
		return result;
	}
	
	private void mergeProperties(Vertex primary, Vertex secondary, Map<String, Set<String>> forceCopy) throws AAIUnknownObjectException {
		final String primaryType = primary.<String>property(AAIProperties.NODE_TYPE).orElse("");
		final String secondaryType = secondary.<String>property(AAIProperties.NODE_TYPE).orElse("");

		final Introspector secondaryObj = loader.introspectorFromName(secondaryType);
		secondary.properties().forEachRemaining(prop -> {
			if (!primary.property(prop.key()).isPresent() || forceCopy.getOrDefault(primaryType, new HashSet<String>()).contains(prop.key())) {
				primary.property(prop.key(), prop.value());
			}
			if (primary.property(prop.key()).isPresent() && secondary.property(prop.key()).isPresent() && secondaryObj.isListType(prop.key())) {
				mergeCollection(primary, prop.key(), secondary.values(prop.key()));
			}
		});
	}
	private void mergeCollection(Vertex primary, String propName, Iterator<Object> secondaryValues) {
		secondaryValues.forEachRemaining(item -> {
			primary.property(propName, item);
		});
	}
	
	
	public static class Builder {
		private final TransactionalGraphEngine engine;

		private final DBSerializer serializer;
		private EdgeSerializer edgeSerializer;

		private final Loader loader;
		private NotificationHelper helper = null;
		private boolean hasNotifications = false;
		public Builder(Loader loader, TransactionalGraphEngine engine, DBSerializer serializer) {
			this.loader = loader;
			this.engine = engine;
			this.serializer = serializer;
		}
		
		public Builder addNotifications(NotificationHelper helper) {
			this.helper = helper;
			this.hasNotifications = true;
			return this;
		}

		public Builder edgeSerializer(EdgeSerializer edgeSerializer){
			this.edgeSerializer = edgeSerializer;
			return this;
		}

		public EdgeSerializer getEdgeSerializer(){
			return edgeSerializer;
		}

		public VertexMerge build() {
			return new VertexMerge(this);
		}
		
		protected TransactionalGraphEngine getEngine() {
			return engine;
		}

		protected DBSerializer getSerializer() {
			return serializer;
		}

		protected Loader getLoader() {
			return loader;
		}

		protected NotificationHelper getHelper() {
			return helper;
		}

		protected boolean isHasNotifications() {
			return hasNotifications;
		}
		
	}
	
}
