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
package org.onap.aai.migration.v12;

import java.nio.charset.UnsupportedCharsetException;
import org.apache.tinkerpop.gremlin.process.traversal.dsl.graph.GraphTraversalSource;
import org.apache.tinkerpop.gremlin.process.traversal.dsl.graph.__;
import org.apache.tinkerpop.gremlin.structure.Vertex;
import org.onap.aai.db.props.AAIProperties;
import org.onap.aai.edges.EdgeIngestor;
import org.onap.aai.edges.enums.EdgeProperty;
import org.onap.aai.exceptions.AAIException;
import org.onap.aai.introspection.LoaderFactory;
import org.onap.aai.introspection.ModelType;
import org.onap.aai.serialization.db.EdgeSerializer;
import org.onap.aai.introspection.exceptions.AAIUnknownObjectException;
import org.onap.aai.migration.*;
import org.onap.aai.edges.enums.AAIDirection;
import org.onap.aai.serialization.db.DBSerializer;
import org.onap.aai.serialization.engines.TransactionalGraphEngine;
import org.onap.aai.setup.SchemaVersion;
import org.onap.aai.setup.SchemaVersions;
import org.springframework.web.util.UriUtils;

import javax.ws.rs.core.UriBuilder;
import java.io.UnsupportedEncodingException;
import java.util.*;
import java.util.stream.Collectors;

//@Enabled

@MigrationPriority(1000)
@MigrationDangerRating(1000)
public class UriMigration extends Migrator {

	private final SchemaVersion version;
	private final ModelType introspectorFactoryType;
	private GraphTraversalSource g;

	private Map<String, UriBuilder> nodeTypeToUri;
	private Map<String, Set<String>> nodeTypeToKeys;

	protected Set<Object> seen = new HashSet<>();

	/**
	 * Instantiates a new migrator.
	 *
	 * @param engine
	 */
	public UriMigration(TransactionalGraphEngine engine, LoaderFactory loaderFactory, EdgeIngestor edgeIngestor, EdgeSerializer edgeSerializer, SchemaVersions schemaVersions) throws AAIException {
		super(engine, loaderFactory, edgeIngestor, edgeSerializer, schemaVersions);
		version = schemaVersions.getDefaultVersion();
		introspectorFactoryType = ModelType.MOXY;
		loader = loaderFactory.createLoaderForVersion(introspectorFactoryType, version);
		g = this.engine.asAdmin().getTraversalSource();
		this.serializer = new DBSerializer(version, this.engine, introspectorFactoryType, this.getMigrationName());

	}

	@Override
	public void run() {
		long start = System.currentTimeMillis();
		nodeTypeToUri = loader.getAllObjects().entrySet().stream().filter(e -> e.getValue().getGenericURI().contains("{")).collect(
				Collectors.toMap(
						e -> e.getKey(),
						e -> UriBuilder.fromPath(e.getValue().getFullGenericURI().replaceAll("\\{"+ e.getKey() + "-", "{"))
				));

		nodeTypeToKeys = loader.getAllObjects().entrySet().stream().filter(e -> e.getValue().getGenericURI().contains("{")).collect(
				Collectors.toMap(
						e -> e.getKey(),
						e -> e.getValue().getKeys()
				));

		Set<String> topLevelNodeTypes = loader.getAllObjects().entrySet().stream()
				.filter(e -> e.getValue().isTopLevel()).map(Map.Entry::getKey)
				.collect(Collectors.toSet());

		logger.info("Top level count : " + topLevelNodeTypes.size());
		topLevelNodeTypes.stream().forEach(topLevelNodeType -> {
			Set<Vertex> parentSet = g.V().has(AAIProperties.NODE_TYPE, topLevelNodeType).toSet();
			logger.info(topLevelNodeType + " : " + parentSet.size());
			try {
				this.verifyOrAddUri("", parentSet);
			} catch (AAIUnknownObjectException e) {
				e.printStackTrace();
			} catch (AAIException e) {
				e.printStackTrace();
			} catch (UnsupportedEncodingException e) {
				e.printStackTrace();
			}
		});
		logger.info("RUNTIME: " + (System.currentTimeMillis() - start));
		logger.info("NO URI: " + g.V().hasNot(AAIProperties.AAI_URI).count().next());
		logger.info("NUM VERTEXES SEEN: " + seen.size());
		seen = new HashSet<>();

	}

	protected void verifyOrAddUri(String parentUri, Set<Vertex> vertexSet) throws UnsupportedEncodingException, AAIException {
		String correctUri;
		for (Vertex v : vertexSet) {
			seen.add(v.id());
			//if there is an issue generating the uri catch, log and move on;
			try {
				correctUri = parentUri + this.getUriForVertex(v);
			} catch (Exception e) {
				logger.error("Vertex has issue generating uri " + e.getMessage() + "\n\t" + this.asString(v));
				continue;
			}
			try {
				v.property(AAIProperties.AAI_URI, correctUri);
			} catch (Exception e) {
				logger.info(e.getMessage() + "\n\t" + this.asString(v));
			}
			if (!v.property(AAIProperties.AAI_UUID).isPresent()) {
				v.property(AAIProperties.AAI_UUID, UUID.randomUUID().toString());
			}
			this.verifyOrAddUri(correctUri, getChildren(v));
		}
	}

	protected Set<Vertex> getChildren(Vertex v) {

		Set<Vertex> children = g.V(v).bothE().not(__.has(EdgeProperty.CONTAINS.toString(), AAIDirection.NONE.toString())).otherV().toSet();

		return children.stream().filter(child -> !seen.contains(child.id())).collect(Collectors.toSet());
	}

	protected String getUriForVertex(Vertex v) {
		String aaiNodeType = v.property(AAIProperties.NODE_TYPE).value().toString();


		Map<String, String> parameters = this.nodeTypeToKeys.get(aaiNodeType).stream().collect(Collectors.toMap(
				key -> key,
				key -> encodeProp(v.property(key).value().toString())
		));

		return this.nodeTypeToUri.get(aaiNodeType).buildFromEncodedMap(parameters).toString();
	}

	private static String encodeProp(String s) {
		try {
			return UriUtils.encode(s, "UTF-8");
		} catch (UnsupportedCharsetException e) {
			return "";
		}
	}

	@Override
	public Status getStatus() {
		return Status.SUCCESS;
	}

	@Override
	public Optional<String[]> getAffectedNodeTypes() {
		return Optional.empty();
	}

	@Override
	public String getMigrationName() {
		return UriMigration.class.getSimpleName();
	}
}
