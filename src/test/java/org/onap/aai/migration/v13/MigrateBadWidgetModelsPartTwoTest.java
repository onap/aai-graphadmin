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
package org.onap.aai.migration.v13;

import static org.junit.Assert.*;
import static org.mockito.Mockito.spy;
import static org.mockito.Mockito.when;

import java.util.Optional;

import org.apache.tinkerpop.gremlin.process.traversal.dsl.graph.GraphTraversalSource;
import org.apache.tinkerpop.gremlin.structure.Vertex;
import org.junit.After;
import org.junit.Before;
import org.junit.Test;
import org.mockito.Mockito;
import org.onap.aai.AAISetup;
import org.onap.aai.dbmap.DBConnectionType;
import org.onap.aai.edges.EdgeIngestor;
import org.onap.aai.exceptions.AAIException;
import org.onap.aai.introspection.Loader;
import org.onap.aai.introspection.LoaderFactory;
import org.onap.aai.introspection.ModelType;
import org.onap.aai.serialization.db.EdgeSerializer;
import org.onap.aai.setup.SchemaVersions;
import org.onap.aai.setup.SchemaVersion;
import org.onap.aai.serialization.engines.QueryStyle;
import org.onap.aai.serialization.engines.JanusGraphDBEngine;
import org.onap.aai.serialization.engines.TransactionalGraphEngine;

import org.janusgraph.core.JanusGraphFactory;
import org.janusgraph.core.JanusGraph;
import org.janusgraph.core.JanusGraphTransaction;
import org.janusgraph.core.schema.JanusGraphManagement;

public class MigrateBadWidgetModelsPartTwoTest extends AAISetup {

	private final static ModelType introspectorFactoryType = ModelType.MOXY;
	private final static QueryStyle queryStyle = QueryStyle.TRAVERSAL;
	private final static DBConnectionType type = DBConnectionType.REALTIME;
	private Loader loader;
	private TransactionalGraphEngine dbEngine;
	private GraphTraversalSource g;
	private MockMigrateBadWidgetModelsPartTwo migration;
	private Vertex modelVer1 = null;
	private Vertex modelVer3 = null;

	@Before
	public void setUp() throws Exception {
		JanusGraphManagement janusgraphManagement = graph.openManagement();
		g = graph.traversal();
		loader = loaderFactory.createLoaderForVersion(introspectorFactoryType, schemaVersions.getDefaultVersion());
		dbEngine = new JanusGraphDBEngine(queryStyle, type, loader);
		createFirstVertexAndRelatedVertexes();
		TransactionalGraphEngine spy = spy(dbEngine);
		TransactionalGraphEngine.Admin adminSpy = spy(dbEngine.asAdmin());
		GraphTraversalSource traversal = g;
		when(spy.asAdmin()).thenReturn(adminSpy);
		when(adminSpy.getTraversalSource()).thenReturn(traversal);
		Mockito.doReturn(janusgraphManagement).when(adminSpy).getManagementSystem();

		migration = new MockMigrateBadWidgetModelsPartTwo(spy, loaderFactory, edgeIngestor, edgeSerializer, schemaVersions);
		migration.run();
	}

	private void createFirstVertexAndRelatedVertexes() throws AAIException {
		Vertex model1 = g.addV().property("aai-node-type", "model")
				.property("model-invariant-id", "model-invariant-id-1").property("model-type", "widget").next();
		modelVer1 = g.addV().property("aai-node-type", "model-ver").property("model-version-id", "model-version-id-1")
				.property("model-name", "connector").property("model-version", "v1.0").next();
		edgeSerializer.addTreeEdge(g, model1, modelVer1);

		// Create the cousin vertex - modelElement2 for modelVer1
		Vertex model2 = g.addV().property("aai-node-type", "model")
				.property("model-invariant-id", "model-invariant-id-2").property("model-type", "resource").next();
		Vertex modelVer2 = g.addV().property("aai-node-type", "model-ver")
				.property("model-version-id", "model-version-id-2").property("model-name", "resourceMod4Test")
				.property("model-version", "v1.0").next();
		edgeSerializer.addTreeEdge(g, model2, modelVer2);
		Vertex modelElement2 = g.addV().property("aai-node-type", "model-element")
				.property("model-element-uuid", "model-element-uuid-2").property("new-data-del-flag", "T")
				.property("cardinality", "unbounded").next();
		edgeSerializer.addTreeEdge(g, modelVer2, modelElement2);
		edgeSerializer.addEdge(g, modelVer1, modelElement2);

		// For model3/model-ver3 - we use valid invid/versionIds
		Vertex model3 = g.addV().property("aai-node-type", "model")
				.property("model-invariant-id", "4c01c948-7607-4d66-8a6c-99c2c2717936").property("model-type", "widget")
				.next();
		modelVer3 = g.addV().property("aai-node-type", "model-ver")
				.property("model-version-id", "22104c9f-29fd-462f-be07-96cd6b46dd33")
				.property("model-name", "connector").property("model-version", "v1.0").next();
		edgeSerializer.addTreeEdge(g, model3, modelVer3);
	}

	class MockMigrateBadWidgetModelsPartTwo extends MigrateBadWidgetModelsPartTwo {

		public MockMigrateBadWidgetModelsPartTwo(TransactionalGraphEngine engine, LoaderFactory loaderFactory, EdgeIngestor edgeIngestor, EdgeSerializer edgeSerializer, SchemaVersions schemaVersions) {
			super(engine, loaderFactory, edgeIngestor, edgeSerializer, schemaVersions);
		}

		@Override
		public Optional<String[]> getAffectedNodeTypes() {
			return Optional.of(new String[] { "model", "model-element", "model-ver" });
		}

		@Override
		public String getMigrationName() {
			return "MockMigrateNodesThatUseBadWidgetModelsPartTwo";
		}
	}


	@Test
	public void testBelongsToEdgesStillThereX() {
		assertEquals(true,
				g.V().has("aai-node-type", "model-ver").has("model-version-id", "22104c9f-29fd-462f-be07-96cd6b46dd33")
						.out("org.onap.relationships.inventory.BelongsTo")
						.has("model-invariant-id", "4c01c948-7607-4d66-8a6c-99c2c2717936").hasNext());
		assertEquals(true,
				g.V().has("aai-node-type", "model-element").has("model-element-uuid", "model-element-uuid-2")
						.out("org.onap.relationships.inventory.BelongsTo").has("model-version-id", "model-version-id-2")
						.hasNext());
	}
	
	@Test
	public void testBadVerNodeIsGoneX() {
		assertEquals(false,
				g.V().has("aai-node-type", "model-ver").has("model-version-id", "model-version-id-1").hasNext());
	}
	
	@Test
	public void testBadModelNodeIsGoneX() {
		assertEquals(false,
				g.V().has("aai-node-type", "model").has("model-invariant-id", "model-invariant-id-1").hasNext());
	}

	@Test
	public void testThatNewEdgeAddedX() {
		assertEquals(true,
				g.V().has("aai-node-type", "model-ver").has("model-version-id", "22104c9f-29fd-462f-be07-96cd6b46dd33")
						.in("org.onap.relationships.inventory.IsA").has("model-element-uuid", "model-element-uuid-2")
						.hasNext());
	}

	@Test
	public void testThatOldEdgeGone() {
		assertEquals(false,
				g.V().has("aai-node-type", "model-ver").has("model-version-id", "model-version-id-1")
						.in("org.onap.relationships.inventory.IsA").has("model-element-uuid", "model-element-uuid-2")
						.hasNext());
	}

}