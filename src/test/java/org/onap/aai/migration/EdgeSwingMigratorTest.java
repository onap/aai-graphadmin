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

import org.apache.tinkerpop.gremlin.process.traversal.dsl.graph.GraphTraversal;
import org.apache.tinkerpop.gremlin.process.traversal.dsl.graph.GraphTraversalSource;
import org.apache.tinkerpop.gremlin.structure.Direction;
import org.apache.tinkerpop.gremlin.structure.Edge;
import org.apache.tinkerpop.gremlin.structure.Property;
import org.apache.tinkerpop.gremlin.structure.Vertex;
import org.janusgraph.core.schema.JanusGraphManagement;
import org.javatuples.Pair;
import org.junit.Before;
import org.junit.Test;
import org.springframework.test.annotation.DirtiesContext;
import org.mockito.Mockito;
import org.onap.aai.AAISetup;
import org.onap.aai.edges.EdgeIngestor;
import org.onap.aai.exceptions.AAIException;
import org.onap.aai.introspection.Loader;
import org.onap.aai.introspection.LoaderFactory;
import org.onap.aai.introspection.ModelType;
import org.onap.aai.serialization.db.EdgeSerializer;
import org.onap.aai.serialization.engines.JanusGraphDBEngine;
import org.onap.aai.serialization.engines.QueryStyle;
import org.onap.aai.serialization.engines.TransactionalGraphEngine;
import org.onap.aai.setup.SchemaVersions;
import org.springframework.test.annotation.DirtiesContext;

import java.util.*;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertTrue;
import static org.mockito.Mockito.spy;
import static org.mockito.Mockito.when;

@DirtiesContext(classMode = DirtiesContext.ClassMode.BEFORE_CLASS)
public class EdgeSwingMigratorTest extends AAISetup {
	
	private final static ModelType introspectorFactoryType = ModelType.MOXY;
	private final static QueryStyle queryStyle = QueryStyle.TRAVERSAL;
	private Loader loader;
	private TransactionalGraphEngine dbEngine;
	private GraphTraversalSource g;
	private MockEdgeSwingMigrator migration;
	private Vertex modelVer1 = null;
	private Vertex modelVer3 = null;
	
	
	@Before
	public void setUp() throws Exception {
		JanusGraphManagement janusgraphManagement = graph.openManagement();
		g = graph.traversal();
		loader = loaderFactory.createLoaderForVersion(introspectorFactoryType, schemaVersions.getDefaultVersion());
		dbEngine = new JanusGraphDBEngine(
				queryStyle,
				loader);
		createFirstVertexAndRelatedVertexes();
		TransactionalGraphEngine spy = spy(dbEngine);
		TransactionalGraphEngine.Admin adminSpy = spy(dbEngine.asAdmin());
		GraphTraversalSource traversal = g;
		when(spy.asAdmin()).thenReturn(adminSpy);
		when(adminSpy.getTraversalSource()).thenReturn(traversal);
		Mockito.doReturn(janusgraphManagement).when(adminSpy).getManagementSystem();
		
		
		migration = new MockEdgeSwingMigrator(spy, loaderFactory, edgeIngestor, edgeSerializer, schemaVersions);
		migration.run();
	}
	
	private void createFirstVertexAndRelatedVertexes() throws AAIException {
		Vertex model1 = g.addV().property("aai-node-type", "model")
				.property("model-invariant-id", "model-invariant-id-1")
				.property("model-type", "widget")
				.next();
		modelVer1 = g.addV().property("aai-node-type", "model-ver")
				.property("model-version-id", "model-version-id-1")
				.property("model-name", "connector")
				.property("model-version", "v1.0")
				.next();
		edgeSerializer.addTreeEdge(g, model1, modelVer1);
		
		//Create the cousin vertex - modelElement2 which will point to modelVer1
		Vertex model2 = g.addV().property("aai-node-type", "model")
				.property("model-invariant-id", "model-invariant-id-2")
				.property("model-type", "resource")
				.next();
		Vertex modelVer2 = g.addV().property("aai-node-type", "model-ver")
				.property("model-version-id", "model-version-id-2")
				.property("model-name", "resourceModTestVer")
				.property("model-version", "v1.0")
				.next();
		edgeSerializer.addTreeEdge(g, model2, modelVer2);
		Vertex modelElement2 = g.addV().property("aai-node-type", "model-element")
				.property("model-element-uuid", "model-element-uuid-2")
				.property("new-data-del-flag", "T")
				.property("cardinality", "unbounded")
				.next();
		edgeSerializer.addTreeEdge(g, modelVer2, modelElement2);
		edgeSerializer.addEdge(g, modelVer1, modelElement2);
		
		Vertex model3 = g.addV().property("aai-node-type", "model")
				.property("model-invariant-id", "model-invariant-id-3")
				.property("model-type", "widget")
				.next();
		modelVer3 = g.addV().property("aai-node-type", "model-ver")
				.property("model-version-id", "model-version-id-3")
				.property("model-name", "connector")
				.property("model-version", "v1.0")
				.next();
		edgeSerializer.addTreeEdge(g, model3, modelVer3);
	}
	
	class MockEdgeSwingMigrator extends EdgeSwingMigrator {
		
		public MockEdgeSwingMigrator(TransactionalGraphEngine engine, LoaderFactory loaderFactory, EdgeIngestor edgeIngestor, EdgeSerializer edgeSerializer, SchemaVersions schemaVersions) {
			super(engine, loaderFactory, edgeIngestor, edgeSerializer, schemaVersions);
		}

		@Override
		public List<Pair<Vertex, Vertex>> getAffectedNodePairs() {
			List<Pair<Vertex, Vertex>> fromToVertPairList = new ArrayList<Pair<Vertex, Vertex>>();
			Vertex fromVert = modelVer1;
			Vertex toVert = modelVer3;
			fromToVertPairList.add(new Pair<>(fromVert, toVert));
			return fromToVertPairList;
		}
		
		public String getNodeTypeRestriction(){
			return "model-element";
		}

		public String getEdgeLabelRestriction(){
			return "org.onap.relationships.inventory.IsA";
		}
				
		public String getEdgeDirRestriction(){
			return "IN";
		}

		@Override
		public void cleanupAsAppropriate(List<Pair<Vertex, Vertex>> nodePairL) {
			// For the scenario we're testing, we would define this to remove the model-ver that
			// we moved off of, and also remove its parent model since it was a widget model and 
			// these are currently one-to-one (model-ver to model).
			//
			// But what gets cleaned up (if anything) after a node's edges are migrated will vary depending 
			// on what the edgeSwingMigration is being used for.
			

		}

		@Override
		public Optional<String[]> getAffectedNodeTypes() {
			return Optional.of(new String[]{"model", "model-element", "model-ver"});
		}

		@Override
		public String getMigrationName() {
			return "MockEdgeSwingMigrator";
		}
	}

	@Test
	public void testBelongsToEdgesStillThere() {
		assertEquals(true, g.V().has("aai-node-type", "model-ver").has("model-version-id", "model-version-id-1")
				.out("org.onap.relationships.inventory.BelongsTo").has("model-invariant-id", "model-invariant-id-1").hasNext());
		assertEquals(true, g.V().has("aai-node-type", "model-ver").has("model-version-id", "model-version-id-3")
				.out("org.onap.relationships.inventory.BelongsTo").has("model-invariant-id", "model-invariant-id-3").hasNext());
		assertEquals(true, g.V().has("aai-node-type", "model-element").has("model-element-uuid", "model-element-uuid-2")
				.out("org.onap.relationships.inventory.BelongsTo").has("model-version-id", "model-version-id-2").hasNext());
	}
	
	@Test
	public void testThatNewEdgeAdded() {
		assertEquals(true, g.V().has("aai-node-type", "model-ver").has("model-version-id", "model-version-id-3")
				.in("org.onap.relationships.inventory.IsA").has("model-element-uuid", "model-element-uuid-2").hasNext());
	}
	
	@Test
	public void testThatNewEdgeHasAaiUuidAndDelProperties() {
		boolean haveUuidProp = false;
		boolean haveDelOtherVProp = false;
		GraphTraversal<Vertex, Vertex> modVerTrav = g.V().has("aai-node-type", "model-ver").has("model-version-id", "model-version-id-3");
		while (modVerTrav.hasNext()) {
         	Vertex modVerVtx = modVerTrav.next();
         	Iterator <Edge> edgeIter = modVerVtx.edges(Direction.IN, "org.onap.relationships.inventory.IsA");
         	while( edgeIter.hasNext() ){
         		Edge oldOutE = edgeIter.next();
         		
         		Iterator <Property<Object>> propsIter2 = oldOutE.properties();
				HashMap<String, String> propMap2 = new HashMap<String,String>();
				while( propsIter2.hasNext() ){
					Property <Object> ep2 = propsIter2.next();
					if( ep2.key().equals("aai-uuid") ){
						haveUuidProp = true;
					}
					else if( ep2.key().equals("delete-other-v") ){
						haveDelOtherVProp = true;
					}
				}
         	}
		}
			
		assertTrue("New IsA edge has aai-uuid property ", haveUuidProp );
		assertTrue("New IsA edge has delete-other-v property ", haveDelOtherVProp );
	}
		
		
	@Test
	public void testThatOldEdgeGone() {
		assertEquals(false, g.V().has("aai-node-type", "model-ver").has("model-version-id", "model-version-id-1")
				.in("org.onap.relationships.inventory.IsA").has("model-element-uuid", "model-element-uuid-2").hasNext());
	}
	
	
}
