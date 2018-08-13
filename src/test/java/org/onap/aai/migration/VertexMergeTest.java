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

import org.janusgraph.core.Cardinality;
import org.janusgraph.core.JanusGraphFactory;
import org.janusgraph.core.JanusGraph;
import org.janusgraph.core.schema.JanusGraphManagement;
import org.apache.tinkerpop.gremlin.process.traversal.dsl.graph.GraphTraversalSource;
import org.apache.tinkerpop.gremlin.structure.Graph;
import org.apache.tinkerpop.gremlin.structure.Vertex;
import org.junit.*;
import org.onap.aai.AAISetup;
import org.onap.aai.db.props.AAIProperties;
import org.onap.aai.dbmap.DBConnectionType;
import org.onap.aai.introspection.Loader;
import org.onap.aai.introspection.ModelType;
import org.onap.aai.setup.SchemaVersions;
import org.onap.aai.setup.SchemaVersion;
import org.onap.aai.serialization.db.DBSerializer;
import org.onap.aai.serialization.engines.QueryStyle;
import org.onap.aai.serialization.engines.JanusGraphDBEngine;
import org.onap.aai.serialization.engines.TransactionalGraphEngine;

import java.io.UnsupportedEncodingException;
import java.util.*;

import static org.hamcrest.collection.IsIterableContainingInAnyOrder.containsInAnyOrder;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertThat;
import static org.mockito.Mockito.spy;
import static org.mockito.Mockito.when;

@Ignore
public class VertexMergeTest extends AAISetup {
	
	
	private final static SchemaVersion version = new SchemaVersion("v10");
	private final static ModelType introspectorFactoryType = ModelType.MOXY;
	private final static QueryStyle queryStyle = QueryStyle.TRAVERSAL;
	private final static DBConnectionType type = DBConnectionType.REALTIME;

	private Loader loader;
	private TransactionalGraphEngine dbEngine;
	private JanusGraph graph;
	private GraphTraversalSource g;
	private Graph tx;

	@Before
	public void setUp() throws Exception {
		graph = JanusGraphFactory.build().set("storage.backend","inmemory").open();
		tx = graph.newTransaction();
		g = tx.traversal();
		loader = loaderFactory.createLoaderForVersion(introspectorFactoryType, version);
		dbEngine = new JanusGraphDBEngine(
				queryStyle,
				type,
				loader);

		JanusGraphManagement mgmt = graph.openManagement();
		mgmt.makePropertyKey("test-list").dataType(String.class).cardinality(Cardinality.SET).make();
		mgmt.commit();
		Vertex pserverSkeleton = g.addV().property("aai-node-type", "pserver").property("hostname", "TEST1")
				.property("source-of-truth", "AAI-EXTENSIONS").property("fqdn", "test1.com").property("test-list", "value1").next();

		Vertex pInterface1 = g.addV().property("aai-node-type", "p-interface").property("interface-name", "p-interface1")
				.property(AAIProperties.AAI_URI, "/cloud-infrastructure/pservers/pserver/TEST1/p-interfaces/p-interface/p-interface1").next();
		
		Vertex pInterface2 = g.addV().property("aai-node-type", "p-interface").property("interface-name", "p-interface2")
				.property(AAIProperties.AAI_URI, "/cloud-infrastructure/pservers/pserver/TEST1/p-interfaces/p-interface/p-interface2").next();
		
		Vertex pInterface2Secondary = g.addV().property("aai-node-type", "p-interface").property("interface-name", "p-interface2").property("special-prop", "value")
				.property(AAIProperties.AAI_URI, "/cloud-infrastructure/pservers/pserver/TEST1/p-interfaces/p-interface/p-interface2").next();
		
		Vertex lInterface1 = g.addV().property("aai-node-type", "l-interface").property("interface-name", "l-interface1").property("special-prop", "value")
				.property(AAIProperties.AAI_URI, "/cloud-infrastructure/pservers/pserver/TEST1/p-interfaces/p-interface/p-interface2/l-interfaces/l-interface/l-interface1").next();
		
		Vertex lInterface1Canopi = g.addV().property("aai-node-type", "l-interface").property("interface-name", "l-interface1")
				.property(AAIProperties.AAI_URI, "/cloud-infrastructure/pservers/pserver/TEST1/p-interfaces/p-interface/p-interface2/l-interfaces/l-interface/l-interface1").next();
		
		Vertex logicalLink = g.addV().property("aai-node-type", "logical-link").property("link-name", "logical-link1")
				.property(AAIProperties.AAI_URI, "/network/logical-links/logical-link/logical-link1").next();
		Vertex pserverCanopi = g.addV().property("aai-node-type", "pserver").property("hostname",  "TEST1")
				.property("source-of-truth", "CANOPI-WS").property("fqdn", "test2.com").property("test-list", "value2").next();
		
		Vertex complex1 = g.addV().property("aai-node-type", "complex").property("physical-location-id", "complex1")
				.property("source-of-truth", "RO").next();
		
		Vertex complex2 = g.addV().property("aai-node-type", "complex").property("physical-location-id", "complex2")
				.property("source-of-truth", "RCT").next();
		
		Vertex vserver1 = g.addV().property("aai-node-type", "vserver").property("vserver-id", "vserver1")
				.property("source-of-truth", "RO").next();
		
		Vertex vserver2 = g.addV().property("aai-node-type", "vserver").property("vserver-id", "vserver2")
				.property("source-of-truth", "RCT").next();
		Vertex vserver3 = g.addV().property("aai-node-type", "vserver").property("vserver-id", "vserver3")
				.property("source-of-truth", "RCT").next();
		Vertex vserver4 = g.addV().property("aai-node-type", "vserver").property("vserver-id", "vserver4")
				.property("source-of-truth", "RCT").next();
		Vertex vserver5 = g.addV().property("aai-node-type", "vserver").property("vserver-id", "vserver5")
				.property("source-of-truth", "RCT").next();
		
		
		edgeSerializer.addEdge(g, pserverSkeleton, complex1);
		edgeSerializer.addEdge(g, pserverSkeleton, vserver1);
		edgeSerializer.addEdge(g, pserverSkeleton, vserver2);
		edgeSerializer.addTreeEdge(g, pserverSkeleton, pInterface1);
		edgeSerializer.addTreeEdge(g, pserverSkeleton, pInterface2Secondary);
		edgeSerializer.addTreeEdge(g, pInterface2Secondary, lInterface1);
		edgeSerializer.addEdge(g, lInterface1, logicalLink);
		edgeSerializer.addEdge(g, pserverCanopi, complex2);
		edgeSerializer.addEdge(g, pserverCanopi, vserver3);
		edgeSerializer.addEdge(g, pserverCanopi, vserver4);
		edgeSerializer.addEdge(g, pserverCanopi, vserver5);
		edgeSerializer.addTreeEdge(g, pserverCanopi, pInterface2);
		edgeSerializer.addTreeEdge(g, pInterface2, lInterface1Canopi);

		Map<String, Set<String>> forceCopy = new HashMap<>();
		Set<String> forceSet = new HashSet<>();
		forceSet.add("fqdn");
		forceCopy.put("pserver", forceSet);
		
		TransactionalGraphEngine spy = spy(dbEngine);
		TransactionalGraphEngine.Admin adminSpy = spy(dbEngine.asAdmin());
		GraphTraversalSource traversal = g;
		GraphTraversalSource readOnly = g;
		when(spy.asAdmin()).thenReturn(adminSpy);
		when(adminSpy.getTraversalSource()).thenReturn(traversal);
		when(adminSpy.getReadOnlyTraversalSource()).thenReturn(readOnly);
		DBSerializer serializer = new DBSerializer(version, spy, introspectorFactoryType, "Merge test");

		VertexMerge merge = new VertexMerge.Builder(loader, spy, serializer).edgeSerializer(edgeSerializer).build();
		merge.performMerge(pserverCanopi, pserverSkeleton, forceCopy, basePath);
	}

	@After
	public void cleanUp() {
		tx.tx().rollback();
		graph.close();
	}

	@Test
	public void run() throws UnsupportedEncodingException {

		assertEquals("pserver merged", false, g.V().has("hostname", "TEST1").has("source-of-truth", "AAI-EXTENSIONS").hasNext());
		assertThat("pserver list merge", Arrays.asList("value1", "value2"), containsInAnyOrder(g.V().has("hostname", "TEST1").values("test-list").toList().toArray()));
		assertEquals("canopi pserver has one edge to vserver2", 1, g.V().has("hostname", "TEST1").both().has("vserver-id", "vserver2").toList().size());
		assertEquals("canopi pserver has one edge to vserver1", 1, g.V().has("hostname", "TEST1").both().has("vserver-id", "vserver1").toList().size());
		assertEquals("canopi pserver retained edge to complex2", true, g.V().has("hostname", "TEST1").both().has("physical-location-id", "complex2").hasNext());
		assertEquals("canopi pserver received forced prop", "test1.com", g.V().has("hostname", "TEST1").values("fqdn").next());
		assertEquals("pserver skeleton child copied", true, g.V().has("hostname", "TEST1").both().has("interface-name", "p-interface1").hasNext());
		assertEquals("pserver skeleton child merged", true, g.V().has("hostname", "TEST1").both().has("interface-name", "p-interface2").has("special-prop", "value").hasNext());
		assertEquals("l-interface child merged", true, g.V().has("hostname", "TEST1").both().has("interface-name", "p-interface2").both().has("interface-name", "l-interface1").has("special-prop", "value").hasNext());
		assertEquals("l-interface child cousin edge merged", true, g.V().has("hostname", "TEST1").both().has("interface-name", "p-interface2").both().has("interface-name", "l-interface1").both().has("link-name", "logical-link1").hasNext());
		assertEquals("one l-interface1 found", new Long(1), g.V().has("interface-name", "l-interface1").count().next());
		assertEquals("one p-interface2 found", new Long(1), g.V().has("interface-name", "p-interface2").count().next());

	}
}
