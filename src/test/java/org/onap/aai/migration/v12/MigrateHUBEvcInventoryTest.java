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

import static org.junit.Assert.assertArrayEquals;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertNotNull;
import static org.mockito.Mockito.spy;
import static org.mockito.Mockito.when;

import java.util.Optional;

import org.apache.tinkerpop.gremlin.process.traversal.dsl.graph.GraphTraversalSource;
import org.apache.tinkerpop.gremlin.process.traversal.strategy.verification.ReadOnlyStrategy;
import org.apache.tinkerpop.gremlin.structure.Vertex;
import org.junit.After;
import org.junit.Before;
import org.junit.Test;
import org.onap.aai.AAISetup;
import org.onap.aai.dbmap.DBConnectionType;
import org.onap.aai.introspection.Loader;
import org.onap.aai.introspection.LoaderFactory;
import org.onap.aai.introspection.ModelType;
import org.onap.aai.setup.SchemaVersions;
import org.onap.aai.setup.SchemaVersion;
import org.onap.aai.serialization.engines.QueryStyle;
import org.onap.aai.serialization.engines.JanusGraphDBEngine;
import org.onap.aai.serialization.engines.TransactionalGraphEngine;

import org.janusgraph.core.JanusGraphFactory;
import org.janusgraph.core.JanusGraph;
import org.janusgraph.core.JanusGraphTransaction;

public class MigrateHUBEvcInventoryTest extends AAISetup {

	private final static ModelType introspectorFactoryType = ModelType.MOXY;
	private final static QueryStyle queryStyle = QueryStyle.TRAVERSAL;

	private Loader loader;
	private TransactionalGraphEngine dbEngine;
	private JanusGraph graph;
	private MigrateHUBEvcInventory migration;
	private JanusGraphTransaction tx;
	private GraphTraversalSource g;

	@Before
	public void setUp() throws Exception {
		graph = JanusGraphFactory.build().set("storage.backend","inmemory").open();
		tx = graph.newTransaction();
		g = tx.traversal();
		loader = loaderFactory.createLoaderForVersion(introspectorFactoryType, schemaVersions.getDefaultVersion());
		dbEngine = new JanusGraphDBEngine(
				queryStyle,
				loader);

		System.setProperty("BUNDLECONFIG_DIR", "src/test/resources");
		
		Vertex customer1 = g.addV()
				.property("aai-node-type", "customer")
				.property("global-customer-id", "customer-id-1")
				.property("subscriber-type", "CUST")
				.next();
		
		Vertex servSub1 = g.addV()
				.property("aai-node-type", "service-subscription")
				.property("service-type", "SAREA")
				.next();
		
		Vertex servInstance1 = g.addV()
				.property("aai-node-type", "service-instance")
				.property("service-type", "SAREA")
				.property("service-instance-id", "evc-name-1")
				.next();
		Vertex servInstance3 = g.addV()
				.property("aai-node-type", "service-instance")
				.property("service-type", "SAREA")
				.property("service-instance-id", "evc-name-3")
				.next();
		Vertex servInstance2 = g.addV()
				.property("aai-node-type", "service-instance")
				.property("service-type", "SAREA")
				.property("service-instance-id", "evc-name-2")
				.next();
		
		Vertex evc1 = g.addV().property("aai-node-type", "evc")
				.property("evc-id", "evc-name-1")
				.next();
		Vertex config1 = g.addV().property("aai-node-type", "configuration")
				.property("configuration-id", "evc-name-1")
				.next();
		Vertex fp1 =  g.addV()
				.property("aai-node-type", "forwarding-path")
				.property("forwarding-path-id", "evc-name-1")
				.next();
		Vertex for11 = g.addV()
				.property("aai-node-type", "forwarder")
				.property("sequence", "1")
				.property("forwarder-role","ingress")
				.next();
		Vertex for12 = g.addV()
				.property("aai-node-type", "forwarder")
				.property("sequence", "2")
				.property("forwarder-role","egress")
				.next();
		Vertex config11 = g.addV()
				.property("aai-node-type", "configuration")
				.property("configuration-id", "evc-name-1-1")
				.next();
		Vertex config12 = g.addV()
				.property("aai-node-type", "configuration")
				.property("configuration-id", "evc-name-1-2")
				.next();
		Vertex fevc11 = g.addV()
				.property("aai-node-type", "forwarder-evc")
				.property("forwarder-evc-id", "evc-name-1-1")
				.property("svlan",  "6")
				.next();
		Vertex fevc12 = g.addV()
				.property("aai-node-type", "forwarder-evc")
				.property("forwarder-evc-id", "evc-name-1-2")
				.property("svlan",  "16")
				.next();
		
		
		
		Vertex evc2 = g.addV().property("aai-node-type", "evc")
				.property("evc-id", "evc-name-2")
				.next();
		Vertex config2 = g.addV().property("aai-node-type", "configuration")
				.property("configuration-id", "evc-name-2")
				.next();
		Vertex fp2 =  g.addV()
				.property("aai-node-type", "forwarding-path")
				.property("forwarding-path-id", "evc-name-2")
				.next();
		Vertex for21 = g.addV()
				.property("aai-node-type", "forwarder")
				.property("sequence", "1")
				.property("forwarder-role","ingress")
				.next();
		Vertex for22 = g.addV()
				.property("aai-node-type", "forwarder")
				.property("sequence", "2")
				.property("forwarder-role","ingress")
				.next();
		Vertex for23 = g.addV()
				.property("aai-node-type", "forwarder")
				.property("sequence", "3")
				.property("forwarder-role","egress")
				.next();
		Vertex for24 = g.addV()
				.property("aai-node-type", "forwarder")
				.property("sequence", "4")
				.property("forwarder-role","egress")
				.next();
		Vertex config21 = g.addV()
				.property("aai-node-type", "configuration")
				.property("configuration-id", "evc-name-2-1")
				.next();
		Vertex config22 = g.addV()
				.property("aai-node-type", "configuration")
				.property("configuration-id", "evc-name-2-2")
				.next();
		Vertex config23 = g.addV()
				.property("aai-node-type", "configuration")
				.property("configuration-id", "evc-name-2-3")
				.next();
		Vertex config24 = g.addV()
				.property("aai-node-type", "configuration")
				.property("configuration-id", "evc-name-2-4")
				.next();
		Vertex fevc21 = g.addV()
				.property("aai-node-type", "forwarder-evc")
				.property("forwarder-evc-id", "evc-name-2-1")
				.property("svlan",  "6")
				.next();
		Vertex fevc22 = g.addV()
				.property("aai-node-type", "forwarder-evc")
				.property("forwarder-evc-id", "evc-name-2-2")
				.property("svlan",  "16")
				.next();
		Vertex fevc23 = g.addV()
				.property("aai-node-type", "forwarder-evc")
				.property("forwarder-evc-id", "evc-name-2-3")
				.property("svlan",  "12")
				.property("ivlan", "600")
				.next();
		Vertex fevc24 = g.addV()
				.property("aai-node-type", "forwarder-evc")
				.property("forwarder-evc-id", "evc-name-2-4")
				.property("svlan",  "16")
				.property("ivlan", "600")
				.next();

		Vertex evc3 = g.addV().property("aai-node-type", "evc")
				.property("evc-id", "evc-name-3")
				.next();
		Vertex config3 = g.addV().property("aai-node-type", "configuration")
				.property("configuration-id", "evc-name-3")
				.next();
		Vertex fp3 =  g.addV()
				.property("aai-node-type", "forwarding-path")
				.property("forwarding-path-id", "evc-name-3")
				.next();
		Vertex for31 = g.addV()
				.property("aai-node-type", "forwarder")
				.property("sequence", "1")
				.property("forwarder-role","ingress")
				.next();
		Vertex for32 = g.addV()
				.property("aai-node-type", "forwarder")
				.property("sequence", "2")
				.property("forwarder-role","egress")
				.next();
		Vertex config31 = g.addV()
				.property("aai-node-type", "configuration")
				.property("configuration-id", "evc-name-3-1")
				.next();
		Vertex config32 = g.addV()
				.property("aai-node-type", "configuration")
				.property("configuration-id", "evc-name-3-2")
				.next();
		Vertex fevc31 = g.addV()
				.property("aai-node-type", "forwarder-evc")
				.property("forwarder-evc-id", "evc-name-3-1")
				.property("svlan",  "6")
				.next();
		Vertex fevc32 = g.addV()
				.property("aai-node-type", "forwarder-evc")
				.property("forwarder-evc-id", "evc-name-3-2")
//				.property("svlan",  "16")
				.next();
		
		// graph 1
		edgeSerializer.addTreeEdge(g, customer1, servSub1);
		edgeSerializer.addTreeEdge(g, servSub1, servInstance1);
		edgeSerializer.addTreeEdge(g, servSub1, servInstance2);
		edgeSerializer.addTreeEdge(g, servSub1, servInstance3);
		
		edgeSerializer.addEdge(g, servInstance1, fp1);
		edgeSerializer.addEdge(g, servInstance2, fp2);
		
		edgeSerializer.addEdge(g, fp1, config1);
		edgeSerializer.addEdge(g, fp2, config2);
		edgeSerializer.addEdge(g, fp3, config3);
		
		edgeSerializer.addTreeEdge(g, evc1,  config1);
		edgeSerializer.addTreeEdge(g, evc2, config2);
		edgeSerializer.addTreeEdge(g, evc3, config3);
		
		edgeSerializer.addTreeEdge(g, fp1, for11);
		edgeSerializer.addTreeEdge(g, fp1, for12);
		edgeSerializer.addTreeEdge(g, fp2, for21);
		edgeSerializer.addTreeEdge(g, fp2, for22);
		edgeSerializer.addTreeEdge(g, fp2, for23);
		edgeSerializer.addTreeEdge(g, fp2, for24);
		edgeSerializer.addTreeEdge(g, fp3, for31);
		edgeSerializer.addTreeEdge(g, fp3, for32);
		
		edgeSerializer.addEdge(g,  for11, config11);
		edgeSerializer.addEdge(g,  for12, config12);
		edgeSerializer.addEdge(g,  for21, config21);
		edgeSerializer.addEdge(g,  for22, config22);
		edgeSerializer.addEdge(g,  for23, config23);
		edgeSerializer.addEdge(g,  for24, config24);
		edgeSerializer.addEdge(g,  for31, config31);
		edgeSerializer.addEdge(g,  for32, config32);
		
		edgeSerializer.addTreeEdge(g, config11, fevc11);
		edgeSerializer.addTreeEdge(g, config12, fevc12);
		edgeSerializer.addTreeEdge(g, config21, fevc21);
		edgeSerializer.addTreeEdge(g, config22, fevc22);
		edgeSerializer.addTreeEdge(g, config23, fevc23);
		edgeSerializer.addTreeEdge(g, config24, fevc24);
		edgeSerializer.addTreeEdge(g, config31, fevc31);
		edgeSerializer.addTreeEdge(g, config32, fevc32);
		
		TransactionalGraphEngine spy = spy(dbEngine);
		TransactionalGraphEngine.Admin adminSpy = spy(dbEngine.asAdmin());

		GraphTraversalSource traversal = g;
		GraphTraversalSource readOnly = graph.traversal().withStrategies(ReadOnlyStrategy.instance());
		when (spy.tx()).thenReturn(tx);
		when(spy.asAdmin()).thenReturn(adminSpy);
		when(adminSpy.getTraversalSource()).thenReturn(traversal);
		when(adminSpy.getReadOnlyTraversalSource()).thenReturn(readOnly);
		
		migration = new MigrateHUBEvcInventory(spy, loaderFactory, edgeIngestor, edgeSerializer, schemaVersions);
		migration.run();
	}
	
	@After
	public void cleanUp() {
		tx.tx().rollback();
		graph.close();
	}
	
	@Test
	public void testRun_checkFevc1AndFevc2AreUpdated() throws Exception {
		
		// check if forwarder-evc nodes get updated
		assertEquals("forwarder-evc evc-name-1-1 updated with ivlan", true, 
				g.V().has("aai-node-type", "forwarder-evc")
				.has("forwarder-evc-id", "evc-name-1-1")
				.has("ivlan","4054")
				.hasNext());
		
		assertEquals("forwarder-evc evc-name-2-2 updated with ivlan", true, 
				g.V().has("aai-node-type", "forwarder-evc")
				.has("forwarder-evc-id", "evc-name-2-2")
				.has("ivlan","4084")
				.hasNext());
		assertEquals("forwarder-evc evc-name-2-3 updated with ivlan", true, 
				g.V().has("aai-node-type", "forwarder-evc")
				.has("forwarder-evc-id", "evc-name-2-3")
				.has("ivlan","4054")
				.hasNext());
		
		assertEquals("4 forwarder-evcs exist for evc evc-name-2", new Long(4L), 
				g.V().has("forwarding-path-id", "evc-name-2")
				.in("org.onap.relationships.inventory.BelongsTo").has("aai-node-type", "forwarder")
				.out("org.onap.relationships.inventory.Uses").has("aai-node-type", "configuration")
				.in("org.onap.relationships.inventory.BelongsTo").has("aai-node-type", "forwarder-evc")
				.count().next());
		
		assertEquals("3 forwarder-evcs updated for evc evc-name-2", new Long(3L), 
				g.V().has("forwarding-path-id", "evc-name-2")
				.in("org.onap.relationships.inventory.BelongsTo").has("aai-node-type", "forwarder")
				.out("org.onap.relationships.inventory.Uses").has("aai-node-type", "configuration")
				.in("org.onap.relationships.inventory.BelongsTo").has("aai-node-type", "forwarder-evc")
				.has("forwarder-evc-id").has("ivlan")
				.count().next());
		
		assertEquals("forwarder-evc evc-name-3-1 updated with ivlan", false, 
				g.V().has("aai-node-type", "forwarder-evc")
				.has("forwarder-evc-id", "evc-name-3-1")
				.has("ivlan")
				.hasNext());
	}

	
	@Test
	public void testGetAffectedNodeTypes() {
		Optional<String[]> types = migration.getAffectedNodeTypes();
		Optional<String[]> expected = Optional.of(new String[]{"forwarder-evc"});
		
		assertNotNull(types);
		assertArrayEquals(expected.get(), types.get());
	}

	@Test
	public void testGetMigrationName() {
		String migrationName = migration.getMigrationName();

		assertNotNull(migrationName);
		assertEquals("MigrateHUBEvcInventory", migrationName);
	}
}
