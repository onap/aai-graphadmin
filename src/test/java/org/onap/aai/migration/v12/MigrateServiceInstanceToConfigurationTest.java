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
import org.junit.*;
import org.onap.aai.AAISetup;
import org.onap.aai.dbmap.DBConnectionType;
import org.onap.aai.introspection.Loader;
import org.onap.aai.introspection.ModelType;
import org.onap.aai.setup.SchemaVersions;
import org.onap.aai.setup.SchemaVersion;
import org.onap.aai.serialization.engines.QueryStyle;
import org.onap.aai.serialization.engines.JanusGraphDBEngine;
import org.onap.aai.serialization.engines.TransactionalGraphEngine;

import org.janusgraph.core.JanusGraphFactory;
import org.janusgraph.core.JanusGraph;
import org.janusgraph.core.JanusGraphTransaction;

public class MigrateServiceInstanceToConfigurationTest extends AAISetup {

	private final static ModelType introspectorFactoryType = ModelType.MOXY;
	private final static QueryStyle queryStyle = QueryStyle.TRAVERSAL;
	private final static DBConnectionType type = DBConnectionType.REALTIME;

	private Loader loader;
	private TransactionalGraphEngine dbEngine;
	private JanusGraph graph;
	private MigrateServiceInstanceToConfiguration migration;
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
				type,
				loader);

		Vertex customer1 = g.addV()
				.property("aai-node-type", "customer")
				.property("global-customer-id", "customer-id-1")
				.property("subscriber-type", "CUST")
				.next();
		
		Vertex customer2 = g.addV()
				.property("aai-node-type", "customer")
				.property("global-customer-id", "customer-id-2")
				.property("subscriber-type", "CUST")
				.next();
		
		Vertex customer3 = g.addV()
				.property("aai-node-type", "customer")
				.property("global-customer-id", "customer-id-3")
				.property("subscriber-type", "CUST")
				.next();
		
		Vertex customer4 = g.addV()
				.property("aai-node-type", "customer")
				.property("global-customer-id", "customer-id-4")
				.property("subscriber-type", "CUST")
				.next();

		Vertex servSub1 = g.addV()
				.property("aai-node-type", "service-subscription")
				.property("service-type", "DHV")
				.next();
		
		Vertex servSub2 = g.addV()
				.property("aai-node-type", "service-subscription")
				.property("service-type", "OTHER")
				.next();
		
		Vertex servSub3 = g.addV()
				.property("aai-node-type", "service-subscription")
				.property("service-type", "DHV")
				.next();
		
		Vertex servSub4 = g.addV()
				.property("aai-node-type", "service-subscription")
				.property("service-type", "DHV")
				.next();
		
		Vertex servSub5 = g.addV()
				.property("aai-node-type", "service-subscription")
				.property("service-type", "DHV")
				.next();
		
		Vertex servInstance1 = g.addV()
				.property("aai-node-type", "service-instance")
				.property("service-instance-id", "service-inst-1")
				.property("operational-status", "activated")
				.property("bandwidth-total", "5")
				.next();
		
		Vertex servInstance2 = g.addV()
				.property("aai-node-type", "service-instance")
				.property("service-instance-id", "service-inst-2")
				.property("operational-status", "activated")
				.property("bandwidth-total", "8")
				.next();
		
		Vertex servInstance3 = g.addV()
				.property("aai-node-type", "service-instance")
				.property("service-instance-id", "service-inst-3")
				.property("operational-status", "activated")
				.property("bandwidth-total", "10")
				.next();
		
		Vertex servInstance4 = g.addV()
				.property("aai-node-type", "service-instance")
				.property("service-instance-id", "service-inst-4")
				.property("operational-status", "activated")
				.property("bandwidth-total", "15")
				.next();
		
		Vertex config1 = g.addV()
				.property("aai-node-type", "configuration")
				.property("configuration-id", "configuration-1")
				.property("configuration-type", "DHV")
				.property("tunnel-bandwidth", "7")
				.next();
		
		Vertex config2 = g.addV()
				.property("aai-node-type", "configuration")
				.property("configuration-id", "configuration-2")
				.property("configuration-type", "OTHER")
				.property("tunnel-bandwidth", "3")
				.next();
		
		Vertex config3 = g.addV()
				.property("aai-node-type", "configuration")
				.property("configuration-id", "configuration-3")
				.property("configuration-type", "OTHER")
				.property("tunnel-bandwidth", "2")
				.next();
		
		Vertex config4 = g.addV()
				.property("aai-node-type", "configuration")
				.property("configuration-id", "configuration-4")
				.property("configuration-type", "OTHER")
				.property("tunnel-bandwidth", "4")
				.next();

		// graph 1
		edgeSerializer.addTreeEdge(g, customer1, servSub1);
		edgeSerializer.addTreeEdge(g, customer1, servSub2);
		edgeSerializer.addTreeEdge(g, servSub1, servInstance1);
		edgeSerializer.addTreeEdge(g, servSub2, servInstance2);
		
		// graph 2
		edgeSerializer.addTreeEdge(g, customer2, servSub3);
		
		// graph 3
		edgeSerializer.addTreeEdge(g, customer3, servSub4);
		edgeSerializer.addTreeEdge(g, servSub4, servInstance3);
		edgeSerializer.addEdge(g, servInstance3, config1);
		edgeSerializer.addEdge(g, servInstance3, config2);
		
		// graph 4
		edgeSerializer.addTreeEdge(g, customer4, servSub5);
		edgeSerializer.addTreeEdge(g, servSub5, servInstance4);
		edgeSerializer.addEdge(g, servInstance4, config3);
		edgeSerializer.addEdge(g, servInstance4, config4);

		TransactionalGraphEngine spy = spy(dbEngine);
		TransactionalGraphEngine.Admin adminSpy = spy(dbEngine.asAdmin());

		GraphTraversalSource traversal = g;
		GraphTraversalSource readOnly = tx.traversal(GraphTraversalSource.build().with(ReadOnlyStrategy.instance()));
		when (spy.tx()).thenReturn(tx);
		when(spy.asAdmin()).thenReturn(adminSpy);
		when(adminSpy.getTraversalSource()).thenReturn(traversal);
		when(adminSpy.getReadOnlyTraversalSource()).thenReturn(readOnly);
		
		migration = new MigrateServiceInstanceToConfiguration(spy, loaderFactory, edgeIngestor, edgeSerializer, schemaVersions);
		migration.run();
	}
	
	@After
	public void cleanUp() {
		tx.tx().rollback();
		graph.close();
	}
	
	@Test
	public void testRun_createConfigNode() throws Exception {
		// check if graph nodes exist
		assertEquals("customer node exists", true, 
				g.V().has("global-customer-id", "customer-id-1")
				.hasNext());
		
		assertEquals("service subscription node, service-type=DHV", true, 
				g.V().has("global-customer-id", "customer-id-1")
				.in("org.onap.relationships.inventory.BelongsTo").has("service-type", "DHV")
				.hasNext());
		
		assertEquals("service instance node, bandwidth-total=5", true, 
				g.V().has("global-customer-id", "customer-id-1")
				.in("org.onap.relationships.inventory.BelongsTo").has("service-type", "DHV")
				.in("org.onap.relationships.inventory.BelongsTo").has("service-instance-id", "service-inst-1").has("bandwidth-total", "5")
				.hasNext());
		
		// check if configuration node gets created
		assertEquals("configuration node exists", true, 
				g.V().has("global-customer-id", "customer-id-1")
				.in("org.onap.relationships.inventory.BelongsTo").has("service-type", "DHV")
				.in("org.onap.relationships.inventory.BelongsTo").has("service-instance-id", "service-inst-1")
				.out("org.onap.relationships.inventory.Uses").has("aai-node-type", "configuration")
				.hasNext());
		
		// check configuration type
		assertEquals("configuration node, configuration-type=DHV", true, 
				g.V().has("global-customer-id", "customer-id-1")
				.in("org.onap.relationships.inventory.BelongsTo").has("service-type", "DHV")
				.in("org.onap.relationships.inventory.BelongsTo").has("service-instance-id", "service-inst-1")
				.out("org.onap.relationships.inventory.Uses").has("aai-node-type", "configuration").has("configuration-type", "DHV")
				.hasNext());
		
		// check configuration tunnel-bandwidth
		assertEquals("configuration node, tunnel-bandwidth=5", true, 
				g.V().has("global-customer-id", "customer-id-1")
				.in("org.onap.relationships.inventory.BelongsTo").has("service-type", "DHV")
				.in("org.onap.relationships.inventory.BelongsTo").has("service-instance-id", "service-inst-1")
				.out("org.onap.relationships.inventory.Uses").has("aai-node-type", "configuration").has("tunnel-bandwidth", "5")
				.hasNext());
	}

	@Test
	public void testRun_configNodeNotCreated() throws Exception {
		// check if graph nodes exist
		assertEquals("customer node exists", true, 
				g.V().has("global-customer-id", "customer-id-1")
				.hasNext());
		
		assertEquals("service subscription node, service-type=OTHER", true, 
				g.V().has("global-customer-id", "customer-id-1")
				.in("org.onap.relationships.inventory.BelongsTo").has("service-type", "OTHER")
				.hasNext());
		
		assertEquals("service instance node, bandwidth-total=8", true, 
				g.V().has("global-customer-id", "customer-id-1")
				.in("org.onap.relationships.inventory.BelongsTo").has("service-type", "OTHER")
				.in("org.onap.relationships.inventory.BelongsTo").has("service-instance-id", "service-inst-2").has("bandwidth-total", "8")
				.hasNext());
		
		// configuration node should not be created
		assertEquals("configuration node does not exist", false, 
				g.V().has("global-customer-id", "customer-id-1")
				.in("org.onap.relationships.inventory.BelongsTo").has("service-type", "OTHER")
				.in("org.onap.relationships.inventory.BelongsTo").has("service-instance-id", "service-inst-2")
				.out("org.onap.relationships.inventory.Uses").has("aai-node-type", "configuration")
				.hasNext());
		
		// edge between service instance and configuration should not be created
		assertEquals("configuration node does not exist", false, 
				g.V().has("global-customer-id", "customer-id-1")
				.in("org.onap.relationships.inventory.BelongsTo").has("service-type", "OTHER")
				.in("org.onap.relationships.inventory.BelongsTo").has("service-instance-id", "service-inst-2")
				.out("org.onap.relationships.inventory.Uses").hasNext());
	}
	
	@Test
	public void testRun_noServiceInstance() throws Exception {
		// check if graph nodes exist
		assertEquals("customer node exists", true, 
				g.V().has("global-customer-id", "customer-id-2")
				.hasNext());
		
		assertEquals("service subscription node, service-type=DHV", true, 
				g.V().has("global-customer-id", "customer-id-2")
				.in("org.onap.relationships.inventory.BelongsTo").has("service-type", "DHV")
				.hasNext());
		
		// no service instance nodes
		assertEquals("no service instance nodes", false, 
				g.V().has("global-customer-id", "customer-id-2")
				.in("org.onap.relationships.inventory.BelongsTo").has("service-type", "DHV")
				.in("org.onap.relationships.inventory.BelongsTo").has("aai-node-type", "service-instance")
				.hasNext());
	}
	
	@Test
	public void testRun_existingConfig() throws Exception {
		// check if graph nodes exist
		assertEquals("customer node exists", true, 
				g.V().has("global-customer-id", "customer-id-3")
				.hasNext());
		
		assertEquals("service subscription node, service-type=DHV", true, 
				g.V().has("global-customer-id", "customer-id-3")
				.in("org.onap.relationships.inventory.BelongsTo").has("service-type", "DHV")
				.hasNext());
		
		assertEquals("service instance node, bandwidth-total=10", true, 
				g.V().has("global-customer-id", "customer-id-3")
				.in("org.onap.relationships.inventory.BelongsTo").has("service-type", "DHV")
				.in("org.onap.relationships.inventory.BelongsTo").has("service-instance-id", "service-inst-3").has("bandwidth-total", "10")
				.hasNext());
		
		assertEquals("configuration node with type DHV, tunnel-bandwidth changed to 10", true, 
				g.V().has("global-customer-id", "customer-id-3")
				.in("org.onap.relationships.inventory.BelongsTo").has("service-type", "DHV")
				.in("org.onap.relationships.inventory.BelongsTo").has("service-instance-id", "service-inst-3")
				.out("org.onap.relationships.inventory.Uses").has("aai-node-type", "configuration").has("tunnel-bandwidth", "10")
				.hasNext());
		
		assertEquals("configuration node with type OTHER, tunnel-bandwidth remains same", true, 
				g.V().has("global-customer-id", "customer-id-3")
				.in("org.onap.relationships.inventory.BelongsTo").has("service-type", "DHV")
				.in("org.onap.relationships.inventory.BelongsTo").has("service-instance-id", "service-inst-3")
				.out("org.onap.relationships.inventory.Uses").has("aai-node-type", "configuration").has("tunnel-bandwidth", "3")
				.hasNext());
	}
	
	@Test
	public void testRun_existingConfigNotDHV() throws Exception {
		// check if graph nodes exist
		assertEquals("customer node exists", true, 
				g.V().has("global-customer-id", "customer-id-4")
				.hasNext());
		
		assertEquals("service subscription node, service-type=DHV", true, 
				g.V().has("global-customer-id", "customer-id-4")
				.in("org.onap.relationships.inventory.BelongsTo").has("service-type", "DHV")
				.hasNext());
		
		assertEquals("service instance node, bandwidth-total=15", true, 
				g.V().has("global-customer-id", "customer-id-4")
				.in("org.onap.relationships.inventory.BelongsTo").has("service-type", "DHV")
				.in("org.onap.relationships.inventory.BelongsTo").has("service-instance-id", "service-inst-4").has("bandwidth-total", "15")
				.hasNext());
		
		assertEquals("first configuration node with type OTHER, tunnel-bandwidth remains same", true, 
				g.V().has("global-customer-id", "customer-id-4")
				.in("org.onap.relationships.inventory.BelongsTo").has("service-type", "DHV")
				.in("org.onap.relationships.inventory.BelongsTo").has("service-instance-id", "service-inst-4")
				.out("org.onap.relationships.inventory.Uses").has("aai-node-type", "configuration").has("tunnel-bandwidth", "2")
				.hasNext());
		
		assertEquals("second configuration node with type OTHER, tunnel-bandwidth remains same", true, 
				g.V().has("global-customer-id", "customer-id-4")
				.in("org.onap.relationships.inventory.BelongsTo").has("service-type", "DHV")
				.in("org.onap.relationships.inventory.BelongsTo").has("service-instance-id", "service-inst-4")
				.out("org.onap.relationships.inventory.Uses").has("aai-node-type", "configuration").has("tunnel-bandwidth", "4")
				.hasNext());
		
		assertEquals("new configuration node created with type DHV, tunnel-bandwidth=15", true, 
				g.V().has("global-customer-id", "customer-id-4")
				.in("org.onap.relationships.inventory.BelongsTo").has("service-type", "DHV")
				.in("org.onap.relationships.inventory.BelongsTo").has("service-instance-id", "service-inst-4")
				.out("org.onap.relationships.inventory.Uses").has("aai-node-type", "configuration").has("tunnel-bandwidth", "15")
				.hasNext());
	}
	
	@Test
	public void testGetAffectedNodeTypes() {
		Optional<String[]> types = migration.getAffectedNodeTypes();
		Optional<String[]> expected = Optional.of(new String[]{"service-instance"});
		
		assertNotNull(types);
		assertArrayEquals(expected.get(), types.get());
	}

	@Test
	public void testGetMigrationName() {
		String migrationName = migration.getMigrationName();

		assertNotNull(migrationName);
		assertEquals("service-instance-to-configuration", migrationName);
	}
}
