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

import static org.junit.jupiter.api.Assertions.*;
import static org.mockito.Mockito.spy;
import static org.mockito.Mockito.when;

import java.util.Optional;

import org.apache.tinkerpop.gremlin.process.traversal.dsl.graph.GraphTraversalSource;
import org.apache.tinkerpop.gremlin.process.traversal.strategy.verification.ReadOnlyStrategy;
import org.apache.tinkerpop.gremlin.structure.Vertex;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Disabled;
import org.junit.jupiter.api.Test;

import org.onap.aai.AAISetup;
import org.onap.aai.introspection.Loader;
import org.onap.aai.introspection.ModelType;
import org.onap.aai.serialization.engines.QueryStyle;
import org.onap.aai.serialization.engines.JanusGraphDBEngine;
import org.onap.aai.serialization.engines.TransactionalGraphEngine;

import org.janusgraph.core.JanusGraphFactory;
import org.janusgraph.core.JanusGraph;
import org.janusgraph.core.JanusGraphTransaction;

public class MigrateSAREvcInventoryTest extends AAISetup {

	private final static ModelType introspectorFactoryType = ModelType.MOXY;
	private final static QueryStyle queryStyle = QueryStyle.TRAVERSAL;
	private Loader loader;
	private TransactionalGraphEngine dbEngine;
	private JanusGraph graph;
	private MigrateSAREvcInventory migration;
	private JanusGraphTransaction tx;
	private GraphTraversalSource g;

	@BeforeEach
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
				.property("global-customer-id", "8a00890a-e6ae-446b-9dbe-b828dbeb38bd")
				.property("subscriber-type", "CUST")
				.next();

		Vertex servSub1 = g.addV()
				.property("aai-node-type", "service-subscription")
				.property("service-type", "SAREA")
				.next();

		Vertex servInst1 = g.addV()
				.property("aai-node-type", "service-instance")
				.property("service-instance-id", "evc-name-1")
				.next();

		Vertex customer2 = g.addV()
				.property("aai-node-type", "customer")
				.property("global-customer-id", "cust-1")
				.property("subscriber-type", "CUST")
				.next();

		Vertex servSub2 = g.addV()
				.property("aai-node-type", "service-subscription")
				.property("service-type", "SAREA")
				.next();

		Vertex servInst2 = g.addV()
				.property("aai-node-type", "service-instance")
				.property("service-instance-id", "evc-name-1")
				.next();

		Vertex collectorPnf = g.addV()
				.property("aai-node-type", "pnf")
				.property("pnf-name", "pnf-name-collector-1")
				.next();

		Vertex bearerPnf = g.addV()
				.property("aai-node-type", "pnf")
				.property("pnf-name", "pnf-name-bearer-1")
				.next();

		Vertex collectorPort = g.addV()
				.property("aai-node-type", "p-interface")
				.property("interface-name", "p-int-collector-1")
				.next();

		Vertex bearerPort = g.addV()
				.property("aai-node-type", "p-interface")
				.property("interface-name", "p-int-bearer-1")
				.next();

		Vertex servInst4 = g.addV()
				.property("aai-node-type", "service-instance")
				.property("service-instance-id", "evc-name-4")
				.next();

		// graph 1
		edgeSerializer.addTreeEdge(g, customer1, servSub1);
//		edgeSerializer.addTreeEdge(g, servSub1, servInst1);
		edgeSerializer.addTreeEdge(g, customer2, servSub2);
		edgeSerializer.addTreeEdge(g, servSub2, servInst2);
		edgeSerializer.addTreeEdge(g, servSub1, servInst4); //evc-name-4 exists in graph as a child of SAREA serv-sub
		edgeSerializer.addTreeEdge(g, collectorPnf, collectorPort);
		edgeSerializer.addTreeEdge(g, bearerPnf, bearerPort);

		TransactionalGraphEngine spy = spy(dbEngine);
		TransactionalGraphEngine.Admin adminSpy = spy(dbEngine.asAdmin());

		GraphTraversalSource traversal = g;
		GraphTraversalSource readOnly = graph.traversal().withStrategies(ReadOnlyStrategy.instance());
		when (spy.tx()).thenReturn(tx);
		when(spy.asAdmin()).thenReturn(adminSpy);
		when(adminSpy.getTraversalSource()).thenReturn(traversal);
		when(adminSpy.getReadOnlyTraversalSource()).thenReturn(readOnly);

		migration = new MigrateSAREvcInventory(spy, loaderFactory, edgeIngestor, edgeSerializer, schemaVersions);
		migration.run();
	}

	@AfterEach
	public void cleanUp() {
		tx.tx().rollback();
		graph.close();
	}

	@Disabled
	@Test
	public void testRun_createServiceInstanceNode() throws Exception {
		// check if graph nodes exist
		assertEquals(true,
				g.V().has("service-instance-id", "evc-name-1")
				.hasNext(),
				"service instance node exists");

		// check if service-instance node gets created
		assertEquals(true,
				g.V().has("service-instance-id", "evc-name-1")
				.out("org.onap.relationships.inventory.BelongsTo").has("service-type", "SAREA")
				.hasNext(),
				"service subscription node, service-type=SAREA");



		// check if fowarding-path node gets created
		assertEquals(true, g.V().has("forwarding-path-id", "evc-name-1")
				.has("forwarding-path-name", "evc-name-1").hasNext(), "fowarding-path is created");

		assertEquals(true,
				g.V().has("global-customer-id", "8a00890a-e6ae-446b-9dbe-b828dbeb38bd")
				.in("org.onap.relationships.inventory.BelongsTo").has("service-type", "SAREA")
				.in("org.onap.relationships.inventory.BelongsTo").has("service-instance-id", "evc-name-1")
				.in("org.onap.relationships.inventory.AppliesTo")
				.has("aai-node-type", "forwarding-path")
				.has("forwarding-path-id", "evc-name-1")
				.has("forwarding-path-name", "evc-name-1")
				.hasNext(),
				"fowarding-path node exists");

		// check if configuration node gets created
		assertEquals(true,
				g.V().has("global-customer-id", "8a00890a-e6ae-446b-9dbe-b828dbeb38bd")
				.in("org.onap.relationships.inventory.BelongsTo").has("service-type", "SAREA")
				.in("org.onap.relationships.inventory.BelongsTo").has("service-instance-id", "evc-name-1")
				.in("org.onap.relationships.inventory.AppliesTo").has("aai-node-type", "forwarding-path")
				.out("org.onap.relationships.inventory.Uses").has("aai-node-type", "configuration")
				.has("configuration-type", "forwarding-path")
				.has("configuration-sub-type", "evc")
				.hasNext(),
				"configuration node, configuration-type= forwarding-path");

		//check if evc node gets created
		assertEquals(true,
				g.V().has("global-customer-id", "8a00890a-e6ae-446b-9dbe-b828dbeb38bd")
				.in("org.onap.relationships.inventory.BelongsTo").has("service-type", "SAREA")
				.in("org.onap.relationships.inventory.BelongsTo").has("service-instance-id", "evc-name-1")
				.in("org.onap.relationships.inventory.AppliesTo").has("aai-node-type", "forwarding-path")
				.out("org.onap.relationships.inventory.Uses").has("aai-node-type", "configuration")
				.in("org.onap.relationships.inventory.BelongsTo").has("aai-node-type", "evc")
				.hasNext(),
				"evc is created");

		// check if evc node gets created
		assertEquals(true,
				g.V().has("global-customer-id", "8a00890a-e6ae-446b-9dbe-b828dbeb38bd")
				.in("org.onap.relationships.inventory.BelongsTo").has("service-type", "SAREA")
				.in("org.onap.relationships.inventory.BelongsTo").has("service-instance-id", "evc-name-1")
				.in("org.onap.relationships.inventory.AppliesTo").has("aai-node-type", "forwarding-path")
				.out("org.onap.relationships.inventory.Uses").has("aai-node-type", "configuration")
				.in("org.onap.relationships.inventory.BelongsTo").has("aai-node-type", "evc")
				.has("evc-id", "evc-name-1")
				.has("forwarding-path-topology", "PointToPoint")
				.has("cir-value", "40")
				.has("cir-units", "Mbps")
				.has("tagmode-access-ingress", "DOUBLE")
				.has("tagmode-access-egress", "DOUBLE")
				.hasNext(),
				"configuration node, configuration-type= evc");
	}

	@Test
	public void testRun_evcNotCreated() throws Exception {
		// check if graph nodes exist
		assertEquals(true,
				g.V().has("global-customer-id", "8a00890a-e6ae-446b-9dbe-b828dbeb38bd")
				.hasNext(),
				"customer node exists");

		assertEquals(true,
				g.V().has("global-customer-id", "8a00890a-e6ae-446b-9dbe-b828dbeb38bd")
				.in("org.onap.relationships.inventory.BelongsTo").has("service-type", "SAREA")
				.hasNext(),
				"service subscription node, service-type=SAREA");

		//service-instance should not be created
		assertEquals(false,
				g.V().has("global-customer-id", "8a00890a-e6ae-446b-9dbe-b828dbeb38bd")
				.in("org.onap.relationships.inventory.BelongsTo").has("service-type", "SAREA")
				.in("org.onap.relationships.inventory.BelongsTo").has("service-instance-id", "evc-name-2")
				.hasNext(),
				"service instance node created");

		assertEquals(true,
				g.V().has("global-customer-id", "cust-1")
				.in("org.onap.relationships.inventory.BelongsTo").has("service-type", "SAREA")
				.in("org.onap.relationships.inventory.BelongsTo").has("service-instance-id", "evc-name-1")
				.hasNext(),
				"service instance node already exists");

		// fowarding-path node should not be created
		assertEquals(false, g.V().has("aai-node-type", "forwarding-path")
				.has("forwarding-path-name", "evc-name-2").hasNext(), "fowarding-path created");

		// configuration node should not be created
		assertEquals(false, g.V().has("aai-node-type", "configuration")
				.has("configuration-id", "evc-name-2").hasNext(), "configuration node created");

		// evc node should not be created
		assertEquals(false, g.V().has("aai-node-type", "evc")
				.has("evc-id", "evc-name-2").hasNext(), "evc node created");

		// service-instance is not created because pnf exists, but p-interface does not
		assertEquals(false,
				g.V().has("global-customer-id", "8a00890a-e6ae-446b-9dbe-b828dbeb38bd")
				.in("org.onap.relationships.inventory.BelongsTo").has("service-type", "SAREA")
				.in("org.onap.relationships.inventory.BelongsTo").has("service-instance-id", "evc-name-3")
				.hasNext(),
				"service instance node created");
	}

	@Disabled
	@Test
	public void testRun_createFPConfigurationEvcNode4() throws Exception {
		// check if graph nodes exist
		assertEquals(true,
				g.V().has("service-instance-id", "evc-name-4")
				.hasNext(),
				"service instance node exists");

		// check if service-instance node gets created
		assertEquals(true,
				g.V().has("service-instance-id", "evc-name-4")
				.out("org.onap.relationships.inventory.BelongsTo").has("service-type", "SAREA")
				.hasNext(),
				"service subscription node, service-type=SAREA");



		// check if fowarding-path node gets created
		assertEquals(true, g.V().has("forwarding-path-id", "evc-name-4")
				.has("forwarding-path-name", "evc-name-4").hasNext(), "fowarding-path is created");

		assertEquals(true,
				g.V().has("global-customer-id", "8a00890a-e6ae-446b-9dbe-b828dbeb38bd")
				.in("org.onap.relationships.inventory.BelongsTo").has("service-type", "SAREA")
				.in("org.onap.relationships.inventory.BelongsTo").has("service-instance-id", "evc-name-4")
				.in("org.onap.relationships.inventory.AppliesTo")
				.has("aai-node-type", "forwarding-path")
				.has("forwarding-path-id", "evc-name-4")
				.has("forwarding-path-name", "evc-name-4")
				.hasNext(),
				"fowarding-path node exists");

		// check if configuration node gets created
		assertEquals(true,
				g.V().has("global-customer-id", "8a00890a-e6ae-446b-9dbe-b828dbeb38bd")
				.in("org.onap.relationships.inventory.BelongsTo").has("service-type", "SAREA")
				.in("org.onap.relationships.inventory.BelongsTo").has("service-instance-id", "evc-name-4")
				.in("org.onap.relationships.inventory.AppliesTo").has("aai-node-type", "forwarding-path")
				.out("org.onap.relationships.inventory.Uses").has("aai-node-type", "configuration")
				.has("configuration-type", "forwarding-path")
				.has("configuration-sub-type", "evc")
				.hasNext(),
				"configuration node, configuration-type= forwarding-path");

		//check if evc node gets created
		assertEquals(true,
				g.V().has("global-customer-id", "8a00890a-e6ae-446b-9dbe-b828dbeb38bd")
				.in("org.onap.relationships.inventory.BelongsTo").has("service-type", "SAREA")
				.in("org.onap.relationships.inventory.BelongsTo").has("service-instance-id", "evc-name-4")
				.in("org.onap.relationships.inventory.AppliesTo").has("aai-node-type", "forwarding-path")
				.out("org.onap.relationships.inventory.Uses").has("aai-node-type", "configuration")
				.in("org.onap.relationships.inventory.BelongsTo").has("aai-node-type", "evc")
				.hasNext(),
				"evc is created");

		// check if evc node gets created
		assertEquals(true,
				g.V().has("global-customer-id", "8a00890a-e6ae-446b-9dbe-b828dbeb38bd")
				.in("org.onap.relationships.inventory.BelongsTo").has("service-type", "SAREA")
				.in("org.onap.relationships.inventory.BelongsTo").has("service-instance-id", "evc-name-1")
				.in("org.onap.relationships.inventory.AppliesTo").has("aai-node-type", "forwarding-path")
				.out("org.onap.relationships.inventory.Uses").has("aai-node-type", "configuration")
				.in("org.onap.relationships.inventory.BelongsTo").has("aai-node-type", "evc")
				.has("evc-id", "evc-name-1")
				.has("forwarding-path-topology", "PointToPoint")
				.has("cir-value", "40")
				.has("cir-units", "Mbps")
				.has("tagmode-access-ingress", "DOUBLE")
				.has("tagmode-access-egress", "DOUBLE")
				.hasNext(),
				"configuration node, configuration-type= evc");
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
		assertEquals("MigrateSAREvcInventory", migrationName);
	}
}
