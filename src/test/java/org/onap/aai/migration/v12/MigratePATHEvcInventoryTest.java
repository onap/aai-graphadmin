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

public class MigratePATHEvcInventoryTest extends AAISetup {

	private final static ModelType introspectorFactoryType = ModelType.MOXY;
	private final static QueryStyle queryStyle = QueryStyle.TRAVERSAL;

	private Loader loader;
	private TransactionalGraphEngine dbEngine;
	private MigratePATHEvcInventory migration;
	private GraphTraversalSource g;

	@Before
	public void setUp() throws Exception {
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
		
		Vertex pnf1 = g.addV()
				.property("aai-node-type", "pnf")
				.property("pnf-name", "pnf-name-1")
				.next();
		Vertex  port11 = g.addV()
				.property("aai-node-type", "p-interface")
				.property("interface-name", "1.1")
				.next();
		Vertex  port12 = g.addV()
				.property("aai-node-type", "p-interface")
				.property("interface-name", "1.41")
				.next();
		
		Vertex pnf2 = g.addV()
				.property("aai-node-type", "pnf")
				.property("pnf-name", "pnf-name-2")
				.next();
		Vertex  port21 = g.addV()
				.property("aai-node-type", "p-interface")
				.property("interface-name", "1.25")
				.next();
		Vertex  port22 = g.addV()
				.property("aai-node-type", "lag-interface")
				.property("interface-name", "ae1")
				.next();
		
		Vertex pnf3 = g.addV()
				.property("aai-node-type", "pnf")
				.property("pnf-name", "pnf-name-3")
				.next();
		Vertex  port31 = g.addV()
				.property("aai-node-type", "p-interface")
				.property("interface-name", "1.32")
				.next();
		Vertex  port32 = g.addV()
				.property("aai-node-type", "lag-interface")
				.property("interface-name", "ae1")
				.next();
		
		Vertex pnf4 = g.addV()
				.property("aai-node-type", "pnf")
				.property("pnf-name", "pnf-name-4")
				.next();
		Vertex  port41 = g.addV()
				.property("aai-node-type", "p-interface")
				.property("interface-name", "1.7")
				.next();
		Vertex  port42 = g.addV()
				.property("aai-node-type", "lag-interface")
				.property("interface-name", "ae101")
				.next();
		
		Vertex pnf5 = g.addV()
				.property("aai-node-type", "pnf")
				.property("pnf-name", "pnf-name-5")
				.next();
		Vertex  port51 = g.addV()
				.property("aai-node-type", "lag-interface")
				.property("interface-name", "104")
				.next();
		Vertex  port52 = g.addV()
				.property("aai-node-type", "lag-interface")
				.property("interface-name", "101")
				.next();
		
		Vertex pnf6 = g.addV()
				.property("aai-node-type", "pnf")
				.property("pnf-name", "pnf-name-6")
				.next();
		Vertex  port61 = g.addV()
				.property("aai-node-type", "lag-interface")
				.property("interface-name", "ae104")
				.next();
		Vertex  port62 = g.addV()
				.property("aai-node-type", "p-interface")
				.property("interface-name", "1.39")
				.next();
		
		Vertex evc1 = g.addV().property("aai-node-type", "evc")
				.property("evc-id", "evc-name-1")
				.next();
		Vertex fp1 =  g.addV()
				.property("aai-node-type", "forwarding-path")
				.property("forwarding-path-id", "evc-name-1")
				.next();
		
		Vertex evc2 = g.addV().property("aai-node-type", "evc")
				.property("evc-id", "evc-name-2")
				.next();
		Vertex fp2 =  g.addV()
				.property("aai-node-type", "forwarding-path")
				.property("forwarding-path-id", "evc-name-2")
				.next();
		
		Vertex evc3 = g.addV().property("aai-node-type", "evc")
				.property("evc-id", "evc-name-3")
				.next();
		Vertex fp3 =  g.addV()
				.property("aai-node-type", "forwarding-path")
				.property("forwarding-path-id", "evc-name-3")
				.next();
		
		// graph 1
		edgeSerializer.addTreeEdge(g, customer1, servSub1);
		edgeSerializer.addTreeEdge(g, servSub1, servInstance1);
		edgeSerializer.addTreeEdge(g, servSub1, servInstance2);
		edgeSerializer.addTreeEdge(g, servSub1, servInstance3);
		
		edgeSerializer.addEdge(g, servInstance1, fp1);
		edgeSerializer.addEdge(g, servInstance2, fp2);
		edgeSerializer.addEdge(g, servInstance3, fp3);
		
		edgeSerializer.addTreeEdge(g, pnf1, port11);
		edgeSerializer.addTreeEdge(g, pnf1, port12);
		
		edgeSerializer.addTreeEdge(g, pnf2, port21);
		edgeSerializer.addTreeEdge(g, pnf2, port22);
		
		edgeSerializer.addTreeEdge(g, pnf3, port31);
		edgeSerializer.addTreeEdge(g, pnf3, port32);
		
		edgeSerializer.addTreeEdge(g, pnf4, port41);
		edgeSerializer.addTreeEdge(g, pnf4, port42);
		
		edgeSerializer.addTreeEdge(g, pnf5, port51);
		edgeSerializer.addTreeEdge(g, pnf5, port52);
		
		edgeSerializer.addTreeEdge(g, pnf6, port61);
		edgeSerializer.addTreeEdge(g, pnf6, port62);
		


		TransactionalGraphEngine spy = spy(dbEngine);
		TransactionalGraphEngine.Admin adminSpy = spy(dbEngine.asAdmin());

		GraphTraversalSource traversal = g;
		GraphTraversalSource readOnly = graph.traversal().withStrategies(ReadOnlyStrategy.instance());
		when (spy.tx()).thenReturn(tx);
		when(spy.asAdmin()).thenReturn(adminSpy);
		when(adminSpy.getTraversalSource()).thenReturn(traversal);
		when(adminSpy.getReadOnlyTraversalSource()).thenReturn(readOnly);
		
		migration = new MigratePATHEvcInventory(spy, loaderFactory, edgeIngestor, edgeSerializer, schemaVersions);
		migration.run();
	}

	@Test
	public void testRun_checkServInstanceAndForwardingPathsExist() throws Exception {
		// check if graph nodes exist
		
		// check if service-instance node gets created
		assertEquals("service subscription node, service-type=SAREA", true, 
				g.V().has("service-instance-id", "evc-name-1")
				.out("org.onap.relationships.inventory.BelongsTo").has("service-type", "SAREA")
				.hasNext());
		
		assertEquals("fowarding-path node exists", true, 
				g.V().has("global-customer-id", "customer-id-1")
				.in("org.onap.relationships.inventory.BelongsTo").has("service-type", "SAREA")
				.in("org.onap.relationships.inventory.BelongsTo").has("service-instance-id", "evc-name-1")
				.in("org.onap.relationships.inventory.AppliesTo")
				.has("aai-node-type", "forwarding-path")
				.has("forwarding-path-id", "evc-name-1")
				.hasNext());
		assertEquals("fowarding-path node exists", true, 
				g.V().has("global-customer-id", "customer-id-1")
				.in("org.onap.relationships.inventory.BelongsTo").has("service-type", "SAREA")
				.in("org.onap.relationships.inventory.BelongsTo").has("service-instance-id", "evc-name-2")
				.in("org.onap.relationships.inventory.AppliesTo")
				.has("aai-node-type", "forwarding-path")
				.has("forwarding-path-id", "evc-name-2")
				.hasNext());
		assertEquals("fowarding-path node exists", true, 
				g.V().has("global-customer-id", "customer-id-1")
				.in("org.onap.relationships.inventory.BelongsTo").has("service-type", "SAREA")
				.in("org.onap.relationships.inventory.BelongsTo").has("service-instance-id", "evc-name-3")
				.in("org.onap.relationships.inventory.AppliesTo")
				.has("aai-node-type", "forwarding-path")
				.has("forwarding-path-id", "evc-name-3")
				.hasNext());
		
	}

	@Ignore
	@Test
	public void testRun_checkForwardersForEvc1AreCreated() throws Exception {
		// check if graph nodes exist
		// check if forwarder node gets created
		
		assertEquals("forwarder node is created for evc-name-1 ", true,
		g.V().has("global-customer-id", "customer-id-1")
		.in("org.onap.relationships.inventory.BelongsTo").has("service-type", "SAREA")
		.in("org.onap.relationships.inventory.BelongsTo").has("service-instance-id", "evc-name-1")
		.in("org.onap.relationships.inventory.AppliesTo").has("aai-node-type", "forwarding-path")
		.in("org.onap.relationships.inventory.BelongsTo").has("aai-node-type", "forwarder")
		.has("sequence", 1)
		.has("forwarder-role", "ingress")
		.hasNext());
		
		assertEquals("forwarder node is created for evc-name-1 ", true,
				g.V().has("global-customer-id", "customer-id-1")
				.in("org.onap.relationships.inventory.BelongsTo").has("service-type", "SAREA")
				.in("org.onap.relationships.inventory.BelongsTo").has("service-instance-id", "evc-name-1")
				.in("org.onap.relationships.inventory.AppliesTo").has("aai-node-type", "forwarding-path")
				.in("org.onap.relationships.inventory.BelongsTo").has("aai-node-type", "forwarder")
				.has("sequence", 2)
				.has("forwarder-role", "egress")
				.hasNext());
	}

	@Ignore
	@Test
	public void testRun_checkForwardersForEvc2AreCreated() throws Exception {
		
		// check if forwarder node gets created
		
		assertEquals("forwarder node is created for evc-name-2 ", true,
				g.V().has("global-customer-id", "customer-id-1")
				.in("org.onap.relationships.inventory.BelongsTo").has("service-type", "SAREA")
				.in("org.onap.relationships.inventory.BelongsTo").has("service-instance-id", "evc-name-2")
				.hasNext());
		
		assertEquals("forwarder node is created for evc-name-2 ", true,
				g.V().has("global-customer-id", "customer-id-1")
				.in("org.onap.relationships.inventory.BelongsTo").has("service-type", "SAREA")
				.in("org.onap.relationships.inventory.BelongsTo").has("service-instance-id", "evc-name-2")
				.in("org.onap.relationships.inventory.AppliesTo").has("aai-node-type", "forwarding-path")
				.hasNext());
		
		assertEquals("4 forwarders are created for evc-name-2 ", (Long)4l,
				g.V().
					has("aai-node-type", "forwarding-path").has("forwarding-path-id","evc-name-2")
					.in("org.onap.relationships.inventory.BelongsTo")
					.has("aai-node-type", "forwarder").count().next()); //org.onap.relationships.inventory.BelongsTo
		
		assertEquals("forwarder node is created for evc-name-2 ", true,
		g.V().has("aai-node-type", "forwarding-path").has("forwarding-path-id","evc-name-2")
				.in("org.onap.relationships.inventory.BelongsTo").has("aai-node-type", "forwarder")
				.has("sequence", 1)
				.has("forwarder-role", "ingress")
				.hasNext());
		
		assertEquals("forwarder node is created for evc-name-2 ", true,
		g.V().has("global-customer-id", "customer-id-1")
		.in("org.onap.relationships.inventory.BelongsTo").has("service-type", "SAREA")
		.in("org.onap.relationships.inventory.BelongsTo").has("service-instance-id", "evc-name-2")
		.in("org.onap.relationships.inventory.AppliesTo").has("aai-node-type", "forwarding-path")
		.in("org.onap.relationships.inventory.BelongsTo").has("aai-node-type", "forwarder")
		.has("sequence", 1)
		.has("forwarder-role", "ingress")
		.hasNext());
		
		assertEquals("forwarder node is created for evc-name-2 ", true,
				g.V().has("global-customer-id", "customer-id-1")
				.in("org.onap.relationships.inventory.BelongsTo").has("service-type", "SAREA")
				.in("org.onap.relationships.inventory.BelongsTo").has("service-instance-id", "evc-name-2")
				.in("org.onap.relationships.inventory.AppliesTo").has("aai-node-type", "forwarding-path")
				.in("org.onap.relationships.inventory.BelongsTo").has("aai-node-type", "forwarder")
				.has("sequence", 2)
				.has("forwarder-role", "intermediate")
				.hasNext());
		
		assertEquals("forwarder node is created for evc-name-2 ", true,
				g.V().has("global-customer-id", "customer-id-1")
				.in("org.onap.relationships.inventory.BelongsTo").has("service-type", "SAREA")
				.in("org.onap.relationships.inventory.BelongsTo").has("service-instance-id", "evc-name-2")
				.in("org.onap.relationships.inventory.AppliesTo").has("aai-node-type", "forwarding-path")
				.in("org.onap.relationships.inventory.BelongsTo").has("aai-node-type", "forwarder")
				.has("sequence", 3)
				.has("forwarder-role", "intermediate")
				.hasNext());
		
		assertEquals("forwarder node is created for evc-name-2 ", true,
				g.V().has("global-customer-id", "customer-id-1")
				.in("org.onap.relationships.inventory.BelongsTo").has("service-type", "SAREA")
				.in("org.onap.relationships.inventory.BelongsTo").has("service-instance-id", "evc-name-2")
				.in("org.onap.relationships.inventory.AppliesTo").has("aai-node-type", "forwarding-path")
				.in("org.onap.relationships.inventory.BelongsTo").has("aai-node-type", "forwarder")
				.has("sequence", 4)
				.has("forwarder-role", "egress")
				.hasNext());
	}

	@Ignore
	@Test
	public void testRun_checkForwardersForEvc3AreCreated() throws Exception {
		
		// check if forwarder node gets created
		
		assertEquals("forwarder node is created for evc-name-3 ", true,
		g.V().has("global-customer-id", "customer-id-1")
		.in("org.onap.relationships.inventory.BelongsTo").has("service-type", "SAREA")
		.in("org.onap.relationships.inventory.BelongsTo").has("service-instance-id", "evc-name-3")
		.in("org.onap.relationships.inventory.AppliesTo").has("aai-node-type", "forwarding-path")
		.in("org.onap.relationships.inventory.BelongsTo").has("aai-node-type", "forwarder")
		.has("sequence", 1)
		.has("forwarder-role", "ingress")
		.hasNext());
		
		assertEquals("forwarder node is created for evc-name-3 ", true,
				g.V().has("global-customer-id", "customer-id-1")
				.in("org.onap.relationships.inventory.BelongsTo").has("service-type", "SAREA")
				.in("org.onap.relationships.inventory.BelongsTo").has("service-instance-id", "evc-name-3")
				.in("org.onap.relationships.inventory.AppliesTo").has("aai-node-type", "forwarding-path")
				.in("org.onap.relationships.inventory.BelongsTo").has("aai-node-type", "forwarder")
				.has("sequence", 1)
				.has("forwarder-role", "ingress")
				.out("org.onap.relationships.inventory.ForwardsTo")
				.has("aai-node-type", "p-interface")
				.has("interface-name","1.7")
				.hasNext());
		
		assertEquals("forwarder-evc node is created for forwarder with sequence 1 ", true,
				g.V().has("global-customer-id", "customer-id-1")
				.in("org.onap.relationships.inventory.BelongsTo").has("service-type", "SAREA")
				.in("org.onap.relationships.inventory.BelongsTo").has("service-instance-id", "evc-name-3")
				.in("org.onap.relationships.inventory.AppliesTo").has("aai-node-type", "forwarding-path")
				.in("org.onap.relationships.inventory.BelongsTo").has("aai-node-type", "forwarder")
				.has("sequence", 1)
				.has("forwarder-role", "ingress")
				.out("org.onap.relationships.inventory.Uses").has("aai-node-type", "configuration")
				.has("configuration-id","evc-name-3-1").has("configuration-type","forwarder").has("configuration-sub-type", "forwarder")
				.in("org.onap.relationships.inventory.BelongsTo").has("aai-node-type", "forwarder-evc")
				.has("forwarder-evc-id","evc-name-3-1")
				.has("circuit-id","M0651881")
				.has("cvlan","34")
				.has("svlan","8")
				.hasNext());
		
		assertEquals("forwarder node is created for evc-name-3 ", true,
				g.V().has("global-customer-id", "customer-id-1")
				.in("org.onap.relationships.inventory.BelongsTo").has("service-type", "SAREA")
				.in("org.onap.relationships.inventory.BelongsTo").has("service-instance-id", "evc-name-3")
				.in("org.onap.relationships.inventory.AppliesTo").has("aai-node-type", "forwarding-path")
				.in("org.onap.relationships.inventory.BelongsTo").has("aai-node-type", "forwarder")
				.has("sequence", 2)
				.has("forwarder-role", "intermediate")
				.hasNext());
		
		//forwarder to interface check
		assertEquals("forwarder node is created for evc-name-3 ", true,
				g.V().has("global-customer-id", "customer-id-1")
				.in("org.onap.relationships.inventory.BelongsTo").has("service-type", "SAREA")
				.in("org.onap.relationships.inventory.BelongsTo").has("service-instance-id", "evc-name-3")
				.in("org.onap.relationships.inventory.AppliesTo").has("aai-node-type", "forwarding-path")
				.in("org.onap.relationships.inventory.BelongsTo").has("aai-node-type", "forwarder")
				.has("sequence", 2)
				.has("forwarder-role", "intermediate")
				.out("org.onap.relationships.inventory.ForwardsTo")
				.has("aai-node-type", "lag-interface")
				.has("interface-name","ae101")
				.hasNext());
		
		assertEquals("forwarder-evc node is created for forwarder with sequence 2 ", true,
				g.V().has("global-customer-id", "customer-id-1")
				.in("org.onap.relationships.inventory.BelongsTo").has("service-type", "SAREA")
				.in("org.onap.relationships.inventory.BelongsTo").has("service-instance-id", "evc-name-3")
				.in("org.onap.relationships.inventory.AppliesTo").has("aai-node-type", "forwarding-path")
				.in("org.onap.relationships.inventory.BelongsTo").has("aai-node-type", "forwarder")
				.has("sequence", 2)
				.has("forwarder-role", "intermediate")
				.out("org.onap.relationships.inventory.Uses").has("aai-node-type", "configuration")
				.has("configuration-id","evc-name-3-2").has("configuration-type","forwarder").has("configuration-sub-type", "forwarder")
				.in("org.onap.relationships.inventory.BelongsTo").has("aai-node-type", "forwarder-evc")
				.has("forwarder-evc-id","evc-name-3-2")
				.has("cvlan","34")
				.has("svlan","740")
				.hasNext());
		
		assertEquals("forwarder node is created for evc-name-3 ", true,
				g.V().has("global-customer-id", "customer-id-1")
				.in("org.onap.relationships.inventory.BelongsTo").has("service-type", "SAREA")
				.in("org.onap.relationships.inventory.BelongsTo").has("service-instance-id", "evc-name-3")
				.in("org.onap.relationships.inventory.AppliesTo").has("aai-node-type", "forwarding-path")
				.in("org.onap.relationships.inventory.BelongsTo").has("aai-node-type", "forwarder")
				.has("sequence", 3)
				.has("forwarder-role", "intermediate")
				.hasNext());
		
		assertEquals("forwarder node is created for evc-name-3 ", true,
				g.V().has("global-customer-id", "customer-id-1")
				.in("org.onap.relationships.inventory.BelongsTo").has("service-type", "SAREA")
				.in("org.onap.relationships.inventory.BelongsTo").has("service-instance-id", "evc-name-3")
				.in("org.onap.relationships.inventory.AppliesTo").has("aai-node-type", "forwarding-path")
				.in("org.onap.relationships.inventory.BelongsTo").has("aai-node-type", "forwarder")
				.has("sequence", 3)
				.has("forwarder-role", "intermediate")
				.out("org.onap.relationships.inventory.ForwardsTo")
				.has("aai-node-type", "lag-interface")
				.has("interface-name","101")
				.hasNext());
		
		assertEquals("forwarder-evc node is created for forwarder with sequence 3 ", true,
				g.V().has("global-customer-id", "customer-id-1")
				.in("org.onap.relationships.inventory.BelongsTo").has("service-type", "SAREA")
				.in("org.onap.relationships.inventory.BelongsTo").has("service-instance-id", "evc-name-3")
				.in("org.onap.relationships.inventory.AppliesTo").has("aai-node-type", "forwarding-path")
				.in("org.onap.relationships.inventory.BelongsTo").has("aai-node-type", "forwarder")
				.has("sequence", 3)
				.has("forwarder-role", "intermediate")
				.out("org.onap.relationships.inventory.Uses").has("aai-node-type", "configuration")
				.has("configuration-id","evc-name-3-3").has("configuration-type","forwarder").has("configuration-sub-type", "forwarder")
				.in("org.onap.relationships.inventory.BelongsTo").has("aai-node-type", "forwarder-evc")
				.has("forwarder-evc-id","evc-name-3-3")
				.has("cvlan","35")
				.has("svlan","740")
				.hasNext());
		
		assertEquals("forwarder node is created for evc-name-3 ", true,
				g.V().has("global-customer-id", "customer-id-1")
				.in("org.onap.relationships.inventory.BelongsTo").has("service-type", "SAREA")
				.in("org.onap.relationships.inventory.BelongsTo").has("service-instance-id", "evc-name-3")
				.in("org.onap.relationships.inventory.AppliesTo").has("aai-node-type", "forwarding-path")
				.in("org.onap.relationships.inventory.BelongsTo").has("aai-node-type", "forwarder")
				.has("sequence", 4)
				.has("forwarder-role", "intermediate")
				.hasNext());
		
		assertEquals("forwarder node is created for evc-name-3 ", true,
				g.V().has("global-customer-id", "customer-id-1")
				.in("org.onap.relationships.inventory.BelongsTo").has("service-type", "SAREA")
				.in("org.onap.relationships.inventory.BelongsTo").has("service-instance-id", "evc-name-3")
				.in("org.onap.relationships.inventory.AppliesTo").has("aai-node-type", "forwarding-path")
				.in("org.onap.relationships.inventory.BelongsTo").has("aai-node-type", "forwarder")
				.has("sequence", 4)
				.has("forwarder-role", "intermediate")
				.out("org.onap.relationships.inventory.ForwardsTo")
				.has("aai-node-type", "lag-interface")
				.has("interface-name","104")
				.hasNext());
		
		assertEquals("forwarder-evc node is created for forwarder with sequence 4 ", true,
				g.V().has("global-customer-id", "customer-id-1")
				.in("org.onap.relationships.inventory.BelongsTo").has("service-type", "SAREA")
				.in("org.onap.relationships.inventory.BelongsTo").has("service-instance-id", "evc-name-3")
				.in("org.onap.relationships.inventory.AppliesTo").has("aai-node-type", "forwarding-path")
				.in("org.onap.relationships.inventory.BelongsTo").has("aai-node-type", "forwarder")
				.has("sequence", 4)
				.has("forwarder-role", "intermediate")
				.out("org.onap.relationships.inventory.Uses").has("aai-node-type", "configuration")
				.has("configuration-id","evc-name-3-4").has("configuration-type","forwarder").has("configuration-sub-type", "forwarder")
				.in("org.onap.relationships.inventory.BelongsTo").has("aai-node-type", "forwarder-evc")
				.has("forwarder-evc-id","evc-name-3-4")
				.has("cvlan","37")
				.has("svlan","740")
				.hasNext());
		
		assertEquals("forwarder node is created for evc-name-3 ", true,
				g.V().has("global-customer-id", "customer-id-1")
				.in("org.onap.relationships.inventory.BelongsTo").has("service-type", "SAREA")
				.in("org.onap.relationships.inventory.BelongsTo").has("service-instance-id", "evc-name-3")
				.in("org.onap.relationships.inventory.AppliesTo").has("aai-node-type", "forwarding-path")
				.in("org.onap.relationships.inventory.BelongsTo").has("aai-node-type", "forwarder")
				.has("sequence", 5)
				.has("forwarder-role", "intermediate")
				.hasNext());
		
		assertEquals("forwarder node is created for evc-name-3 ", true,
				g.V().has("global-customer-id", "customer-id-1")
				.in("org.onap.relationships.inventory.BelongsTo").has("service-type", "SAREA")
				.in("org.onap.relationships.inventory.BelongsTo").has("service-instance-id", "evc-name-3")
				.in("org.onap.relationships.inventory.AppliesTo").has("aai-node-type", "forwarding-path")
				.in("org.onap.relationships.inventory.BelongsTo").has("aai-node-type", "forwarder")
				.has("sequence", 5)
				.has("forwarder-role", "intermediate")
				.out("org.onap.relationships.inventory.ForwardsTo")
				.has("aai-node-type", "lag-interface")
				.has("interface-name","ae104")
				.hasNext());
		
		
		
		assertEquals("configuration node is created for forwarder with sequence 5 ", true,
				g.V().has("global-customer-id", "customer-id-1")
				.in("org.onap.relationships.inventory.BelongsTo").has("service-type", "SAREA")
				.in("org.onap.relationships.inventory.BelongsTo").has("service-instance-id", "evc-name-3")
				.in("org.onap.relationships.inventory.AppliesTo").has("aai-node-type", "forwarding-path")
				.in("org.onap.relationships.inventory.BelongsTo").has("aai-node-type", "forwarder")
				.has("sequence", 5)
				.has("forwarder-role", "intermediate")
				.out("org.onap.relationships.inventory.Uses").has("aai-node-type", "configuration")
				.has("configuration-id","evc-name-3-5").has("configuration-type","forwarder").has("configuration-sub-type", "forwarder")
				.hasNext());
		
		assertEquals("forwarder-evc node is created for forwarder with sequence 5 ", true,
				g.V().has("global-customer-id", "customer-id-1")
				.in("org.onap.relationships.inventory.BelongsTo").has("service-type", "SAREA")
				.in("org.onap.relationships.inventory.BelongsTo").has("service-instance-id", "evc-name-3")
				.in("org.onap.relationships.inventory.AppliesTo").has("aai-node-type", "forwarding-path")
				.in("org.onap.relationships.inventory.BelongsTo").has("aai-node-type", "forwarder")
				.has("sequence", 5)
				.has("forwarder-role", "intermediate")
				.out("org.onap.relationships.inventory.Uses").has("aai-node-type", "configuration")
				.has("configuration-id","evc-name-3-5").has("configuration-type","forwarder").has("configuration-sub-type", "forwarder")
				.in("org.onap.relationships.inventory.BelongsTo").has("aai-node-type", "forwarder-evc")
				.has("forwarder-evc-id","evc-name-3-5")
				.has("cvlan","36")
				.has("svlan","740")
				.hasNext());
		
		assertEquals("forwarder node is created for evc-name-3 ", true,
				g.V().has("global-customer-id", "customer-id-1")
				.in("org.onap.relationships.inventory.BelongsTo").has("service-type", "SAREA")
				.in("org.onap.relationships.inventory.BelongsTo").has("service-instance-id", "evc-name-3")
				.in("org.onap.relationships.inventory.AppliesTo").has("aai-node-type", "forwarding-path")
				.in("org.onap.relationships.inventory.BelongsTo").has("aai-node-type", "forwarder")
				.has("sequence", 6)
				.has("forwarder-role", "egress")
				.hasNext());
		
		assertEquals("forwarder node is created for evc-name-3 ", true,
				g.V().has("global-customer-id", "customer-id-1")
				.in("org.onap.relationships.inventory.BelongsTo").has("service-type", "SAREA")
				.in("org.onap.relationships.inventory.BelongsTo").has("service-instance-id", "evc-name-3")
				.in("org.onap.relationships.inventory.AppliesTo").has("aai-node-type", "forwarding-path")
				.in("org.onap.relationships.inventory.BelongsTo").has("aai-node-type", "forwarder")
				.has("sequence", 6)
				.has("forwarder-role", "egress")
				.out("org.onap.relationships.inventory.ForwardsTo")
				.has("aai-node-type", "p-interface")
				.has("interface-name","1.39")
				.hasNext());
		
		assertEquals("configuration node is created for forwarder with sequence 6 ", true,
				g.V().has("global-customer-id", "customer-id-1")
				.in("org.onap.relationships.inventory.BelongsTo").has("service-type", "SAREA")
				.in("org.onap.relationships.inventory.BelongsTo").has("service-instance-id", "evc-name-3")
				.in("org.onap.relationships.inventory.AppliesTo").has("aai-node-type", "forwarding-path")
				.in("org.onap.relationships.inventory.BelongsTo").has("aai-node-type", "forwarder")
				.has("sequence", 6)
				.has("forwarder-role", "egress")
				.out("org.onap.relationships.inventory.Uses").has("aai-node-type", "configuration")
				.has("configuration-id","evc-name-3-6").has("configuration-type","forwarder").has("configuration-sub-type", "forwarder")
				.hasNext());
		
		assertEquals("configuration node is created for forwarder with sequence 6 ", true,
				g.V().has("global-customer-id", "customer-id-1")
				.in("org.onap.relationships.inventory.BelongsTo").has("service-type", "SAREA")
				.in("org.onap.relationships.inventory.BelongsTo").has("service-instance-id", "evc-name-3")
				.in("org.onap.relationships.inventory.AppliesTo").has("aai-node-type", "forwarding-path")
				.in("org.onap.relationships.inventory.BelongsTo").has("aai-node-type", "forwarder")
				.has("sequence", 6)
				.has("forwarder-role", "egress")
				.out("org.onap.relationships.inventory.Uses").has("aai-node-type", "configuration")
				.has("configuration-id","evc-name-3-6").has("configuration-type","forwarder").has("configuration-sub-type", "forwarder")
				.in("org.onap.relationships.inventory.BelongsTo").has("aai-node-type", "forwarder-evc")
				.has("forwarder-evc-id","evc-name-3-6").has("circuit-id","IZEZ.597112..ATI").has("cvlan","36").has("svlan","3")
				.hasNext());
		
	}

	
	@Test
	public void testGetAffectedNodeTypes() {
		Optional<String[]> types = migration.getAffectedNodeTypes();
		Optional<String[]> expected = Optional.of(new String[]{"forwarding-path"});
		
		assertNotNull(types);
		assertArrayEquals(expected.get(), types.get());
	}

	@Test
	public void testGetMigrationName() {
		String migrationName = migration.getMigrationName();

		assertNotNull(migrationName);
		assertEquals("MigratePATHEvcInventory", migrationName);
	}
}
