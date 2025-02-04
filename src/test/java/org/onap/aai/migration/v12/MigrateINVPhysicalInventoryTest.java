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

import org.janusgraph.core.JanusGraphFactory;
import org.janusgraph.core.JanusGraph;
import org.janusgraph.core.JanusGraphTransaction;
import org.apache.tinkerpop.gremlin.process.traversal.dsl.graph.GraphTraversalSource;
import org.apache.tinkerpop.gremlin.process.traversal.strategy.verification.ReadOnlyStrategy;
import org.apache.tinkerpop.gremlin.structure.Vertex;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.onap.aai.AAISetup;
import org.onap.aai.introspection.Loader;
import org.onap.aai.introspection.ModelType;
import org.onap.aai.serialization.engines.QueryStyle;
import org.onap.aai.serialization.engines.JanusGraphDBEngine;
import org.onap.aai.serialization.engines.TransactionalGraphEngine;

import java.util.Optional;

import static org.junit.jupiter.api.Assertions.*;
import static org.mockito.Mockito.spy;
import static org.mockito.Mockito.when;

public class MigrateINVPhysicalInventoryTest extends AAISetup {

	private final static ModelType introspectorFactoryType = ModelType.MOXY;
	private final static QueryStyle queryStyle = QueryStyle.TRAVERSAL;

	private Loader loader;
	private TransactionalGraphEngine dbEngine;
	private JanusGraph graph;
	private MigrateINVPhysicalInventory migration;
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
		
		
		Vertex pnf1 = g.addV()
				.property("aai-node-type", "pnf")
				.property("pnf-name", "pnf-name-1")
				.property("aai-uri", "/network/pnfs/pnf/pnf-name-1")
				.next();
		Vertex  port11 = g.addV()
				.property("aai-node-type", "p-interface")
				.property("interface-name", "1.1")
				.property("aai-uri", "/network/pnfs/pnf/pnf-name-1/p-interfaces/pinterface/1.1")
				.next();
		// graph 1
				
		edgeSerializer.addTreeEdge(g, pnf1, port11);


		TransactionalGraphEngine spy = spy(dbEngine);
		TransactionalGraphEngine.Admin adminSpy = spy(dbEngine.asAdmin());

		GraphTraversalSource traversal = g;
		GraphTraversalSource readOnly = graph.traversal().withStrategies(ReadOnlyStrategy.instance());
		when (spy.tx()).thenReturn(tx);
		when(spy.asAdmin()).thenReturn(adminSpy);
		when(adminSpy.getTraversalSource()).thenReturn(traversal);
		when(adminSpy.getReadOnlyTraversalSource()).thenReturn(readOnly);
		
		migration = new MigrateINVPhysicalInventory(spy, loaderFactory, edgeIngestor, edgeSerializer, schemaVersions);
		migration.run();
	}
	
	@AfterEach
	public void cleanUp() {
		tx.tx().rollback();
		graph.close();
	}


	@Test
	public void pnfsExistTest() throws Exception {
		// check if pnf node gets created
		assertEquals(new Long(2L),
				g.V().has("aai-node-type", "pnf")
						.count().next(),
				"2 PNFs exist");
	}

	@Test
	public void pInterfacesExistTest() throws Exception {

		assertEquals(new Long(4L),
				g.V().has("aai-node-type", "p-interface")
						.count().next(),
				"4 Pinterfaces exist");
	}

	@Test
	public void testRun_checkPnfsAndPInterfacesExist() throws Exception {
		// check if graph nodes exist
		
		// check if pnf node gets created
		assertEquals(new Long(2L), 
				g.V().has("aai-node-type", "pnf")
				.count().next(), 
				"2 PNFs exist");
		
		System.out.println("cOUNT:" +g.V().has("aai-node-type", "pnf")
				.has("pnf-name", "pnf-name-collector-1").in("tosca.relationships.network.BindsTo").count().next());
				
		assertEquals(new Long(1L),
				g.V().has("aai-node-type", "pnf")
				.has("pnf-name", "pnf-name-collector-1").count().next(),
				"p-interfaces created for pnfs");
		
		assertEquals(true,
				g.V().has("aai-node-type", "pnf")
				.has("pnf-name", "pnf-name-collector-1")
				.in("tosca.relationships.network.BindsTo")
				.has("interface-name","1.7")
				.hasNext(),
				"p-interface 1.7 created for pnf-name-collector-1");
		assertEquals(new Long(2L),
				g.V().has("aai-node-type", "pnf")
				.has("pnf-name", "pnf-name-1")
				.in("tosca.relationships.network.BindsTo").count().next(),
				"p-interfaces created for pnfs");
	}
	
	@Test
	public void testGetAffectedNodeTypes() {
		Optional<String[]> types = migration.getAffectedNodeTypes();
		Optional<String[]> expected = Optional.of(new String[]{"pnf"});
		
		assertNotNull(types);
		assertArrayEquals(expected.get(), types.get());
	}

	@Test
	public void testGetMigrationName() {
		String migrationName = migration.getMigrationName();

		assertNotNull(migrationName);
		assertEquals("MigrateINVPhysicalInventory", migrationName);
	}
}
