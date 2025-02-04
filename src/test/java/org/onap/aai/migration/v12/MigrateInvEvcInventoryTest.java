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

public class MigrateInvEvcInventoryTest extends AAISetup {

	private final static ModelType introspectorFactoryType = ModelType.MOXY;
	private final static QueryStyle queryStyle = QueryStyle.TRAVERSAL;

	private static Loader loader;
	private static TransactionalGraphEngine dbEngine;
	private static JanusGraph graph;
	private static MigrateINVEvcInventory migration;
	private static JanusGraphTransaction tx;
	private static GraphTraversalSource g;

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
		
		Vertex evc = g.addV()
				.property("aai-node-type", "evc")
				.property("evc-id", "evc-name-1")
				.next();
		
		Vertex evc2 = g.addV()
				.property("aai-node-type", "evc")
				.property("evc-id", "evc-name-2")
				.next();
		
		TransactionalGraphEngine spy = spy(dbEngine);
		TransactionalGraphEngine.Admin adminSpy = spy(dbEngine.asAdmin());

		GraphTraversalSource traversal = g;
		GraphTraversalSource readOnly = graph.traversal().withStrategies(ReadOnlyStrategy.instance());
		when (spy.tx()).thenReturn(tx);
		when(spy.asAdmin()).thenReturn(adminSpy);
		when(adminSpy.getTraversalSource()).thenReturn(traversal);
		when(adminSpy.getReadOnlyTraversalSource()).thenReturn(readOnly);
		
		migration = new MigrateINVEvcInventory(spy, loaderFactory, edgeIngestor, edgeSerializer, schemaVersions);
		migration.run();
	}
	
	@AfterEach
	public void cleanUp() {
		tx.tx().rollback();
		graph.close();
	}
	
	@Test
	public void testRun_updateEvcNode() throws Exception {
		// check if graph nodes exist
		assertEquals(true, 
				g.V().has("aai-node-type", "evc")
					 .has("evc-id", "evc-name-1")
				.hasNext(), 
				"evc node exists");
		
		// check if evc object is updated to set the value for inter-connect-type-ingress
		assertEquals(true, 
				g.V().has("aai-node-type", "evc").has("evc-id", "evc-name-1")
				.has("inter-connect-type-ingress", "SHARED")
				.hasNext(), 
				"evc is updated");
	}
	
	@Test
	public void testRun_evcNotCreated() throws Exception {
		
		assertEquals(false, 
				g.V().has("aai-node-type", "evc").has("evc-id", "evc-name-3")
				.hasNext(), 
				"evc node does not exist");
		
		//inter-connect-type-ingress is not present on the evc
		assertEquals(true, 
				g.V().has("aai-node-type", "evc").has("evc-id", "evc-name-2")
				.hasNext(), 
				"evc node exists");
		assertEquals(false, 
				g.V().has("aai-node-type", "evc").has("evc-id", "evc-name-2").has("inter-connect-type-ingress")
				.hasNext(), 
				"evc node not updated with inter-connect-type-ingress");
		
	}

	@Test
	public void testGetAffectedNodeTypes() {
		Optional<String[]> types = migration.getAffectedNodeTypes();
		Optional<String[]> expected = Optional.of(new String[]{"evc"});
		
		assertNotNull(types);
		assertArrayEquals(expected.get(), types.get());
	}

	@Test
	public void testGetMigrationName() {
		String migrationName = migration.getMigrationName();

		assertNotNull(migrationName);
		assertEquals("MigrateINVEvcInventory", migrationName);
	}
}
