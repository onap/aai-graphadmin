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
import static org.mockito.Matchers.shortThat;
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

public class MigratePATHPhysicalInventoryTest extends AAISetup {

	private final static ModelType introspectorFactoryType = ModelType.MOXY;
	private final static QueryStyle queryStyle = QueryStyle.TRAVERSAL;
	private final static DBConnectionType type = DBConnectionType.REALTIME;

	private Loader loader;
	private TransactionalGraphEngine dbEngine;
	private MigratePATHPhysicalInventory migration;
	private GraphTraversalSource g;

	@Before
	public void setUp() throws Exception {
		g = tx.traversal();
		loader = loaderFactory.createLoaderForVersion(introspectorFactoryType, schemaVersions.getDefaultVersion());
		dbEngine = new JanusGraphDBEngine(
				queryStyle,
				type,
				loader);

		Vertex pnf2 = g.addV()
				.property("aai-node-type", "pnf")
				.property("pnf-name", "pnf-name-2")
				.next();
		Vertex  port21 = g.addV()
				.property("aai-node-type", "lag-interface")
				.property("interface-name", "ae1")
				.next();
		
		Vertex pnf3 = g.addV()
				.property("aai-node-type", "pnf")
				.property("pnf-name", "pnf-name-3")
				.next();
		Vertex pnf4 = g.addV()
				.property("aai-node-type", "pnf")
				.property("pnf-name", "pnf-name-4")
				.next();
		Vertex pnf5 = g.addV()
				.property("aai-node-type", "pnf")
				.property("pnf-name", "pnf-name-5")
				.next();
		// graph 1
				
		edgeSerializer.addTreeEdge(g, pnf2, port21);


		TransactionalGraphEngine spy = spy(dbEngine);
		TransactionalGraphEngine.Admin adminSpy = spy(dbEngine.asAdmin());

		GraphTraversalSource traversal = g;
		GraphTraversalSource readOnly = tx.traversal(GraphTraversalSource.build().with(ReadOnlyStrategy.instance()));
		when (spy.tx()).thenReturn(tx);
		when(spy.asAdmin()).thenReturn(adminSpy);
		when(adminSpy.getTraversalSource()).thenReturn(traversal);
		when(adminSpy.getReadOnlyTraversalSource()).thenReturn(readOnly);
		
		migration = new MigratePATHPhysicalInventory(spy, loaderFactory, edgeIngestor, edgeSerializer, schemaVersions);
		migration.run();
	}
	
	@Test
	public void testRun_checkPnfsAndPInterfacesExist() throws Exception {
		// check if graph nodes exist

		testGetMigrationName();
		testGetAffectedNodeTypes();
		
		// check if pnf node gets created
		assertEquals("4 PNFs exist", new Long(4L), 
				g.V().has("aai-node-type", "pnf")
				.count().next());
		
		assertEquals("5 lag-interfaces were created", new Long (5L), g.V().has("aai-node-type", "lag-interface")
				.out("tosca.relationships.network.BindsTo").count().next());
				
		assertEquals("lag-interfaces created for pnfs", new Long(1L),
				g.V().has("aai-node-type", "pnf")
				.has("pnf-name", "pnf-name-3").count().next());
		
		assertEquals("lag-interface ae1 created for pnf-name-3", true,
				g.V().has("aai-node-type", "pnf")
				.has("pnf-name", "pnf-name-3")
				.in("tosca.relationships.network.BindsTo")
				.has("aai-node-type", "lag-interface")
				.has("interface-name","ae1")
				.hasNext());
		
		assertEquals("lag-interfaces created for pnfs", new Long(2L),
				g.V().has("aai-node-type", "pnf")
				.has("pnf-name", "pnf-name-5")
				.in("tosca.relationships.network.BindsTo")
				.has("aai-node-type", "lag-interface").count().next());
	}
	
	public void testGetAffectedNodeTypes() {
		Optional<String[]> types = migration.getAffectedNodeTypes();
		Optional<String[]> expected = Optional.of(new String[]{"lag-interface"});
		
		assertNotNull(types);
		assertArrayEquals(expected.get(), types.get());
	}

	public void testGetMigrationName() {
		String migrationName = migration.getMigrationName();

		assertNotNull(migrationName);
		assertEquals("MigratePATHPhysicalInventory", migrationName);
	}
}
