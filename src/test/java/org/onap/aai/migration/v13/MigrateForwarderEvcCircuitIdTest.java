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

import static org.junit.Assert.assertArrayEquals;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertFalse;
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

public class MigrateForwarderEvcCircuitIdTest extends AAISetup {

	private final static ModelType introspectorFactoryType = ModelType.MOXY;
	private final static QueryStyle queryStyle = QueryStyle.TRAVERSAL;

	private Loader loader;
	private TransactionalGraphEngine dbEngine;
	private MigrateForwarderEvcCircuitId migration;
	private GraphTraversalSource g;

	@Before
	public void setUp() throws Exception {
		g = tx.traversal();
		loader = loaderFactory.createLoaderForVersion(introspectorFactoryType, schemaVersions.getDefaultVersion());
		dbEngine = new JanusGraphDBEngine(
				queryStyle,
				loader);
		
		System.setProperty("BUNDLECONFIG_DIR", "src/test/resources");

		Vertex pnf1 = g.addV().property("aai-node-type", "pnf").property("pnf-name", "pnf-1").next();
		Vertex pnf2 = g.addV().property("aai-node-type", "pnf").property("pnf-name", "pnf-2").next();
		Vertex pnf3 = g.addV().property("aai-node-type", "pnf").property("pnf-name", "pnf-3").next();
		Vertex pnf4 = g.addV().property("aai-node-type", "pnf").property("pnf-name", "pnf-4").next();
		Vertex pnf5 = g.addV().property("aai-node-type", "pnf").property("pnf-name", "pnf-5").next();

		Vertex pInterface1 = g.addV().property("aai-node-type", "p-interface").property("interface-name", "p-interface-1").next();
		Vertex pInterface2 = g.addV().property("aai-node-type", "p-interface").property("interface-name", "p-interface-2").next();
		Vertex pInterface3 = g.addV().property("aai-node-type", "p-interface").property("interface-name", "p-interface-3").next();
		Vertex pInterface4 = g.addV().property("aai-node-type", "p-interface").property("interface-name", "p-interface-4").next();
		Vertex pInterface5 = g.addV().property("aai-node-type", "p-interface").property("interface-name", "p-interface-5").next();

		Vertex forwarder1 = g.addV().property("aai-node-type", "forwarder").property("sequence", 1)
					.property("forwarder-role",  "ingress").next();
		Vertex forwarder2 = g.addV().property("aai-node-type", "forwarder").property("sequence", 1)
				.property("forwarder-role",  "ingress").next();
		Vertex forwarder3 = g.addV().property("aai-node-type", "forwarder").property("sequence", 1)
				.property("forwarder-role",  "ingress").next();
		Vertex forwarder4 = g.addV().property("aai-node-type", "forwarder").property("sequence", 1)
				.property("forwarder-role",  "ingress").next();
		Vertex forwarder5 = g.addV().property("aai-node-type", "forwarder").property("sequence", 1)
				.property("forwarder-role",  "ingress").next();
		
		
		Vertex configuration1 = g.addV().property("aai-node-type", "configuration").property("configuration-id", "config-1")
				.property("configuration-type", "test").property("configuration-subt-type", "test").next();
		Vertex configuration2 = g.addV().property("aai-node-type", "configuration").property("configuration-id", "config-2")
				.property("configuration-type", "test").property("configuration-subt-type", "test").next();
		Vertex configuration3 = g.addV().property("aai-node-type", "configuration").property("configuration-id", "config-3")
				.property("configuration-type", "test").property("configuration-subt-type", "test").next();
		Vertex configuration4 = g.addV().property("aai-node-type", "configuration").property("configuration-id", "config-4")
				.property("configuration-type", "test").property("configuration-subt-type", "test").next();
		Vertex configuration5 = g.addV().property("aai-node-type", "configuration").property("configuration-id", "config-5")
				.property("configuration-type", "test").property("configuration-subt-type", "test").next();


		Vertex forwarderEvc1 = g.addV().property("aai-node-type", "forwarder-evc").property("forwarder-evc-id", "evc-1")
				.property("circuit-id", "1").property("resource-version", "v13").next();
		Vertex forwarderEvc2 = g.addV().property("aai-node-type", "forwarder-evc").property("forwarder-evc-id", "evc-2")
				.property("circuit-id", "2").property("resource-version", "v13").next();
		Vertex forwarderEvc3 = g.addV().property("aai-node-type", "forwarder-evc").property("forwarder-evc-id", "evc-3")
				.property("resource-version", "v13").next();
		Vertex forwarderEvc4 = g.addV().property("aai-node-type", "forwarder-evc").property("forwarder-evc-id", "evc-4")
				.property("circuit-id", "3").property("resource-version", "v13").next();
		Vertex forwarderEvc5 = g.addV().property("aai-node-type", "forwarder-evc").property("forwarder-evc-id", "evc-5")
				.property("resource-version", "v13").next();		
		

		
		edgeSerializer.addTreeEdge(g, pnf1, pInterface1);
		edgeSerializer.addEdge(g, pInterface1, forwarder1);
		edgeSerializer.addEdge(g, forwarder1, configuration1);
		edgeSerializer.addTreeEdge(g, configuration1, forwarderEvc1);
		
		edgeSerializer.addTreeEdge(g, pnf2, pInterface2);
		edgeSerializer.addEdge(g, pInterface2, forwarder2);
		edgeSerializer.addEdge(g, forwarder2, configuration2);
		edgeSerializer.addTreeEdge(g, configuration2, forwarderEvc2);
		
		edgeSerializer.addTreeEdge(g, pnf3, pInterface3);
		edgeSerializer.addEdge(g, pInterface3, forwarder3);
		edgeSerializer.addEdge(g, forwarder3, configuration3);
		edgeSerializer.addTreeEdge(g, configuration3, forwarderEvc3);
		
		edgeSerializer.addTreeEdge(g, pnf4, pInterface4);
		edgeSerializer.addEdge(g, pInterface4, forwarder4);
		edgeSerializer.addEdge(g, forwarder4, configuration4);
		edgeSerializer.addTreeEdge(g, configuration4, forwarderEvc4);
		
		edgeSerializer.addTreeEdge(g, pnf5, pInterface5);
		edgeSerializer.addEdge(g, pInterface5, forwarder5);
		edgeSerializer.addEdge(g, forwarder5, configuration5);
		edgeSerializer.addTreeEdge(g, configuration5, forwarderEvc5);

		TransactionalGraphEngine spy = spy(dbEngine);
		TransactionalGraphEngine.Admin adminSpy = spy(dbEngine.asAdmin());

		GraphTraversalSource traversal = g;
		GraphTraversalSource readOnly = tx.traversal(GraphTraversalSource.build().with(ReadOnlyStrategy.instance()));
		when (spy.tx()).thenReturn(tx);
		when(spy.asAdmin()).thenReturn(adminSpy);
		when(adminSpy.getTraversalSource()).thenReturn(traversal);
		when(adminSpy.getReadOnlyTraversalSource()).thenReturn(readOnly);
		
		migration = new MigrateForwarderEvcCircuitId(spy, loaderFactory, edgeIngestor, edgeSerializer, schemaVersions);
		migration.run();
	}
	
	@Test
	public void testCircuitIdsUpdated() throws Exception {
		// check if graph nodes are updated
		
		assertEquals("First circuit-id updated", "10", 
				g.V().has("aai-node-type", "forwarder-evc").has("circuit-id", "10").next().value("circuit-id").toString());

		assertEquals("Second circuit-id updated", "20", 
				g.V().has("aai-node-type", "forwarder-evc").has("circuit-id", "20").next().value("circuit-id").toString());

		assertFalse("Third circuit-id remains empty", g.V().has("aai-node-type", "forwarder-evc").has("forwarder-evc-id", "evc-3")
				.next().property("circuit-id").isPresent());

		assertEquals("Fourth circuit-id not updated", "3", 
				g.V().has("aai-node-type", "forwarder-evc").has("circuit-id", "3").next().value("circuit-id").toString());

		assertFalse("Fifth circuit-id remains empty", g.V().has("aai-node-type", "forwarder-evc").has("forwarder-evc-id", "evc-5")
				.next().property("circuit-id").isPresent());
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
		assertEquals("MigrateForwarderEvcCircuitId", migrationName);
	}
}
