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

import org.apache.tinkerpop.gremlin.process.traversal.dsl.graph.GraphTraversalSource;
import org.apache.tinkerpop.gremlin.structure.Vertex;
import org.janusgraph.core.JanusGraph;
import org.janusgraph.core.JanusGraphFactory;
import org.janusgraph.core.JanusGraphTransaction;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Disabled;
import org.junit.jupiter.api.Test;

import org.onap.aai.AAISetup;
import org.onap.aai.dbmap.DBConnectionType;
import org.onap.aai.introspection.Loader;
import org.onap.aai.introspection.ModelType;
import org.onap.aai.migration.Status;
import org.onap.aai.serialization.engines.JanusGraphDBEngine;
import org.onap.aai.serialization.engines.QueryStyle;
import org.onap.aai.serialization.engines.TransactionalGraphEngine;
import org.onap.aai.setup.SchemaVersions;
import org.onap.aai.setup.SchemaVersion;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.mockito.Mockito.spy;
import static org.mockito.Mockito.when;

@Disabled
public class DeletePInterfaceTest extends AAISetup {

	private final static ModelType introspectorFactoryType = ModelType.MOXY;
	private final static QueryStyle queryStyle = QueryStyle.TRAVERSAL;
	private Loader loader;
	private TransactionalGraphEngine dbEngine;
	private JanusGraph graph;
	private DeletePInterface migration;
	private GraphTraversalSource g;
	private JanusGraphTransaction tx;

	@BeforeEach
	public void setUp() throws Exception {
		graph = JanusGraphFactory.build().set("storage.backend","inmemory").open();
		tx = graph.newTransaction();
		g = tx.traversal();
		loader = loaderFactory.createLoaderForVersion(introspectorFactoryType, schemaVersions.getDefaultVersion());
		System.setProperty("AJSC_HOME", ".");
		System.setProperty("BUNDLECONFIG_DIR", "src/test/resources");
		dbEngine = new JanusGraphDBEngine(
				queryStyle,
				loader);

		Vertex pnf1 = g.addV().property("aai-node-type", "pnf")
			.property("pnf-name", "pnf-name1")
			.next();
		Vertex pInterface1 = g.addV().property("aai-node-type", "p-interface")
				.property("interface-name", "interface-name1")
				.property("source-of-truth", "AAI-CSVP-INSTARAMS")
				.next();
		edgeSerializer.addTreeEdge(g, pnf1, pInterface1);
		
		Vertex pnf2 = g.addV().property("aai-node-type", "pnf")
				.property("pnf-name", "pnf-name2")
				.next();
		Vertex pInterface2 = g.addV().property("aai-node-type", "p-interface")
				.property("interface-name", "interface-name2")
				.property("source-of-truth", "AAI-CSVP-INSTARAMS")
				.next();
		Vertex pLink = g.addV().property("aai-node-type", "physical-link")
				.property("interface-name", "interface-name1")
				.next();
		edgeSerializer.addTreeEdge(g, pnf2, pInterface2);
		edgeSerializer.addEdge(g, pInterface2, pLink);
		
		Vertex pnf3 = g.addV().property("aai-node-type", "pnf")
				.property("pnf-name", "pnf-name3")
				.next();
		Vertex pInterface3 = g.addV().property("aai-node-type", "p-interface")
				.property("interface-name", "interface-name3")
				.property("source-of-truth", "AAI-CSVP-INSTARAMS")
				.next();
		Vertex lInterface = g.addV().property("aai-node-type", "l-interface")
				.property("interface-name", "interface-name3")
				.next();
		edgeSerializer.addTreeEdge(g, pnf3, pInterface3);
		edgeSerializer.addTreeEdge(g, pInterface3, lInterface);
		
		Vertex pnf4 = g.addV().property("aai-node-type", "pnf")
				.property("pnf-name", "pnf-name4")
				.next();
		Vertex pInterface4 = g.addV().property("aai-node-type", "p-interface")
				.property("interface-name", "interface-name4")
				.next();
		edgeSerializer.addTreeEdge(g, pnf4, pInterface4);
		
		TransactionalGraphEngine spy = spy(dbEngine);
		TransactionalGraphEngine.Admin adminSpy = spy(dbEngine.asAdmin());
		GraphTraversalSource traversal = g;
		when(spy.asAdmin()).thenReturn(adminSpy);
		when(adminSpy.getTraversalSource()).thenReturn(traversal);
		migration = new DeletePInterface(spy, loaderFactory, edgeIngestor, edgeSerializer, schemaVersions);
		migration.run();
	}

	@AfterEach
	public void tearDown() throws Exception {
		tx.rollback();
		graph.close();
	}

	@Test
	public void test() {
		assertEquals(false, g.V().has("aai-node-type", "pnf").has("pnf-name", "pnf-name1")
				.in("tosca.relationships.network.BindsTo").has("aai-node-type", "p-interface").has("interface-name", "interface-name1").hasNext(), "pInterface1 deleted");
		
		assertEquals(true, g.V().has("aai-node-type", "pnf").has("pnf-name", "pnf-name2")
				.in("tosca.relationships.network.BindsTo").has("aai-node-type", "p-interface").hasNext(), "pInterface2 skipped");
		
		assertEquals(true, g.V().has("aai-node-type", "pnf").has("pnf-name", "pnf-name3")
				.in("tosca.relationships.network.BindsTo").has("aai-node-type", "p-interface").hasNext(), "pInterface3 skipped");
		
		assertEquals(true, g.V().has("aai-node-type", "pnf").has("pnf-name", "pnf-name4")
				.in("tosca.relationships.network.BindsTo").has("aai-node-type", "p-interface").has("interface-name", "interface-name4").hasNext(), "pInterface4 should not be deleted");
		
		assertEquals(Status.SUCCESS, migration.getStatus(), "Status should be success");
	}

}
