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
import org.junit.jupiter.api.Test;

import org.onap.aai.AAISetup;
import org.onap.aai.db.props.AAIProperties;
import org.onap.aai.introspection.Loader;
import org.onap.aai.introspection.ModelType;
import org.onap.aai.serialization.engines.JanusGraphDBEngine;
import org.onap.aai.serialization.engines.QueryStyle;
import org.onap.aai.serialization.engines.TransactionalGraphEngine;

import java.util.Arrays;
import java.util.Collections;
import java.util.HashSet;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.mockito.Mockito.spy;
import static org.mockito.Mockito.when;

public class UriMigrationTest extends AAISetup {
	private final static ModelType introspectorFactoryType = ModelType.MOXY;
	private final static QueryStyle queryStyle = QueryStyle.TRAVERSAL;
	private Loader loader;
	private TransactionalGraphEngine dbEngine;
	private JanusGraph graph;
	private UriMigration migration;
	private GraphTraversalSource g;
	private JanusGraphTransaction tx;

	private Vertex pnf3;
	private Vertex pInterface3;
	private Vertex pInterface4;
	private Vertex lInterface3;
	private Vertex plink3;

	@BeforeEach
	public void setUp() throws Exception {
		graph = JanusGraphFactory.build().set("storage.backend","inmemory").open();
		tx = graph.newTransaction();
		g = tx.traversal();
		loader = loaderFactory.createLoaderForVersion(introspectorFactoryType, schemaVersions.getDefaultVersion());
		dbEngine = new JanusGraphDBEngine(
				queryStyle,
				loader);

		pnf3 = g.addV().property("aai-node-type", "pnf")
				.property("pnf-name", "pnf-name3")
				.next();
		pInterface3 = g.addV().property("aai-node-type", "p-interface")
				.property("interface-name", "p-interface-name3")
				.next();
		pInterface4 = g.addV().property("aai-node-type", "p-interface")
				.property("interface-name", "p-interface-name/4")
				.next();
		lInterface3 = g.addV().property("aai-node-type", "l-interface")
				.property("interface-name", "l-interface-name3")
				.next();
		plink3 = g.addV().property("aai-node-type", "physical-link")
						.property("link-name", "link-name3")
					.next();
		edgeSerializer.addTreeEdge(g, pnf3, pInterface3);
		edgeSerializer.addTreeEdge(g, pnf3, pInterface4);
		edgeSerializer.addTreeEdge(g, pInterface3, lInterface3);
		edgeSerializer.addEdge(g, pInterface3, plink3);

		TransactionalGraphEngine spy = spy(dbEngine);
		TransactionalGraphEngine.Admin adminSpy = spy(dbEngine.asAdmin());
		GraphTraversalSource traversal = g;
		when(spy.asAdmin()).thenReturn(adminSpy);
		when(adminSpy.getTraversalSource()).thenReturn(traversal);
		migration = new UriMigration(spy, loaderFactory, edgeIngestor, edgeSerializer, schemaVersions);

		migration.run();

	}

	@AfterEach
	public void tearDown() throws Exception {
		graph.close();
	}

	@Test
	public void allVertexHasUri() throws InterruptedException {
		assertEquals(Long.valueOf(0L), g.V().hasNot(AAIProperties.AAI_URI).count().next());
	}

	@Test
	public void pnf() {
		printVertex(pnf3);
		assertEquals("/network/pnfs/pnf/pnf-name3", g.V().has("pnf-name", "pnf-name3").next().value("aai-uri"));
	}

	protected void printVertex(Vertex v) {
		final StringBuilder sb = new StringBuilder();
		v.properties().forEachRemaining(p -> sb.append("\t").append(p.key()).append(" : ").append(p.value()).append("\n"));
		sb.append("\n");
		System.out.println(sb.toString());
	}

	@Test
	public void plink3() {
		printVertex(plink3);
		assertEquals("/network/physical-links/physical-link/link-name3", g.V().has("link-name", "link-name3").next().value("aai-uri"));
	}

	@Test
	public void pinterface3() {
		printVertex(pInterface3);
		assertEquals("/network/pnfs/pnf/pnf-name3/p-interfaces/p-interface/p-interface-name3", g.V().has("interface-name", "p-interface-name3").next().value("aai-uri"));
	}

	@Test
	public void pInterface4() {
		printVertex(pInterface4);
		assertEquals("/network/pnfs/pnf/pnf-name3/p-interfaces/p-interface/p-interface-name%2F4", g.V().has("interface-name", "p-interface-name/4").next().value("aai-uri"));
	}

	@Test
	public void getChildrenTopTest() {
		migration.seen = new HashSet<>();
		migration.seen.add(pnf3.id());
		assertEquals(new HashSet<>(Arrays.asList(pInterface3, pInterface4)), migration.getChildren(pnf3));
	}

	@Test
	public void getChildrenOneDownTest() {
		migration.seen = new HashSet<>();
		migration.seen.add(pnf3.id());
		assertEquals(new HashSet<>(Arrays.asList(lInterface3)), migration.getChildren(pInterface3));
	}

	@Test
	public void getChildrenTwoDownTest() {
		migration.seen = new HashSet<>();
		migration.seen.add(pInterface3.id());
		assertEquals(Collections.EMPTY_SET, migration.getChildren(lInterface3));
	}
}
