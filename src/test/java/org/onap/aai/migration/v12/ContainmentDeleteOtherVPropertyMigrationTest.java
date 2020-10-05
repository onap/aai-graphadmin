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
import org.janusgraph.core.schema.JanusGraphManagement;
import org.apache.tinkerpop.gremlin.process.traversal.dsl.graph.GraphTraversalSource;
import org.apache.tinkerpop.gremlin.structure.Edge;
import org.apache.tinkerpop.gremlin.structure.Graph;
import org.apache.tinkerpop.gremlin.structure.Vertex;
import org.junit.After;
import org.junit.Before;
import org.junit.Test;
import org.springframework.test.annotation.DirtiesContext;
import org.mockito.Mockito;
import org.onap.aai.AAISetup;
import org.onap.aai.dbmap.DBConnectionType;
import org.onap.aai.edges.enums.EdgeProperty;
import org.onap.aai.introspection.Loader;
import org.onap.aai.introspection.ModelType;
import org.onap.aai.setup.SchemaVersions;
import org.onap.aai.setup.SchemaVersion;
import org.onap.aai.edges.enums.AAIDirection;
import org.onap.aai.serialization.engines.QueryStyle;
import org.onap.aai.serialization.engines.JanusGraphDBEngine;
import org.onap.aai.serialization.engines.TransactionalGraphEngine;
import org.springframework.test.annotation.DirtiesContext;

import static org.junit.Assert.assertEquals;
import static org.mockito.Mockito.spy;
import static org.mockito.Mockito.when;

@DirtiesContext(classMode = DirtiesContext.ClassMode.BEFORE_CLASS)
public class ContainmentDeleteOtherVPropertyMigrationTest extends AAISetup {

	private final static ModelType introspectorFactoryType = ModelType.MOXY;
	private final static QueryStyle queryStyle = QueryStyle.TRAVERSAL;

	private Loader loader;
	private TransactionalGraphEngine dbEngine;
	private JanusGraph graph;
	private ContainmentDeleteOtherVPropertyMigration migration;
	private GraphTraversalSource g;
	private Graph tx;

	@Before
	@DirtiesContext(classMode = DirtiesContext.ClassMode.BEFORE_CLASS)
	public void setUp() throws Exception {
		graph = JanusGraphFactory.build().set("storage.backend","inmemory").open();
		JanusGraphManagement janusgraphManagement = graph.openManagement();
		tx = graph.newTransaction();
		g = tx.traversal();
		loader = loaderFactory.createLoaderForVersion(introspectorFactoryType, schemaVersions.getDefaultVersion());
		dbEngine = new JanusGraphDBEngine(
				queryStyle,
				loader);
		Vertex v = g.addV().property("aai-node-type", "generic-vnf")
							.property("vnf-id", "delcontains-test-vnf")
							.next();
		Vertex v2 = g.addV().property("aai-node-type", "l-interface")
							.property("interface-name", "delcontains-test-lint")
							.next();
		Edge e = v.addEdge("hasLInterface", v2, EdgeProperty.CONTAINS.toString(), AAIDirection.OUT.toString(), 
													EdgeProperty.DELETE_OTHER_V.toString(), AAIDirection.NONE.toString());
		
		Vertex v3 = g.addV().property("aai-node-type", "allotted-resource").next();
		
		Edge e2 = v2.addEdge("uses", v3, EdgeProperty.CONTAINS.toString(), AAIDirection.NONE.toString(),
											EdgeProperty.DELETE_OTHER_V.toString(), AAIDirection.NONE.toString());
		TransactionalGraphEngine spy = spy(dbEngine);
		TransactionalGraphEngine.Admin adminSpy = spy(dbEngine.asAdmin());
		GraphTraversalSource traversal = g;
		when(spy.asAdmin()).thenReturn(adminSpy);
		when(adminSpy.getTraversalSource()).thenReturn(traversal);
		Mockito.doReturn(janusgraphManagement).when(adminSpy).getManagementSystem();
		migration = new ContainmentDeleteOtherVPropertyMigration(spy, loaderFactory, edgeIngestor, edgeSerializer, schemaVersions, "/edgeMigrationTestRules.json");
		migration.run();
	}
	
	@After
	public void cleanUp() {
		tx.tx().rollback();
		graph.close();
	}
	
	@Test
	public void run() {
		assertEquals("del other now OUT", true, 
				g.E().hasLabel("hasLInterface").has(EdgeProperty.DELETE_OTHER_V.toString(), AAIDirection.OUT.toString()).hasNext());
		assertEquals("contains val still same", true, 
				g.E().hasLabel("hasLInterface").has(EdgeProperty.CONTAINS.toString(), AAIDirection.OUT.toString()).hasNext());
		assertEquals("non-containment unchanged", true,
				g.E().hasLabel("uses").has(EdgeProperty.DELETE_OTHER_V.toString(), AAIDirection.NONE.toString()).hasNext());
	}

}
