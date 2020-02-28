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

package org.onap.aai.migration;

import org.apache.tinkerpop.gremlin.process.traversal.dsl.graph.GraphTraversalSource;
import org.apache.tinkerpop.gremlin.structure.Graph;
import org.janusgraph.core.JanusGraph;
import org.janusgraph.core.JanusGraphFactory;
import org.janusgraph.core.schema.JanusGraphManagement;
import org.junit.After;
import org.junit.Before;
import org.junit.Test;
import org.mockito.Mockito;
import org.onap.aai.AAISetup;
import org.onap.aai.introspection.Loader;
import org.onap.aai.introspection.ModelType;
import org.onap.aai.serialization.engines.JanusGraphDBEngine;
import org.onap.aai.serialization.engines.QueryStyle;
import org.onap.aai.serialization.engines.TransactionalGraphEngine;

import static org.junit.Assert.assertEquals;
import static org.mockito.Mockito.*;

public class RebuildAllEdgesTest extends AAISetup {

	private static final ModelType introspectorFactoryType = ModelType.MOXY;
	private static final QueryStyle queryStyle = QueryStyle.TRAVERSAL;
	private JanusGraph graph;
	private GraphTraversalSource g;
	private Graph tx;
	private RebuildAllEdges spyRebuildAllEdges;

	@Before
	public void setUp() {
		graph = JanusGraphFactory.build().set("storage.backend","inmemory").open();
		JanusGraphManagement janusgraphManagement = graph.openManagement();
		tx = graph.newTransaction();
		g = tx.traversal();
		Loader loader =
                loaderFactory.createLoaderForVersion(introspectorFactoryType, schemaVersions.getDefaultVersion());
		TransactionalGraphEngine dbEngine = new JanusGraphDBEngine(
				queryStyle,
				loader);
		TransactionalGraphEngine spy = spy(dbEngine);
		TransactionalGraphEngine.Admin adminSpy = spy(dbEngine.asAdmin());
		GraphTraversalSource traversal = g;
		when(spy.asAdmin()).thenReturn(adminSpy);
		when(adminSpy.getTraversalSource()).thenReturn(traversal);
		Mockito.doReturn(janusgraphManagement).when(adminSpy).getManagementSystem();
		RebuildAllEdges rebuildAllEdges = new RebuildAllEdges(spy, loaderFactory, edgeIngestor, edgeSerializer, schemaVersions);
        spyRebuildAllEdges = spy(rebuildAllEdges);
        doNothing().when((EdgeMigrator)spyRebuildAllEdges).rebuildEdges(g.E().toSet());
        spyRebuildAllEdges.run();
	}
	
	@After
	public void cleanUp() {
		tx.tx().rollback();
		graph.close();
	}
	
	@Test
	public void executeModifyOperationTest() {
	    verify((EdgeMigrator)spyRebuildAllEdges,times(1)).rebuildEdges(g.E().toSet());
	   	assertEquals(0, g.E().toSet().size());
	    assertEquals(0,spyRebuildAllEdges.processed);
		assertEquals(0,spyRebuildAllEdges.skipped);
		assertEquals(0,spyRebuildAllEdges.edgeMissingParentProperty.size());
		assertEquals(0,spyRebuildAllEdges.edgeMultiplicityExceptionCtr.values().stream().mapToInt(Number::intValue).sum());
	}

}

 