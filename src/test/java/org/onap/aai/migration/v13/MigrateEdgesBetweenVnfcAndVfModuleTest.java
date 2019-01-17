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

import static org.junit.Assert.assertEquals;
import static org.mockito.Mockito.spy;
import static org.mockito.Mockito.when;

import org.apache.tinkerpop.gremlin.process.traversal.dsl.graph.GraphTraversalSource;
import org.apache.tinkerpop.gremlin.process.traversal.strategy.verification.ReadOnlyStrategy;
import org.apache.tinkerpop.gremlin.structure.Vertex;
import org.janusgraph.core.JanusGraph;
import org.janusgraph.core.JanusGraphFactory;
import org.janusgraph.core.JanusGraphTransaction;
import org.junit.After;
import org.junit.AfterClass;
import org.junit.Before;
import org.junit.Test;
import org.onap.aai.AAISetup;
import org.onap.aai.dbmap.DBConnectionType;
import org.onap.aai.edges.enums.EdgeProperty;
import org.onap.aai.introspection.Loader;
import org.onap.aai.introspection.ModelType;
import org.onap.aai.serialization.engines.JanusGraphDBEngine;
import org.onap.aai.serialization.engines.QueryStyle;
import org.onap.aai.serialization.engines.TransactionalGraphEngine;

public class MigrateEdgesBetweenVnfcAndVfModuleTest extends AAISetup{

	private final static ModelType introspectorFactoryType = ModelType.MOXY;
	private final static QueryStyle queryStyle = QueryStyle.TRAVERSAL;
	private final static DBConnectionType type = DBConnectionType.REALTIME;

	private static Loader loader;
	private static TransactionalGraphEngine dbEngine;
	private static JanusGraph graph;
	private static MigrateEdgesBetweenVnfcAndVfModule migration;
	private static JanusGraphTransaction tx;
	private static GraphTraversalSource g;

	@Before
	public void setUp() throws Exception {
		graph = JanusGraphFactory.build().set("storage.backend","inmemory").open();
		tx = graph.newTransaction();
		g = tx.traversal();
		loader = loaderFactory.createLoaderForVersion(introspectorFactoryType, schemaVersions.getDefaultVersion());
		dbEngine = new JanusGraphDBEngine(
				queryStyle,
				type,
				loader);

		Vertex vnfc = g.addV().property("aai-node-type", "vnfc")
                .property("vnfc-name", "vnfc-name-1").next();

        Vertex vfmodule = g.addV().property("aai-node-type", "vf-module")
                .property("vf-module-id", "vf-module-id-1").next();
        
        
        //edgeSerializer.addEdge(g, vfmodule, vnfc,"org.onap.relationships.inventory.Uses");
        
        vfmodule.addEdge("org.onap.relationships.inventory.Uses", vnfc, EdgeProperty.CONTAINS.toString(), "NONE",
        		EdgeProperty.DELETE_OTHER_V.toString(), "NONE", EdgeProperty.PREVENT_DELETE.toString(), "OUT");
        
        
        TransactionalGraphEngine spy = spy(dbEngine);
		TransactionalGraphEngine.Admin adminSpy = spy(dbEngine.asAdmin());

		GraphTraversalSource traversal = g;
		GraphTraversalSource readOnly = tx.traversal(GraphTraversalSource.build().with(ReadOnlyStrategy.instance()));
		when (spy.tx()).thenReturn(tx);
		when(spy.asAdmin()).thenReturn(adminSpy);
		when(adminSpy.getTraversalSource()).thenReturn(traversal);
		when(adminSpy.getReadOnlyTraversalSource()).thenReturn(readOnly);

		migration = new MigrateEdgesBetweenVnfcAndVfModule(spy,loaderFactory,edgeIngestor,edgeSerializer,schemaVersions);
		migration.run();
	}

	@After
	public void cleanUp() {
		tx.tx().rollback();
		graph.close();
	}

	@Test
    public void testIdsUpdated() throws Exception {
		
		//System.out.println("After Migration: " +migration.asString(g.V().has("aai-node-type","vnfc").inE().next()));
		
		assertEquals("vf-module to vnfc migration done", true,
				g.V().has("aai-node-type", "vf-module").outE().hasLabel("org.onap.relationships.inventory.Uses") 
						.has(EdgeProperty.DELETE_OTHER_V.toString(), "OUT").hasNext());
		
		assertEquals("vf-module to vnfc migration done", true,
				g.V().has("aai-node-type", "vnfc").inE().hasLabel("org.onap.relationships.inventory.Uses") 
						.has(EdgeProperty.PREVENT_DELETE.toString(), "NONE").hasNext());  
    }   
}
