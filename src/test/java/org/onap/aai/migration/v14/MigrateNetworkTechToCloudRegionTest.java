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
package org.onap.aai.migration.v14;

import static org.junit.Assert.assertEquals;
import static org.mockito.Mockito.spy;
import static org.mockito.Mockito.when;

import org.apache.tinkerpop.gremlin.process.traversal.dsl.graph.GraphTraversalSource;
import org.apache.tinkerpop.gremlin.structure.Vertex;
import org.junit.Before;
import org.junit.Test;
import org.onap.aai.AAISetup;
import org.onap.aai.dbmap.DBConnectionType;
import org.onap.aai.introspection.Loader;
import org.onap.aai.introspection.ModelType;
import org.onap.aai.serialization.engines.JanusGraphDBEngine;
import org.onap.aai.serialization.engines.QueryStyle;
import org.onap.aai.serialization.engines.TransactionalGraphEngine;

public class MigrateNetworkTechToCloudRegionTest extends AAISetup{

	private final static ModelType introspectorFactoryType = ModelType.MOXY;
	private final static QueryStyle queryStyle = QueryStyle.TRAVERSAL;

	private Loader loader;
	private TransactionalGraphEngine dbEngine;
	private GraphTraversalSource g;
	private MigrateNetworkTechToCloudRegion migration;

	@Before
	public void setUp() throws Exception {
		g = tx.traversal();
        loader = loaderFactory.createLoaderForVersion(introspectorFactoryType, schemaVersions.getDefaultVersion());
        dbEngine = new JanusGraphDBEngine(
                queryStyle,
                loader);
        Vertex cloudRegion1 = g.addV().property("aai-node-type", "cloud-region").property("cloud-region-id", "cloud-region-id-1").property("cloud-owner", "att-aic").property("resource-version", "1").next();
        Vertex cloudRegion2 = g.addV().property("aai-node-type", "cloud-region").property("cloud-region-id", "cloud-region-id-2").property("cloud-owner", "att-nc").property("resource-version", "2").next();
		Vertex cloudRegion3 = g.addV().property("aai-node-type", "cloud-region").property("cloud-region-id", "cloud-region-id-3").property("cloud-owner", "att-aic").property("resource-version", "7").next();
		
		Vertex networkTech1 = g.addV().property("aai-node-type","network-technology").property("network-technology-id", "network-technology-1").property("network-technology-name", "CONTRAIL").property("resource-version", "3").next();
		Vertex networkTech2 = g.addV().property("aai-node-type", "network-technology").property("network-technology-id", "network-technology-2").property("network-technology-name", "AIC_SR_IOV").property("resource-version", "4").next();
		Vertex networkTech3 = g.addV().property("aai-node-type", "network-technology").property("network-technology-id", "network-technology-3").property("network-technology-name", "TEST").property("resource-version", "5").next();
		Vertex networkTech4 = g.addV().property("aai-node-type", "network-technology").property("network-technology-id", "network-technology-4").property("network-technology-name", "OVS").property("resource-version", "8").next();
		
		
		edgeSerializer.addEdge(g, cloudRegion1, networkTech1);
		
		
		TransactionalGraphEngine spy = spy(dbEngine);
        TransactionalGraphEngine.Admin adminSpy = spy(dbEngine.asAdmin());
        GraphTraversalSource traversal = g;
        when(spy.asAdmin()).thenReturn(adminSpy);
        when(adminSpy.getTraversalSource()).thenReturn(traversal);
		
        
		migration = new MigrateNetworkTechToCloudRegion(spy, loaderFactory, edgeIngestor, edgeSerializer, schemaVersions);
		migration.run();
		
		/*try {
			System.out.println("containsVertexLabel :: "+graph.containsVertexLabel("cloud-region"));
			graph.io(IoCore.graphson()).writeGraph("tinkerpop-modern.json");
			
			try (final InputStream stream = new FileInputStream("tinkerpop-modern.json")) {
				graph.io(IoCore.graphson()).reader().create().readGraph(stream, graph);
			}
			
			OutputStream out = new FileOutputStream("tinkerpop-modern.json");
			GraphSONWriter objGraphSONWriter = new GraphSONWriter(GraphSONWriter.build());
			objGraphSONWriter.writeGraph(out, graph);
		} catch (IOException e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		}*/
	}

	public void cleanUp() {
		tx.rollback();
		graph.close();
	}

	@Test
	public void checkEdgeCreatedForNetworkTechnology() {
		
		assertEquals(true,
				g.V().has("aai-node-type", "cloud-region").has("cloud-region-id", "cloud-region-id-1")
						.out()
						.has("aai-node-type", "network-technology").has("network-technology-id","network-technology-2").hasNext());
		
		assertEquals(true,
				g.V().has("aai-node-type", "cloud-region").has("cloud-region-id", "cloud-region-id-3")
						.out()
						.has("aai-node-type", "network-technology").has("network-technology-id","network-technology-1").hasNext());
		
		
		assertEquals(true,
				g.V().has("aai-node-type", "cloud-region").has("cloud-region-id", "cloud-region-id-2")
						.out()
						.has("aai-node-type", "network-technology").has("network-technology-id","network-technology-4").hasNext());
	
		
		assertEquals("Edge not created", false,
				g.V().has("aai-node-type", "cloud-region").has("cloud-region-id", "cloud-region-id-1")
						.out()
						.has("aai-node-type", "network-technology").has("network-technology-id","network-technology-3").hasNext());
		
		assertEquals("Edge not created", false,
				g.V().has("aai-node-type", "cloud-region").has("cloud-region-id", "cloud-region-id-2")
						.out()
						.has("aai-node-type", "network-technology").has("network-technology-id","network-technology-1").hasNext());
		
		
		assertEquals("Edge exists to 2 cloud regions", new Long(2L),
				g.V().has("aai-node-type", "network-technology").has("network-technology-id", "network-technology-1")
						.in().count().next());
		
		/*
		try {
			graph.io(IoCore.graphson()).writeGraph("tinkerpop-modern.json");
		} catch (IOException e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		}*/
		
	}

}
