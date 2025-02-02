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

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.mockito.Mockito.spy;
import static org.mockito.Mockito.when;

import org.apache.tinkerpop.gremlin.process.traversal.dsl.graph.GraphTraversalSource;
import org.apache.tinkerpop.gremlin.structure.Vertex;
import org.janusgraph.core.JanusGraph;
import org.janusgraph.core.JanusGraphFactory;
import org.janusgraph.core.JanusGraphTransaction;
import org.janusgraph.core.schema.JanusGraphManagement;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.mockito.Mockito;
import org.onap.aai.AAISetup;
import org.onap.aai.introspection.Loader;
import org.onap.aai.introspection.LoaderFactory;
import org.onap.aai.introspection.ModelType;
import org.onap.aai.setup.SchemaVersions;
import org.onap.aai.serialization.engines.JanusGraphDBEngine;
import org.onap.aai.serialization.engines.QueryStyle;
import org.onap.aai.serialization.engines.TransactionalGraphEngine;

public class MigrateModelVerTest extends AAISetup{

	private final static ModelType introspectorFactoryType = ModelType.MOXY;
	private final static QueryStyle queryStyle = QueryStyle.TRAVERSAL;
	private Loader loader;
	private TransactionalGraphEngine dbEngine;
	private JanusGraph graph;
	private GraphTraversalSource g;
	private JanusGraphTransaction tx;
	private MigrateModelVer migration;

	@BeforeEach
	public void setUp() throws Exception {
		graph = JanusGraphFactory.build().set("storage.backend", "inmemory").open();
		JanusGraphManagement janusgraphManagement = graph.openManagement();
		tx = graph.newTransaction();
		g = graph.traversal();
		loader = loaderFactory.createLoaderForVersion(introspectorFactoryType, schemaVersions.getDefaultVersion());
		dbEngine = new JanusGraphDBEngine(queryStyle, loader);

		TransactionalGraphEngine spy = spy(dbEngine);
		TransactionalGraphEngine.Admin adminSpy = spy(dbEngine.asAdmin());
		GraphTraversalSource traversal = g;
		when(spy.asAdmin()).thenReturn(adminSpy);
		when(adminSpy.getTraversalSource()).thenReturn(traversal);
		Mockito.doReturn(janusgraphManagement).when(adminSpy).getManagementSystem();
		
		
		// Add model1/model-ver1 -- invalid model/model-ver
		Vertex model1 = g.addV().property("aai-node-type", "model")
				.property("model-invariant-id", "model-invariant-id-1").property("model-type", "widget").next();
		Vertex modelVer1 = g.addV().property("aai-node-type", "model-ver").property("model-version-id", "model-version-id-1")
				.property("aai-uri", "/service-design-and-creation/models/model/model-invariant-id-1/model-vers/model-ver/model-version-id-1")
				.property("model-name", "connector").property("model-version", "v1.0").next();
		edgeSerializer.addTreeEdge(g, model1, modelVer1);
		//connector
		Vertex connector1= g.addV().property("aai-node-type", "connector").property("resource-instance-id", "connector1")
				.property("model-invariant-id-local", "model-invariant-id-1").property("model-version-id-local", "model-version-id-1").next();
		Vertex connector2= g.addV().property("aai-node-type", "connector").property("resource-instance-id", "connector2")
				.property("model-invariant-id-local", "model-invariant-id-x").property("model-version-id-local", "model-version-id-x").next();
		Vertex connector3= g.addV().property("aai-node-type", "connector").property("resource-instance-id", "connector3")
				.property("model-invariant-id-local", "model-invariant-id-1").property("model-version-id-local", "model-version-id-1").next();
		edgeSerializer.addPrivateEdge(traversal, connector3, modelVer1, null);


		// Add model1/model-ver1 -- invalid model/model-ver
		Vertex model2 = g.addV().property("aai-node-type", "model")
				.property("model-invariant-id", "model-invariant-id-2").property("model-type", "widget").next();
		Vertex modelVer2 = g.addV().property("aai-node-type", "model-ver").property("model-version-id", "model-version-id-2")
				.property("aai-uri", "/service-design-and-creation/models/model/model-invariant-id-2/model-vers/model-ver/model-version-id-2")
				.property("model-name", "service-instance").property("model-version", "v1.0").next();
		edgeSerializer.addTreeEdge(g, model2, modelVer2);
		//serivce-instance
		Vertex serviceInstance1= g.addV().property("aai-node-type", "service-instance").property("service-instance-id", "serviceinstance1")
				.property("model-invariant-id-local", "model-invariant-id-2").property("model-version-id-local", "model-version-id-2").next();
		Vertex serviceInstance2= g.addV().property("aai-node-type", "service-instance").property("service-instance-id", "serviceinstance2")
				.property("model-invariant-id-local", "model-invariant-id-y").property("model-version-id-local", "model-version-id-y").next();
		Vertex serviceInstance3= g.addV().property("aai-node-type", "service-instance").property("service-instance-id", "serviceinstance3")
				.property("model-invariant-id-local", "model-invariant-id-2").property("model-version-id-local", "model-version-id-2").next();
		edgeSerializer.addPrivateEdge(traversal, serviceInstance3, modelVer2, null);

		// Add model3/model-ver3
		Vertex model3 = g.addV().property("aai-node-type", "model")
				.property("model-invariant-id", "model-invariant-id-3").property("model-type", "widget").next();
		Vertex modelVer3 = g.addV().property("aai-node-type", "model-ver").property("model-version-id", "model-version-id-3")
				.property("aai-uri", "/service-design-and-creation/models/model/model-invariant-id-3/model-vers/model-ver/model-version-id-3")
				.property("model-name", "pnf").property("model-version", "v1.0").next();
		edgeSerializer.addTreeEdge(g, model3, modelVer3);
		//pnf
		Vertex pnfName1= g.addV().property("aai-node-type", "pnf").property("pnf-name", "pnfName1")
				.property("model-invariant-id-local", "model-invariant-id-3").property("model-version-id-local", "model-version-id-3").next();
		Vertex pnfName2= g.addV().property("aai-node-type", "pnf").property("pnf-name", "pnfName2")
				.property("model-invariant-id-local", "model-invariant-id-y").property("model-version-id-local", "model-version-id-y").next();
		Vertex pnfName3= g.addV().property("aai-node-type", "pnf").property("pnf-name", "pnfName3")
				.property("model-invariant-id-local", "model-invariant-id-3").property("model-version-id-local", "model-version-id-3").next();
		edgeSerializer.addPrivateEdge(traversal, pnfName3, modelVer3, null);
		
		// Add model4/model-ver4
		Vertex model4 = g.addV().property("aai-node-type", "model")
				.property("model-invariant-id", "model-invariant-id-4").property("model-type", "widget").next();
		Vertex modelVer4 = g.addV().property("aai-node-type", "model-ver").property("model-version-id", "model-version-id-4")
				.property("aai-uri", "/service-design-and-creation/models/model/model-invariant-id-4/model-vers/model-ver/model-version-id-4")
				.property("model-name", "logical-link").property("model-version", "v1.0").next();
		edgeSerializer.addTreeEdge(g, model4, modelVer4);
		//logical-link
		Vertex linkName1= g.addV().property("aai-node-type", "logical-link").property("link-name", "linkName1")
				.property("model-invariant-id-local", "model-invariant-id-4").property("model-version-id-local", "model-version-id-4").next();
		Vertex linkName2= g.addV().property("aai-node-type", "logical-link").property("link-name", "linkName2")
				.property("model-invariant-id-local", "model-invariant-id-y").property("model-version-id-local", "model-version-id-y").next();
		Vertex linkName3= g.addV().property("aai-node-type", "logical-link").property("link-name", "linkName3")
				.property("model-invariant-id-local", "model-invariant-id-4").property("model-version-id-local", "model-version-id-4").next();
		edgeSerializer.addPrivateEdge(traversal, linkName3, modelVer4, null);
		
		
		// Add model5/model-ver5
		Vertex model5 = g.addV().property("aai-node-type", "model")
				.property("model-invariant-id", "model-invariant-id-5").property("model-type", "widget").next();
		Vertex modelVer5 = g.addV().property("aai-node-type", "model-ver").property("model-version-id", "model-version-id-5")
				.property("aai-uri", "/service-design-and-creation/models/model/model-invariant-id-5/model-vers/model-ver/model-version-id-5")
				.property("model-name", "vnfc").property("model-version", "v1.0").next();
		edgeSerializer.addTreeEdge(g, model5, modelVer5);
		//vnfc
		Vertex vnfc1= g.addV().property("aai-node-type", "vnfc").property("vnfc-name", "vnfc1")
				.property("model-invariant-id-local", "model-invariant-id-5").property("model-version-id-local", "model-version-id-5").next();
		Vertex vnfc2= g.addV().property("aai-node-type", "vnfc").property("vnfc-name", "vnfc2")
				.property("model-invariant-id-local", "model-invariant-id-y").property("model-version-id-local", "model-version-id-y").next();
		Vertex vnfc3= g.addV().property("aai-node-type", "vnfc").property("vnfc-name", "vnfc3")
				.property("model-invariant-id-local", "model-invariant-id-5").property("model-version-id-local", "model-version-id-5").next();
		edgeSerializer.addPrivateEdge(traversal, vnfc3, modelVer5, null);
		
		// Add model6/model-ver6
		Vertex model6 = g.addV().property("aai-node-type", "model")
				.property("model-invariant-id", "model-invariant-id-6").property("model-type", "widget").next();
		Vertex modelVer6 = g.addV().property("aai-node-type", "model-ver").property("model-version-id", "model-version-id-6")
				.property("aai-uri", "/service-design-and-creation/models/model/model-invariant-id-6/model-vers/model-ver/model-version-id-6")
				.property("model-name", "vnf").property("model-version", "v1.0").next();
		edgeSerializer.addTreeEdge(g, model6, modelVer6);
		//generic-vnf
		Vertex vnf1= g.addV().property("aai-node-type", "generic-vnf").property("vnf-id", "vnf1")
				.property("model-invariant-id-local", "model-invariant-id-6").property("model-version-id-local", "model-version-id-6").next();
		Vertex vnf2= g.addV().property("aai-node-type", "generic-vnf").property("vnf-id", "vnf2")
				.property("model-invariant-id-local", "model-invariant-id-y").property("model-version-id-local", "model-version-id-y").next();
		Vertex vnf3= g.addV().property("aai-node-type", "generic-vnf").property("vnf-id", "vnf3")
				.property("model-invariant-id-local", "model-invariant-id-6").property("model-version-id-local", "model-version-id-6").next();
		edgeSerializer.addPrivateEdge(traversal, vnf3, modelVer6, null);
		
		// Add model7/model-ver7
		Vertex model7 = g.addV().property("aai-node-type", "model")
				.property("model-invariant-id", "model-invariant-id-7").property("model-type", "widget").next();
		Vertex modelVer7 = g.addV().property("aai-node-type", "model-ver").property("model-version-id", "model-version-id-7")
				.property("aai-uri", "/service-design-and-creation/models/model/model-invariant-id-7/model-vers/model-ver/model-version-id-7")
				.property("model-name", "configuration").property("model-version", "v1.0").next();
		edgeSerializer.addTreeEdge(g, model7, modelVer7);
		//configuration
		Vertex configuration1= g.addV().property("aai-node-type", "configuration").property("configuration-id", "configuration1")
				.property("model-invariant-id-local", "model-invariant-id-7").property("model-version-id-local", "model-version-id-7").next();
		Vertex configuration2= g.addV().property("aai-node-type", "configuration").property("configuration-id", "configuration2")
				.property("model-invariant-id-local", "model-invariant-id-y").property("model-version-id-local", "model-version-id-y").next();
		Vertex configuration3= g.addV().property("aai-node-type", "configuration").property("configuration-id", "configuration3")
				.property("model-invariant-id-local", "model-invariant-id-7").property("model-version-id-local", "model-version-id-7").next();
		edgeSerializer.addPrivateEdge(traversal, configuration3, modelVer7, null);
		
		// Add model8/model-ver8
		Vertex model8 = g.addV().property("aai-node-type", "model")
				.property("model-invariant-id", "model-invariant-id-8").property("model-type", "widget").next();
		Vertex modelVer8 = g.addV().property("aai-node-type", "model-ver").property("model-version-id", "model-version-id-8")
				.property("aai-uri", "/service-design-and-creation/models/model/model-invariant-id-8/model-vers/model-ver/model-version-id-8")
				.property("model-name", "l3-network").property("model-version", "v1.0").next();
		edgeSerializer.addTreeEdge(g, model8, modelVer8);
		//l3-network
		Vertex l3Network1= g.addV().property("aai-node-type", "l3-network").property("network-id", "l3Network1")
				.property("model-invariant-id-local", "model-invariant-id-8").property("model-version-id-local", "model-version-id-8").next();
		Vertex l3Network2= g.addV().property("aai-node-type", "l3-network").property("network-id", "l3Network2")
				.property("model-invariant-id-local", "model-invariant-id-y").property("model-version-id-local", "model-version-id-y").next();
		Vertex l3Network3= g.addV().property("aai-node-type", "l3-network").property("network-id", "l3Network3")
				.property("model-invariant-id-local", "model-invariant-id-8").property("model-version-id-local", "model-version-id-8").next();
		edgeSerializer.addPrivateEdge(traversal, l3Network3, modelVer8, null);
		
		// Add model9/model-ver9
		Vertex model9 = g.addV().property("aai-node-type", "model")
				.property("model-invariant-id", "model-invariant-id-9").property("model-type", "widget").next();
		Vertex modelVer9 = g.addV().property("aai-node-type", "model-ver").property("model-version-id", "model-version-id-9")
				.property("aai-uri", "/service-design-and-creation/models/model/model-invariant-id-9/model-vers/model-ver/model-version-id-9")
				.property("model-name", "vf-module").property("model-version", "v1.0").next();
		edgeSerializer.addTreeEdge(g, model9, modelVer9);
		//vf-module
		Vertex vfModule1= g.addV().property("aai-node-type", "vf-module").property("vf-module-id", "vfModule1")
				.property("model-invariant-id-local", "model-invariant-id-9").property("model-version-id-local", "model-version-id-9").next();
		Vertex vfModule2= g.addV().property("aai-node-type", "vf-module").property("vf-module-id", "vfModule2")
				.property("model-invariant-id-local", "model-invariant-id-y").property("model-version-id-local", "model-version-id-y").next();
		Vertex vfModule3= g.addV().property("aai-node-type", "vf-module").property("vf-module-id", "vfModule3")
				.property("model-invariant-id-local", "model-invariant-id-9").property("model-version-id-local", "model-version-id-9").next();
		edgeSerializer.addPrivateEdge(traversal, vfModule3, modelVer9, null);
		
		// Add model10/model-ver10
		Vertex model10 = g.addV().property("aai-node-type", "model")
				.property("model-invariant-id", "model-invariant-id-10").property("model-type", "widget").next();
		Vertex modelVer10 = g.addV().property("aai-node-type", "model-ver").property("model-version-id", "model-version-id-10")
				.property("aai-uri", "/service-design-and-creation/models/model/model-invariant-id-10/model-vers/model-ver/model-version-id-10")
				.property("model-name", "collection").property("model-version", "v1.0").next();
		edgeSerializer.addTreeEdge(g, model10, modelVer10);
		//collection
		Vertex collection1= g.addV().property("aai-node-type", "collection").property("collection-id", "collection1")
				.property("model-invariant-id-local", "model-invariant-id-10").property("model-version-id-local", "model-version-id-10").next();
		Vertex collection2= g.addV().property("aai-node-type", "collection").property("collection-id", "collection2")
				.property("model-invariant-id-local", "model-invariant-id-y").property("model-version-id-local", "model-version-id-y").next();
		Vertex collection3= g.addV().property("aai-node-type", "collection").property("collection-id", "collection3")
				.property("model-invariant-id-local", "model-invariant-id-10").property("model-version-id-local", "model-version-id-10").next();
		edgeSerializer.addPrivateEdge(traversal, collection3, modelVer10, null);
		
		// Add model11/model-ver11
		Vertex model11 = g.addV().property("aai-node-type", "model")
				.property("model-invariant-id", "model-invariant-id-11").property("model-type", "widget").next();
		Vertex modelVer11 = g.addV().property("aai-node-type", "model-ver").property("model-version-id", "model-version-id-11")
				.property("aai-uri", "/service-design-and-creation/models/model/model-invariant-id-11/model-vers/model-ver/model-version-id-11")
				.property("model-name", "instance-group").property("model-version", "v1.0").next();
		edgeSerializer.addTreeEdge(g, model11, modelVer11);
		//instance-group
		Vertex instanceGroup1= g.addV().property("aai-node-type", "instance-group").property("id", "instanceGroup1")
				.property("model-invariant-id-local", "model-invariant-id-11").property("model-version-id-local", "model-version-id-11").next();
		Vertex instanceGroup2= g.addV().property("aai-node-type", "instance-group").property("id", "instanceGroup2")
				.property("model-invariant-id-local", "model-invariant-id-y").property("model-version-id-local", "model-version-id-y").next();
		Vertex instanceGroup3= g.addV().property("aai-node-type", "instance-group").property("id", "instanceGroup3")
				.property("model-invariant-id-local", "model-invariant-id-11").property("model-version-id-local", "model-version-id-11").next();
		edgeSerializer.addPrivateEdge(traversal, instanceGroup3, modelVer11, null);
		
		// Add model12/model-ver12
		Vertex model12 = g.addV().property("aai-node-type", "model")
				.property("model-invariant-id", "model-invariant-id-12").property("model-type", "widget").next();
		Vertex modelVer12 = g.addV().property("aai-node-type", "model-ver").property("model-version-id", "model-version-id-12")
				.property("aai-uri", "/service-design-and-creation/models/model/model-invariant-id-12/model-vers/model-ver/model-version-id-12")
				.property("model-name", "allotted-resource").property("model-version", "v1.0").next();
		edgeSerializer.addTreeEdge(g, model12, modelVer12);
		//allotted-resource
		Vertex allottedResource1= g.addV().property("aai-node-type", "allotted-resource").property("id", "allottedResource1")
				.property("model-invariant-id-local", "model-invariant-id-12").property("model-version-id-local", "model-version-id-12").next();
		Vertex allottedResource2= g.addV().property("aai-node-type", "allotted-resource").property("id", "allottedResource2")
				.property("model-invariant-id-local", "model-invariant-id-y").property("model-version-id-local", "model-version-id-y").next();
		Vertex allottedResource3= g.addV().property("aai-node-type", "allotted-resource").property("id", "allottedResource3")
				.property("model-invariant-id-local", "model-invariant-id-12").property("model-version-id-local", "model-version-id-12").next();
		edgeSerializer.addPrivateEdge(traversal, allottedResource3, modelVer12, null);
		
		
		migration = new MigrateModelVer(spy, loaderFactory, edgeIngestor, edgeSerializer, schemaVersions);
		migration.run();
	}

	@AfterEach
	public void cleanUp() {
		tx.rollback();
		graph.close();
	}

	@Test
	public void checkEdgeCreatedForConnector() {
		assertEquals(true,
				g.V().has("aai-node-type", "connector").has("resource-instance-id", "connector1")
						.out()
						.has("aai-node-type", "model-ver").has("model-version-id","model-version-id-1").hasNext());

		assertEquals( false,
				g.V().has("aai-node-type", "connector").has("resource-instance-id", "connector2")
						.out()
						.has("aai-node-type", "model-ver").has("model-version-id","model-version-id-1").hasNext(),
				"Edge not created");

		assertEquals(Long.valueOf(2L),
				g.V().has("aai-node-type", "model-ver").has("model-version-id", "model-version-id-1")
						.in().count().next(),
				"Edge exists to 2 connectors");
	}


	@Test
	public void checkEdgeCreatedForSerivceInstance() {
		assertEquals(true,
				g.V().has("aai-node-type", "service-instance").has("service-instance-id", "serviceinstance1")
						.out()
						.has("aai-node-type", "model-ver").has("model-version-id","model-version-id-2").hasNext());

		assertEquals( false,
				g.V().has("aai-node-type", "service-instance").has("service-instance-id", "serviceinstance2")
						.out()
						.has("aai-node-type", "model-ver").has("model-version-id","model-version-id-2").hasNext(),
				"Edge not created");

		assertEquals(Long.valueOf(2L),
				g.V().has("aai-node-type", "model-ver").has("model-version-id", "model-version-id-2")
						.in().count().next(),
				"Edge exists to only 2 service-instances");
	}
	
	@Test
	public void checkEdgeCreatedForPnf() {
		assertEquals(true,
				g.V().has("aai-node-type", "pnf").has("pnf-name", "pnfName1")
						.out()
						.has("aai-node-type", "model-ver").has("model-version-id","model-version-id-3").hasNext());

		assertEquals( false,
				g.V().has("aai-node-type", "pnf").has("pnf-name", "pnfName2")
						.out()
						.has("aai-node-type", "model-ver").has("model-version-id","model-version-id-3").hasNext(),
				"Edge not created");

		assertEquals(Long.valueOf(2L),
				g.V().has("aai-node-type", "model-ver").has("model-version-id", "model-version-id-3")
						.in().count().next(),
				"Edge exists to only 2 pnfs");
	}
	
	@Test
	public void checkEdgeCreatedForLogicalLink() {
		assertEquals(true,
				g.V().has("aai-node-type", "logical-link").has("link-name", "linkName1")
						.out()
						.has("aai-node-type", "model-ver").has("model-version-id","model-version-id-4").hasNext());

		assertEquals( false,
				g.V().has("aai-node-type", "logical-link").has("link-name", "linkName2")
						.out()
						.has("aai-node-type", "model-ver").has("model-version-id","model-version-id-4").hasNext(),
				"Edge not created");

		assertEquals(Long.valueOf(2L),
				g.V().has("aai-node-type", "model-ver").has("model-version-id", "model-version-id-4")
						.in().count().next(),
				"Edge exists to only 2 logical-link");
	}
	
	@Test
	public void checkEdgeCreatedForVnfc() {
		assertEquals(true,
				g.V().has("aai-node-type", "vnfc").has("vnfc-name", "vnfc1")
						.out()
						.has("aai-node-type", "model-ver").has("model-version-id","model-version-id-5").hasNext());

		assertEquals( false,
				g.V().has("aai-node-type", "vnfc").has("vnfc-name", "vnfc2")
						.out()
						.has("aai-node-type", "model-ver").has("model-version-id","model-version-id-5").hasNext(),
				"Edge not created");

		assertEquals(Long.valueOf(2L),
				g.V().has("aai-node-type", "model-ver").has("model-version-id", "model-version-id-5")
						.in().count().next(),
				"Edge exists to only 2 logical-link");
	}
	
	@Test
	public void checkEdgeCreatedForGenericVnf() {
		assertEquals(true,
				g.V().has("aai-node-type", "generic-vnf").has("vnf-id", "vnf1")
						.out()
						.has("aai-node-type", "model-ver").has("model-version-id","model-version-id-6").hasNext());

		assertEquals( false,
				g.V().has("aai-node-type", "generic-vnf").has("vnf-id", "vnf2")
						.out()
						.has("aai-node-type", "model-ver").has("model-version-id","model-version-id-6").hasNext(),
				"Edge not created");

		assertEquals(Long.valueOf(2L),
				g.V().has("aai-node-type", "model-ver").has("model-version-id", "model-version-id-6")
						.in().count().next(),
				"Edge exists to only 2 generic-vnfs");
	}
	
	@Test
	public void checkEdgeCreatedForConfiguration() {
		assertEquals(true,
				g.V().has("aai-node-type", "configuration").has("configuration-id", "configuration1")
						.out()
						.has("aai-node-type", "model-ver").has("model-version-id","model-version-id-7").hasNext());

		assertEquals( false,
				g.V().has("aai-node-type", "configuration").has("configuration-id", "configuration2")
						.out()
						.has("aai-node-type", "model-ver").has("model-version-id","model-version-id-7").hasNext(),
				"Edge not created");

		assertEquals(Long.valueOf(2L),
				g.V().has("aai-node-type", "model-ver").has("model-version-id", "model-version-id-7")
						.in().count().next(),
				"Edge exists to only 2 configurations");
	}
	
	@Test
	public void checkEdgeCreatedForl3Network() {
		assertEquals(true,
				g.V().has("aai-node-type", "l3-network").has("network-id", "l3Network1")
						.out()
						.has("aai-node-type", "model-ver").has("model-version-id","model-version-id-8").hasNext());

		assertEquals( false,
				g.V().has("aai-node-type", "l3-network").has("network-id", "l3Network2")
						.out()
						.has("aai-node-type", "model-ver").has("model-version-id","model-version-id-8").hasNext(),
				"Edge not created");

		assertEquals(Long.valueOf(2L),
				g.V().has("aai-node-type", "model-ver").has("model-version-id", "model-version-id-8")
						.in().count().next(),
				"Edge exists to only 2 l3-networks");
	}
	
	@Test
	public void checkEdgeCreatedForVfModule() {
		assertEquals(true,
				g.V().has("aai-node-type", "vf-module").has("vf-module-id", "vfModule1")
						.out()
						.has("aai-node-type", "model-ver").has("model-version-id","model-version-id-9").hasNext());

		assertEquals( false,
				g.V().has("aai-node-type", "vf-module").has(" vf-module-id", "vfModule2")
						.out()
						.has("aai-node-type", "model-ver").has("model-version-id","model-version-id-9").hasNext(),
				"Edge not created");

		assertEquals(Long.valueOf(2L),
				g.V().has("aai-node-type", "model-ver").has("model-version-id", "model-version-id-9")
						.in().count().next(),
				"Edge exists to only 2 vf-modules");
	}
	
	@Test
	public void checkEdgeCreatedForCollection() {
		assertEquals(true,
				g.V().has("aai-node-type", "collection").has("collection-id", "collection1")
						.out()
						.has("aai-node-type", "model-ver").has("model-version-id","model-version-id-10").hasNext());

		assertEquals( false,
				g.V().has("aai-node-type", "collection").has("collection-id", "collection2")
						.out()
						.has("aai-node-type", "model-ver").has("model-version-id","model-version-id-10").hasNext(),
				"Edge not created");

		assertEquals(Long.valueOf(2L),
				g.V().has("aai-node-type", "model-ver").has("model-version-id", "model-version-id-10")
						.in().count().next(),
				"Edge exists to only 2 collections");
	}
	
	@Test
	public void checkEdgeCreatedForInstanceGroup() {
		assertEquals(true,
				g.V().has("aai-node-type", "instance-group").has("id", "instanceGroup1")
						.out()
						.has("aai-node-type", "model-ver").has("model-version-id","model-version-id-11").hasNext());

		assertEquals( false,
				g.V().has("aai-node-type", "instance-group").has("collection-id", "instanceGroup2")
						.out()
						.has("aai-node-type", "model-ver").has("model-version-id","model-version-id-11").hasNext(),
				"Edge not created");

		assertEquals(Long.valueOf(2L),
				g.V().has("aai-node-type", "model-ver").has("model-version-id", "model-version-id-11")
						.in().count().next(),
				"Edge exists to only 2 instance-group2");
	}
	
	@Test
	public void checkEdgeCreatedForAllottedResource() {
		assertEquals(true,
				g.V().has("aai-node-type", "allotted-resource").has("id", "allottedResource1")
						.out()
						.has("aai-node-type", "model-ver").has("model-version-id","model-version-id-12").hasNext());

		assertEquals( false,
				g.V().has("aai-node-type", "allotted-resource").has("id", "allottedResource2")
						.out()
						.has("aai-node-type", "model-ver").has("model-version-id","model-version-id-12").hasNext(),
				"Edge not created");

		assertEquals(Long.valueOf(2L),
				g.V().has("aai-node-type", "model-ver").has("model-version-id", "model-version-id-12")
						.in().count().next(),
				"Edge exists to only 2 allotted-resource");
	}

}
