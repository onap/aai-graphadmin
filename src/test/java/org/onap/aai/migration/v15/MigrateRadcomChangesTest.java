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
package org.onap.aai.migration.v15;

import static org.junit.Assert.*;
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

public class MigrateRadcomChangesTest extends AAISetup {

	private final static ModelType introspectorFactoryType = ModelType.MOXY;
	private final static QueryStyle queryStyle = QueryStyle.TRAVERSAL;
	private final static DBConnectionType type = DBConnectionType.REALTIME;

	private Loader loader;
	private TransactionalGraphEngine dbEngine;
	private MigrateRadcomChanges migration;
	private GraphTraversalSource g;

	@Before
	public void setUp() throws Exception {
		g = tx.traversal();
		loader = loaderFactory.createLoaderForVersion(introspectorFactoryType, schemaVersions.getDefaultVersion());
		dbEngine = new JanusGraphDBEngine(
				queryStyle,
				type,
				loader);

		Vertex genericVnf1 = g.addV().property("aai-node-type", "generic-vnf").property("vnf-id", "test-1")
				.property("vnf-name",  "name-1").property("vnf-type", "test")
				.property("model-invariant-id-local", "test").property("model-version-id-local", "test").property("model-customization-id", "test").next();
		Vertex genericVnf2 = g.addV().property("aai-node-type", "generic-vnf").property("vnf-id", "test-2")
				.property("vnf-name",  "name-2").property("vnf-type", "test")
				.property("model-invariant-id-local", "change").property("model-version-id-local", "change").property("model-customization-id", "change").next();
		Vertex genericVnf3 = g.addV().property("aai-node-type", "generic-vnf").property("vnf-id", "test-3")
				.property("vnf-name",  "no-service").property("vnf-type", "test")
				.property("model-invariant-id-local", "test").property("model-version-id-local", "test").property("model-customization-id", "test").next();
		Vertex genericVnf4 = g.addV().property("aai-node-type", "generic-vnf").property("vnf-id", "test-4")
				.property("vnf-name",  "no-invariant").property("vnf-type", "test")
				.property("model-version-id-local", "test").property("model-customization-id", "test").next();
		Vertex genericVnf5 = g.addV().property("aai-node-type", "generic-vnf").property("vnf-id", "test-5")
				.property("vnf-name",  "no-version").property("vnf-type", "test")
				.property("model-invariant-id-local", "test").property("model-customization-id", "test").next();
		Vertex genericVnf6 = g.addV().property("aai-node-type", "generic-vnf").property("vnf-id", "test-6")
				.property("vnf-name",  "no-customization").property("vnf-type", "test")
				.property("model-invariant-id-local", "test").property("model-version-id-local", "test").next();
		Vertex genericVnf7 = g.addV().property("aai-node-type", "generic-vnf").property("vnf-id", "test-7")
				.property("vnf-name",  "no ids").property("vnf-type", "test").next();
		Vertex genericVnf8 = g.addV().property("aai-node-type", "generic-vnf").property("vnf-id", "test-8")
				.property("vnf-name",  "many-service-1").property("vnf-type", "test")
				.property("model-invariant-id-local", "test").property("model-version-id-local", "test").property("model-customization-id", "test").next();
		Vertex genericVnf9 = g.addV().property("aai-node-type", "generic-vnf").property("vnf-id", "test-9")
				.property("vnf-name",  "many-service-2").property("vnf-type", "test")
				.property("model-invariant-id-local", "test").property("model-version-id-local", "test").property("model-customization-id", "test").next();
		Vertex genericVnf10 = g.addV().property("aai-node-type", "generic-vnf").property("vnf-id", "test-10")
				.property("vnf-name",  "multi-name").property("vnf-type", "test").next();
		Vertex genericVnf11 = g.addV().property("aai-node-type", "generic-vnf").property("vnf-id", "test-11")
				.property("vnf-name",  "multi-name").property("vnf-type", "test")
				.property("model-invariant-id-local", "test").property("model-version-id-local", "test").property("model-customization-id", "test").next();
		Vertex genericVnf12 = g.addV().property("aai-node-type", "generic-vnf").property("vnf-id", "test-12")
				.property("vnf-name",  "wrong-type").property("vnf-type", "none").next();
		Vertex genericVnf13 = g.addV().property("aai-node-type", "generic-vnf").property("vnf-id", "test-13")
				.property("vnf-name",  "wrong-name").property("vnf-type", "test")
				.property("model-invariant-id-local", "test").property("model-version-id-local", "test").property("model-customization-id", "test").next();

		Vertex serviceInstance1 = g.addV().property("aai-node-type", "service-instance")
				.property("service-instance-id", "service-1")
				.property("model-invariant-id-local", "test").property("model-version-id-local", "test").next();
		Vertex serviceInstance2 = g.addV().property("aai-node-type", "service-instance")
				.property("service-instance-id", "service-2")
				.property("model-invariant-id-local", "diff").property("model-version-id-local", "diff").next();
		Vertex serviceInstance3 = g.addV().property("aai-node-type", "service-instance")
				.property("service-instance-id", "no-invariant")
				.property("model-version-id-local", "test").next();
		Vertex serviceInstance4 = g.addV().property("aai-node-type", "service-instance")
				.property("service-instance-id", "no-version")
				.property("model-invariant-id-local", "test").next();
		Vertex serviceInstance5 = g.addV().property("aai-node-type", "service-instance")
				.property("service-instance-id", "no ids").next();
		Vertex serviceInstance6 = g.addV().property("aai-node-type", "service-instance")
				.property("service-instance-id", "service-many")
				.property("model-invariant-id-local", "test").property("model-version-id-local", "test").next();
		Vertex serviceInstance7 = g.addV().property("aai-node-type", "service-instance")
				.property("service-instance-id", "wrong").next();
		Vertex serviceInstance8 = g.addV().property("aai-node-type", "service-instance")
				.property("service-instance-id", "connected-wrong")
				.property("model-invariant-id-local", "test").property("model-version-id-local", "test").next();
		
		Vertex serviceModel = g.addV().property("aai-node-type", "model")
				.property("model-invariant-id", "new-service-inv").property("model-type", "Service").next();
		Vertex serviceModelVer = g.addV().property("aai-node-type", "model-ver")
				.property("model-version-id", "new-service-ver").property("model-name", "test-service")
				.property("version", "test").next();
		Vertex resourceModel = g.addV().property("aai-node-type", "model")
				.property("model-invariant-id", "new-resource-inv").property("model-type", "VNF-resource").next();
		Vertex resourceModelVer = g.addV().property("aai-node-type", "model-ver")
				.property("model-version-id", "new-resource-ver").property("model-name", "test-resource")
				.property("version", "test").next();
		Vertex resourceModelElement1 = g.addV().property("aai-node-type", "model-element")
				.property("model-element-uuid", "resource-element-start").property("new-data-del-flag", "T")
				.property("cardinality", "unbounded").next();
		Vertex newVfModuleModelVer2 = g.addV().property("aai-node-type", "model-ver")
				.property("model-version-id", "new-vf-module-ver-2").property("model-name", "model-2")
				.property("version", "test").next();
		Vertex newVfModuleModel2 = g.addV().property("aai-node-type", "model")
				.property("model-invariant-id", "new-vf-module-inv-2").property("model-type", "2").next();
		Vertex resourceModelElement2 = g.addV().property("aai-node-type", "model-element")
				.property("model-element-uuid", "resource-element-depth-1").property("new-data-del-flag", "T")
				.property("cardinality", "unbounded").next();
		Vertex newVfModuleModelVer3 = g.addV().property("aai-node-type", "model-ver")
				.property("model-version-id", "new-vf-module-ver-3").property("model-name", "model-3")
				.property("version", "test").next();
		Vertex newVfModuleModel3 = g.addV().property("aai-node-type", "model")
				.property("model-invariant-id", "new-vf-module-inv-3").property("model-type", "3").next();
		Vertex resourceModelElement3 = g.addV().property("aai-node-type", "model-element")
				.property("model-element-uuid", "resource-element-depth-2-1").property("new-data-del-flag", "T")
				.property("cardinality", "unbounded").next();
		Vertex newVfModuleModelVer4 = g.addV().property("aai-node-type", "model-ver")
				.property("model-version-id", "new-vf-module-ver-4").property("model-name", "model-4")
				.property("version", "test").next();
		Vertex newVfModuleModel4 = g.addV().property("aai-node-type", "model")
				.property("model-invariant-id", "new-vf-module-inv-4").property("model-type", "4").next();
		Vertex resourceModelElement4 = g.addV().property("aai-node-type", "model-element")
				.property("model-element-uuid", "resource-element-depth-2-2").property("new-data-del-flag", "T")
				.property("cardinality", "unbounded").next();
		Vertex newVfModuleModelVer5 = g.addV().property("aai-node-type", "model-ver")
				.property("model-version-id", "new-vf-module-ver-5").property("model-name", "model-5")
				.property("version", "test").next();
		Vertex newVfModuleModel5 = g.addV().property("aai-node-type", "model")
				.property("model-invariant-id", "new-vf-module-inv-5").property("model-type", "5").next();
		Vertex resourceModelElement5 = g.addV().property("aai-node-type", "model-element")
				.property("model-element-uuid", "resource-element-depth-2-3").property("new-data-del-flag", "T")
				.property("cardinality", "unbounded").next();
		Vertex newVfModuleModelVer1 = g.addV().property("aai-node-type", "model-ver")
				.property("model-version-id", "new-vf-module-ver-1").property("model-name", "model-1")
				.property("version", "test").next();
		Vertex newVfModuleModel1 = g.addV().property("aai-node-type", "model")
				.property("model-invariant-id", "new-vf-module-inv-1").property("model-type", "1").next();
		
		Vertex vfModule1 = g.addV().property("aai-node-type", "vf-module")
				.property("vf-module-id", "vf-module-1")
				.property("model-invariant-id-local", "test").property("model-version-id-local", "test").next();
		Vertex vfModule2 = g.addV().property("aai-node-type", "vf-module")
				.property("vf-module-id", "vf-module-2")
				.property("model-invariant-id-local", "test").property("model-version-id-local", "test").next();
		Vertex vfModule3 = g.addV().property("aai-node-type", "vf-module")
				.property("vf-module-id", "no-invariant")
				.property("model-version-id-local", "test").next();
		Vertex vfModule4 = g.addV().property("aai-node-type", "vf-module")
				.property("vf-module-id", "no-ver")
				.property("model-invariant-id-local", "test").next();
		Vertex vfModule5 = g.addV().property("aai-node-type", "vf-module")
				.property("vf-module-id", "no-ids").next();
		Vertex badVfModule = g.addV().property("aai-node-type", "vf-module")
				.property("vf-module-id", "bad").next();
		
		Vertex vserver1 = g.addV().property("aai-node-type", "vserver")
				.property("vserver-id", "vserver-1").property("vserver-name", "vname-1").next();
		Vertex vserver2 = g.addV().property("aai-node-type", "vserver")
				.property("vserver-id", "vserver-2").property("vserver-name", "vname-2").next();
		Vertex unchangedVserver = g.addV().property("aai-node-type", "vserver")
				.property("vserver-id", "unchanged").property("vserver-name", "unchanged").next();
		
		Vertex image1 = g.addV().property("aai-node-type", "image")
				.property("image-id", "image-id-1").property("image-name", "image-1").next();
		Vertex image2 = g.addV().property("aai-node-type", "image")
				.property("image-id", "image-id-2").property("image-name", "image-2").next();
		Vertex oldImage = g.addV().property("aai-node-type", "image")
				.property("image-id", "image-old").property("image-name", "image-old-name").next();
		Vertex badImage = g.addV().property("aai-node-type", "image")
				.property("image-id", "image-bad").property("image-name", "image-bad").next();
		
		Vertex tenant1 = g.addV().property("aai-node-type", "tenant")
				.property("tenant-id", "tenant-id-1").property("tenant-name", "tenant-1").next();
		Vertex tenant2 = g.addV().property("aai-node-type", "tenant")
				.property("tenant-id", "tenant-id-2").property("tenant-name", "tenant-2").next();
		Vertex cloudRegion1 = g.addV().property("aai-node-type", "cloud-region")
				.property("cloud-region-id", "region-1").property("cloud-owner", "owner-1").next();
		Vertex cloudRegion2 = g.addV().property("aai-node-type", "cloud-region")
				.property("cloud-region-id", "region-2").property("cloud-owner", "owner-2").next();
	
		
		edgeSerializer.addEdge(g, genericVnf1, serviceInstance1);
		edgeSerializer.addEdge(g, genericVnf2, serviceInstance2);
		edgeSerializer.addEdge(g, genericVnf4, serviceInstance3);
		edgeSerializer.addEdge(g, genericVnf5, serviceInstance4);
		edgeSerializer.addEdge(g, genericVnf6, serviceInstance5);
		edgeSerializer.addEdge(g, genericVnf8, serviceInstance6);
		edgeSerializer.addEdge(g, genericVnf9, serviceInstance6);
		edgeSerializer.addEdge(g, genericVnf12, serviceInstance8);
		
		edgeSerializer.addTreeEdge(g, genericVnf2, vfModule1);
		edgeSerializer.addTreeEdge(g, genericVnf4, vfModule2);
		edgeSerializer.addTreeEdge(g, genericVnf5, vfModule3);
		edgeSerializer.addTreeEdge(g, genericVnf6, vfModule4);
		edgeSerializer.addTreeEdge(g, genericVnf7, vfModule5);
		edgeSerializer.addTreeEdge(g, genericVnf12, badVfModule);
		
		edgeSerializer.addTreeEdge(g, serviceModel, serviceModelVer);
		edgeSerializer.addTreeEdge(g, resourceModel, resourceModelVer);
		edgeSerializer.addTreeEdge(g, resourceModelVer, resourceModelElement1);
		edgeSerializer.addEdge(g, resourceModelElement1, newVfModuleModelVer2);
		edgeSerializer.addTreeEdge(g, newVfModuleModelVer2, newVfModuleModel2);
		edgeSerializer.addTreeEdge(g, resourceModelElement1, resourceModelElement2);
		edgeSerializer.addEdge(g, resourceModelElement2, newVfModuleModelVer3);
		edgeSerializer.addTreeEdge(g, newVfModuleModelVer3, newVfModuleModel3);
		edgeSerializer.addTreeEdge(g, resourceModelElement2, resourceModelElement3);
		edgeSerializer.addTreeEdge(g, resourceModelElement2, resourceModelElement4);
		edgeSerializer.addTreeEdge(g, resourceModelElement2, resourceModelElement5);
		edgeSerializer.addEdge(g, resourceModelElement3, newVfModuleModelVer4);
		edgeSerializer.addTreeEdge(g, newVfModuleModelVer4, newVfModuleModel4);
		edgeSerializer.addEdge(g, resourceModelElement4, newVfModuleModelVer5);
		edgeSerializer.addTreeEdge(g, newVfModuleModelVer5, newVfModuleModel5);
		edgeSerializer.addEdge(g, resourceModelElement5, newVfModuleModelVer1);
		edgeSerializer.addTreeEdge(g, newVfModuleModelVer1, newVfModuleModel1);	
		
		edgeSerializer.addEdge(g, vfModule1, vserver1);
		edgeSerializer.addEdge(g, vfModule2, vserver2);
		edgeSerializer.addEdge(g, vfModule4, unchangedVserver);
		edgeSerializer.addEdge(g, vserver2, oldImage);
		edgeSerializer.addEdge(g, unchangedVserver, badImage);
		edgeSerializer.addTreeEdge(g, image1, cloudRegion1);
		edgeSerializer.addTreeEdge(g, tenant1, cloudRegion1);
		edgeSerializer.addTreeEdge(g, tenant1, vserver1);
		edgeSerializer.addTreeEdge(g, image2, cloudRegion2);
		edgeSerializer.addTreeEdge(g, tenant2, cloudRegion2);
		edgeSerializer.addTreeEdge(g, tenant2, vserver2);
		
		TransactionalGraphEngine spy = spy(dbEngine);
		TransactionalGraphEngine.Admin adminSpy = spy(dbEngine.asAdmin());

		GraphTraversalSource traversal = g;
		GraphTraversalSource readOnly = tx.traversal(GraphTraversalSource.build().with(ReadOnlyStrategy.instance()));
		when (spy.tx()).thenReturn(tx);
		when(spy.asAdmin()).thenReturn(adminSpy);
		when(adminSpy.getTraversalSource()).thenReturn(traversal);
		when(adminSpy.getReadOnlyTraversalSource()).thenReturn(readOnly);
		
		migration = new MigrateRadcomChanges(spy, loaderFactory, edgeIngestor, edgeSerializer, schemaVersions);
		migration.run();
	}
	
	@Test
	public void testGenericVnfsUpdated() throws Exception {
		// check if generic-vnf nodes are updated
		
		assertEquals("First generic-vnf updated invariant", "new-resource-inv", 
				g.V().has("aai-node-type", "generic-vnf").has("vnf-name", "name-1").next().value("model-invariant-id-local"));
		assertEquals("First generic-vnf updated version", "new-resource-ver", 
				g.V().has("aai-node-type", "generic-vnf").has("vnf-name", "name-1").next().value("model-version-id-local"));
		assertEquals("First generic-vnf updated customization", "new-resource-cust", 
				g.V().has("aai-node-type", "generic-vnf").has("vnf-name", "name-1").next().value("model-customization-id"));
		
		assertEquals("Second generic-vnf updated invariant", "new-resource-inv", 
				g.V().has("aai-node-type", "generic-vnf").has("vnf-name", "name-2").next().value("model-invariant-id-local"));
		assertEquals("Second generic-vnf updated version", "new-resource-ver", 
				g.V().has("aai-node-type", "generic-vnf").has("vnf-name", "name-2").next().value("model-version-id-local"));
		assertEquals("Second generic-vnf updated customization", "new-resource-cust", 
				g.V().has("aai-node-type", "generic-vnf").has("vnf-name", "name-2").next().value("model-customization-id"));
		
		assertEquals("Generic-vnf with no service updated invariant", "new-resource-inv", 
				g.V().has("aai-node-type", "generic-vnf").has("vnf-name", "no-service").next().value("model-invariant-id-local"));
		assertEquals("Generic-vnf with no service updated version", "new-resource-ver", 
				g.V().has("aai-node-type", "generic-vnf").has("vnf-name", "no-service").next().value("model-version-id-local"));
		assertEquals("Generic-vnf with no service updated customization", "new-resource-cust", 
				g.V().has("aai-node-type", "generic-vnf").has("vnf-name", "no-service").next().value("model-customization-id"));
		
		assertEquals("Generic-vnf with no invariant updated invariant", "new-resource-inv", 
				g.V().has("aai-node-type", "generic-vnf").has("vnf-name", "no-invariant").next().value("model-invariant-id-local"));
		assertEquals("Generic-vnf with no invariant updated version", "new-resource-ver", 
				g.V().has("aai-node-type", "generic-vnf").has("vnf-name", "no-invariant").next().value("model-version-id-local"));
		assertEquals("Generic-vnf with no invariant updated customization", "new-resource-cust", 
				g.V().has("aai-node-type", "generic-vnf").has("vnf-name", "no-invariant").next().value("model-customization-id"));
		
		assertEquals("Generic-vnf with no version updated invariant", "new-resource-inv", 
				g.V().has("aai-node-type", "generic-vnf").has("vnf-name", "no-version").next().value("model-invariant-id-local"));
		assertEquals("Generic-vnf with no version updated version", "new-resource-ver", 
				g.V().has("aai-node-type", "generic-vnf").has("vnf-name", "no-version").next().value("model-version-id-local"));
		assertEquals("Generic-vnf with no version updated customization", "new-resource-cust", 
				g.V().has("aai-node-type", "generic-vnf").has("vnf-name", "no-version").next().value("model-customization-id"));
		
		assertEquals("Generic-vnf with no customization updated invariant", "new-resource-inv", 
				g.V().has("aai-node-type", "generic-vnf").has("vnf-name", "no-customization").next().value("model-invariant-id-local"));
		assertEquals("Generic-vnf with no customization updated version", "new-resource-ver", 
				g.V().has("aai-node-type", "generic-vnf").has("vnf-name", "no-customization").next().value("model-version-id-local"));
		assertEquals("Generic-vnf with no customization updated customization", "new-resource-cust", 
				g.V().has("aai-node-type", "generic-vnf").has("vnf-name", "no-customization").next().value("model-customization-id"));
		
		assertEquals("Generic-vnf with no ids updated invariant", "new-resource-inv", 
				g.V().has("aai-node-type", "generic-vnf").has("vnf-name", "no ids").next().value("model-invariant-id-local"));
		assertEquals("Generic-vnf with no ids updated version", "new-resource-ver", 
				g.V().has("aai-node-type", "generic-vnf").has("vnf-name", "no ids").next().value("model-version-id-local"));
		assertEquals("Generic-vnf with no version updated customization", "new-resource-cust", 
				g.V().has("aai-node-type", "generic-vnf").has("vnf-name", "no-version").next().value("model-customization-id"));
		
		assertEquals("First generic-vnf for many-to-service test updated invariant", "new-resource-inv", 
				g.V().has("aai-node-type", "generic-vnf").has("vnf-name", "many-service-1").next().value("model-invariant-id-local"));
		assertEquals("First generic-vnf for many-to-service test updated version", "new-resource-ver", 
				g.V().has("aai-node-type", "generic-vnf").has("vnf-name", "many-service-1").next().value("model-version-id-local"));
		assertEquals("First generic-vnf for many-to-service test updated customization", "new-resource-cust", 
				g.V().has("aai-node-type", "generic-vnf").has("vnf-name", "many-service-1").next().value("model-customization-id"));
		
		assertEquals("Second generic-vnf for many-to-service test updated invariant", "new-resource-inv", 
				g.V().has("aai-node-type", "generic-vnf").has("vnf-name", "many-service-2").next().value("model-invariant-id-local"));
		assertEquals("Second generic-vnf for many-to-service test updated version", "new-resource-ver", 
				g.V().has("aai-node-type", "generic-vnf").has("vnf-name", "many-service-2").next().value("model-version-id-local"));
		assertEquals("Second generic-vnf for many-to-service test updated customization", "new-resource-cust", 
				g.V().has("aai-node-type", "generic-vnf").has("vnf-name", "many-service-2").next().value("model-customization-id"));
		
		
		assertEquals("First generic-vnf for multi-name test updated invariant", "new-resource-inv", 
				g.V().has("aai-node-type", "generic-vnf").has("vnf-id", "test-10").next().value("model-invariant-id-local"));
		assertEquals("First generic-vnf for multi-name test updated version", "new-resource-ver", 
				g.V().has("aai-node-type", "generic-vnf").has("vnf-id", "test-10").next().value("model-version-id-local"));
		assertEquals("First generic-vnf for multi-name test updated customization", "new-resource-cust", 
				g.V().has("aai-node-type", "generic-vnf").has("vnf-id", "test-10").next().value("model-customization-id"));
		
		assertEquals("Second generic-vnf for multi-name test updated invariant", "new-resource-inv", 
				g.V().has("aai-node-type", "generic-vnf").has("vnf-id", "test-11").next().value("model-invariant-id-local"));
		assertEquals("Second generic-vnf for multi-name test updated version", "new-resource-ver", 
				g.V().has("aai-node-type", "generic-vnf").has("vnf-id", "test-11").next().value("model-version-id-local"));
		assertEquals("Second generic-vnf for multi-name test customization", "new-resource-cust", 
				g.V().has("aai-node-type", "generic-vnf").has("vnf-id", "test-11").next().value("model-customization-id"));
	}
	

	@Test
	public void testServiceInstancesUpdated() throws Exception {
		// check if service-instance nodes are updated	
		
		assertEquals("First service-instance updated invariant", "new-service-inv", 
				g.V().has("aai-node-type", "service-instance").has("service-instance-id", "service-1").next().value("model-invariant-id-local"));
		assertEquals("First service-instance-updated version", "new-service-ver", 
				g.V().has("aai-node-type", "service-instance").has("service-instance-id", "service-1").next().value("model-version-id-local"));
		
		assertEquals("Second service-instance updated invariant", "new-service-inv", 
				g.V().has("aai-node-type", "service-instance").has("service-instance-id", "service-2").next().value("model-invariant-id-local"));
		assertEquals("Second service-instance-updated version", "new-service-ver", 
				g.V().has("aai-node-type", "service-instance").has("service-instance-id", "service-2").next().value("model-version-id-local"));
		
		assertEquals("Service-instance with no invariant updated invariant", "new-service-inv", 
				g.V().has("aai-node-type", "service-instance").has("service-instance-id", "no-invariant").next().value("model-invariant-id-local"));
		assertEquals("Service-instance with no invariant updated version", "new-service-ver", 
				g.V().has("aai-node-type", "service-instance").has("service-instance-id", "no-invariant").next().value("model-version-id-local"));
		
		assertEquals("Service-instance with no version updated invariant", "new-service-inv", 
				g.V().has("aai-node-type", "service-instance").has("service-instance-id", "no-version").next().value("model-invariant-id-local"));
		assertEquals("Service-instance with no version updated version", "new-service-ver", 
				g.V().has("aai-node-type", "service-instance").has("service-instance-id", "no-version").next().value("model-version-id-local"));
		
		assertEquals("Service-instance with no ids updated invariant", "new-service-inv", 
				g.V().has("aai-node-type", "service-instance").has("service-instance-id", "no ids").next().value("model-invariant-id-local"));
		assertEquals("Service-instance with no ids updated version", "new-service-ver", 
				g.V().has("aai-node-type", "service-instance").has("service-instance-id", "no ids").next().value("model-version-id-local"));
		
		assertEquals("Service-instance for many-to-service test updated invariant", "new-service-inv", 
				g.V().has("aai-node-type", "service-instance").has("service-instance-id", "service-many").next().value("model-invariant-id-local"));
		assertEquals("Service-instance for many-to-service test updated version", "new-service-ver", 
				g.V().has("aai-node-type", "service-instance").has("service-instance-id", "service-many").next().value("model-version-id-local"));		
	}
	
	@Test
	public void testVfModulesUpdated() throws Exception {
		//test if vf-module nodes are updated
		
		assertEquals("First vf-module updated invariant", "new-vf-module-inv-1", 
				g.V().has("aai-node-type", "vf-module").has("vf-module-id", "vf-module-1").next().value("model-invariant-id-local"));
		assertEquals("First vf-module updated version", "new-vf-module-ver-1", 
				g.V().has("aai-node-type", "vf-module").has("vf-module-id", "vf-module-1").next().value("model-version-id-local"));
		
		assertEquals("Second vf-module updated invariant", "new-vf-module-inv-2", 
				g.V().has("aai-node-type", "vf-module").has("vf-module-id", "vf-module-2").next().value("model-invariant-id-local"));
		assertEquals("Second vf-module updated version", "new-vf-module-ver-2", 
				g.V().has("aai-node-type", "vf-module").has("vf-module-id", "vf-module-2").next().value("model-version-id-local"));
		
		assertEquals("Vf-module with no invariant updated invariant", "new-vf-module-inv-3", 
				g.V().has("aai-node-type", "vf-module").has("vf-module-id", "no-invariant").next().value("model-invariant-id-local"));
		assertEquals("Vf-module with no invariant updated version", "new-vf-module-ver-3", 
				g.V().has("aai-node-type", "vf-module").has("vf-module-id", "no-invariant").next().value("model-version-id-local"));
		
		assertEquals("Vf-module with no version updated invariant", "new-vf-module-inv-4", 
				g.V().has("aai-node-type", "vf-module").has("vf-module-id", "no-ver").next().value("model-invariant-id-local"));
		assertEquals("Vf-module with no version updated version", "new-vf-module-ver-4", 
				g.V().has("aai-node-type", "vf-module").has("vf-module-id", "no-ver").next().value("model-version-id-local"));
		
		assertEquals("Vf-module with no ids updated invariant", "new-vf-module-inv-5", 
				g.V().has("aai-node-type", "vf-module").has("vf-module-id", "no-ids").next().value("model-invariant-id-local"));
		assertEquals("Vf-module with no ids updated version", "new-vf-module-ver-5", 
				g.V().has("aai-node-type", "vf-module").has("vf-module-id", "no-ids").next().value("model-version-id-local"));
	}
	
	@Test
	public void testVserverAndImageUpdated() throws Exception {
		//test if vserver-image relationships are updated
		assertTrue("Vserver not connected to image is connected to new image",
				g.V().has("aai-node-type", "vserver").has("vserver-id", "vserver-1").out("org.onap.relationships.inventory.Uses")
				.has("aai-node-type", "image").has("image-id", "image-id-1").hasNext());
		assertTrue("Vserver connected to existing image is connected to new image",
				g.V().has("aai-node-type", "vserver").has("vserver-id", "vserver-2").out("org.onap.relationships.inventory.Uses")
				.has("aai-node-type", "image").has("image-id", "image-id-2").hasNext());
		assertFalse("Vserver connected to existing image is not connected to that image",
				g.V().has("aai-node-type", "vserver").has("vserver-id", "vserver-2").out("org.onap.relationships.inventory.Uses")
				.has("aai-node-type", "image").has("image-id", "image-old").hasNext());	
		assertTrue("Existing image still exists",
				g.V().has("aai-node-type", "image").has("image-id", "image-old").hasNext());
	}
	
	@Test
	public void testNodesNotUpdated() throws Exception {
		// negative tests
	
		assertFalse("Generic-vnf with wrong type has unchanged invariant", 
				g.V().has("aai-node-type", "generic-vnf").has("vnf-name", "wrong-type").next()
				.property("model-invariant-id-local").isPresent());
		assertFalse("Generic-vnf with wrong type has unchanged version", 
				g.V().has("aai-node-type", "generic-vnf").has("vnf-name", "wrong-type").next()
				.property("model-version-id-local").isPresent());
		assertFalse("Generic-vnf with wrong type has unchanged customization", 
				g.V().has("aai-node-type", "generic-vnf").has("vnf-name", "wrong-type").next()
				.property("model-customizaiton-id").isPresent());
		
		assertEquals("Generic-vnf with wrong name has unchanged invariant", "test", 
				g.V().has("aai-node-type", "generic-vnf").has("vnf-name", "wrong-name").next().value("model-invariant-id-local"));
		assertEquals("Generic-vnf with wrong name has unchanged version", "test", 
				g.V().has("aai-node-type", "generic-vnf").has("vnf-name", "wrong-name").next().value("model-version-id-local"));
		assertEquals("Generic-vnf with wrong name has unchanged customization", "test", 
				g.V().has("aai-node-type", "generic-vnf").has("vnf-name", "wrong-name").next().value("model-customization-id"));
		
		assertFalse("Unconnected service-instance has unchanged invariant", 
				g.V().has("aai-node-type", "service-instance").has("service-instance-id", "wrong").next()
				.property("model-invariant-id-local").isPresent());
		assertFalse("Unconnected service-instance has unchanged version", 
				g.V().has("aai-node-type", "service-instance").has("service-instance-id", "wrong").next()
				.property("model-version-id-local").isPresent());
		
		assertEquals("Service-instance connected to unctouched generic-vnf has unchanged invariant", "test", 
				g.V().has("aai-node-type", "service-instance").has("service-instance-id", "connected-wrong")
				.next().value("model-invariant-id-local"));
		assertEquals("Service-instance connected to untouched generic-vnf has unchanged version", "test", 
				g.V().has("aai-node-type", "service-instance").has("service-instance-id", "connected-wrong")
				.next().value("model-version-id-local"));	
		
		assertFalse("Vf-module connected to untouched generic-vnf has unchanged invariant", 
				g.V().has("aai-node-type", "vf-module").has("vf-module-id", "bad")
				.next().property("model-invariant-id-local").isPresent());
		assertFalse("Vf-module connected to untouched generic-vnf has unchanged version", 
				g.V().has("aai-node-type", "vf-module").has("vf-module-id", "bad")
				.next().property("model-version-id-local").isPresent());

		assertTrue("Untouched vserver still connected to image",
				g.V().has("aai-node-type", "vserver").has("vserver-id", "unchanged").out("org.onap.relationships.inventory.Uses")
				.has("aai-node-type", "image").has("image-id", "image-bad").hasNext());
	}
	
	@Test
	public void testGetAffectedNodeTypes() {
		Optional<String[]> types = migration.getAffectedNodeTypes();
		Optional<String[]> expected = Optional.of(new String[]{"generic-vnf", "service-instance", "vf-module", "vserver", "image"});
		
		assertNotNull(types);
		assertArrayEquals(expected.get(), types.get());
	}

	@Test
	public void testGetMigrationName() {
		String migrationName = migration.getMigrationName();

		assertNotNull(migrationName);
		assertEquals("MigrateRadcomChanges", migrationName);
	}
}
