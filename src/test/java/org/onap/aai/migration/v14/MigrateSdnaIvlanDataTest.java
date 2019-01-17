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

import static org.junit.Assert.*;
import static org.mockito.Mockito.spy;
import static org.mockito.Mockito.when;

import java.util.ArrayList;
import java.util.List;

import org.apache.tinkerpop.gremlin.process.traversal.dsl.graph.GraphTraversalSource;
import org.apache.tinkerpop.gremlin.process.traversal.dsl.graph.__;
import org.apache.tinkerpop.gremlin.process.traversal.strategy.verification.ReadOnlyStrategy;
import org.apache.tinkerpop.gremlin.structure.Vertex;
import org.janusgraph.core.JanusGraph;
import org.janusgraph.core.JanusGraphFactory;
import org.janusgraph.core.JanusGraphTransaction;
import org.junit.Before;
import org.junit.Test;
import org.onap.aai.AAISetup;
import org.onap.aai.db.props.AAIProperties;
import org.onap.aai.dbmap.DBConnectionType;
import org.onap.aai.introspection.Loader;
import org.onap.aai.introspection.ModelType;
import org.onap.aai.serialization.engines.JanusGraphDBEngine;
import org.onap.aai.serialization.engines.QueryStyle;
import org.onap.aai.serialization.engines.TransactionalGraphEngine;

public class MigrateSdnaIvlanDataTest extends AAISetup {
	
	private final String CONFIGURATION_NODE_TYPE = "configuration";
	private final String EVC_NODE_TYPE = "evc";
	private final String FORWARDER_NODE_TYPE = "forwarder";
	private final String FORWRDER_EVC_NODE_TYPE = "forwarder-evc";	
	private final String FORWARDING_PATH_NODE_TYPE = "forwarding-path";
	private final String LAG_INTERFACE_NODE_TYPE = "lag-interface";
	private final String P_INTERFACE_NODE_TYPE = "p-interface";
	private final String PNF_NODE_TYPE = "pnf";
	private final String SERVICE_INSTANCE_NODE_TYPE = "service-instance";
	
	private final static ModelType introspectorFactoryType = ModelType.MOXY;
	private final static QueryStyle queryStyle = QueryStyle.TRAVERSAL;
	private final static DBConnectionType type = DBConnectionType.REALTIME;

	private Loader loader;
	private TransactionalGraphEngine dbEngine;
	private MigrateSdnaIvlanData migration;
	private GraphTraversalSource g;

	@Before
	public void setUp() throws Exception {
		g = tx.traversal();
		loader = loaderFactory.createLoaderForVersion(introspectorFactoryType, schemaVersions.getDefaultVersion());
		dbEngine = new JanusGraphDBEngine(
				queryStyle,
				type,
				loader);
		
		//PNF -  pnf1
		Vertex pnf1 = g.addV()
				.property("aai-node-type", "pnf")
				.property("pnf-name", "pnf1")
				.next();
		
		//P-INTERFACE - "11111.1"
		Vertex pInterface1 = g.addV()
				.property("aai-node-type", "p-interface")
				.property("interface-name", "11111.1")
				.next();
		
		//LAG-INTERFACE - lag-interface1
		Vertex lagInterface1 = g.addV()
				.property("aai-node-type", "lag-interface")
				.property("interface-name", "lag-interface1")
				.next();
		
		//CONFIGURATION - "test/evc/one"
		Vertex configuration1 = g.addV()
						.property("aai-node-type", "configuration")
						.property("configuration-id", "test/evc/one")
						.next();
		
		//CONFIGURATION - "test/evc/one-1"
		Vertex configuration1_1 = g.addV()
				.property("aai-node-type", "configuration")
				.property("configuration-id", "test/evc/one-1")
				.next();
		//CONFIGURATION - "test/evc/one-2"
		Vertex configuration1_2 = g.addV()
				.property("aai-node-type", "configuration")
				.property("configuration-id", "test/evc/one-2")
				.next();
		
		//FORWARDER - "test/evc/one" sequence 1
		Vertex forwarder1_1 = g.addV()
				.property("aai-node-type", "forwarder")
				.property("sequence", 1)
				.property("forwarder-role", "ingress")
				.next();

		//FORWARDER - "test/evc/one"  sequence 2
		Vertex forwarder1_2 = g.addV()
				.property("aai-node-type", "forwarder")
				.property("sequence", 2)
				.property("forwarder-role", "ingress")
				.next();
	
		//FORWARDING-PATH - "test/evc/one"
				Vertex forwardingPath1 = g.addV()
						.property("aai-node-type", "forwarding-path")
						.property("forwarding-path-id", "test/evc/one")
						.property("forwarding-path-name", "test/evc/one")		
						.next();
		
		//EVC - "test/evc/one"
		Vertex evc = g.addV()
				.property("aai-node-type", "evc")
				.property("evc-id", "test/evc/one")
				.next();		
		
		//FORWARDER-EVC - "test/evc/one-1"
		Vertex fwdEvc1_1 = g.addV()
				.property("aai-node-type", "forwarder-evc")
				.property("forwarder-evc-id", "test/evc/one-1")
				.next();
	
		//FORWARDER-EVC - "test/evc/one-2"
		Vertex fwdEvc1_2 = g.addV()
				.property("aai-node-type", "forwarder-evc")
				.property("forwarder-evc-id", "test/evc/one-2")
				.next();
		
		//pnf -> p-interface -> forwarder -> configuration -> forwarder-evc
		//pnf1 -> pInterface1 -> forwarder1_1 -> configuration1_1 -> fwdEvc1_1
		edgeSerializer.addTreeEdge(g, pnf1, pInterface1);
		edgeSerializer.addEdge(g, pInterface1,forwarder1_1);
		edgeSerializer.addEdge(g, forwarder1_1, configuration1_1);
		
		edgeSerializer.addEdge(g, forwardingPath1, configuration1);
		edgeSerializer.addTreeEdge(g, forwarder1_1, forwardingPath1);
		edgeSerializer.addTreeEdge(g, forwarder1_2, forwardingPath1);	
		
		edgeSerializer.addTreeEdge(g, configuration1_1, fwdEvc1_1);
		
		//pnf -> lag-interface -> forwarder -> configuration -> forwarder-evc
		//pnf1 -> lagInterface1 -> forwarder1_2 -> configuration1_2 -> fwdEvc1_2
		edgeSerializer.addTreeEdge(g, pnf1, lagInterface1);
		edgeSerializer.addEdge(g, forwarder1_2, configuration1_2);
		edgeSerializer.addEdge(g, lagInterface1, forwarder1_2);
		edgeSerializer.addTreeEdge(g, configuration1_2, fwdEvc1_2);
		
		
		
		TransactionalGraphEngine spy = spy(dbEngine);
        TransactionalGraphEngine.Admin adminSpy = spy(dbEngine.asAdmin());
        
        GraphTraversalSource traversal = g;
		GraphTraversalSource readOnly = tx.traversal(GraphTraversalSource.build().with(ReadOnlyStrategy.instance()));
		when (spy.tx()).thenReturn(tx);
		when(spy.asAdmin()).thenReturn(adminSpy);
		when(adminSpy.getTraversalSource()).thenReturn(traversal);
		when(adminSpy.getReadOnlyTraversalSource()).thenReturn(readOnly);
		
        migration = new MigrateSdnaIvlanData(spy, loaderFactory, edgeIngestor, edgeSerializer, schemaVersions);
        migration.run();
        		
	}

	@Test
	public void testSdnaIvlanMigration() {		
	
		assertTrue("Value of node-type forwarder-evc, forwarder-evc-id of test/evc/one-1 has ben updated with the ivlan property value of 111 ",
				g.V()
				.has(AAIProperties.NODE_TYPE, PNF_NODE_TYPE).has("pnf-name", "pnf1")
				.in("tosca.relationships.network.BindsTo")
				.has(AAIProperties.NODE_TYPE, P_INTERFACE_NODE_TYPE).has("interface-name", "11111.1")
				.in("org.onap.relationships.inventory.ForwardsTo")
				.where(__.out("org.onap.relationships.inventory.BelongsTo").has("forwarding-path-id", "test/evc/one"))
				.out("org.onap.relationships.inventory.Uses")
				.in("org.onap.relationships.inventory.BelongsTo")
				.has("ivlan", "111").hasNext());
		
		assertTrue("Value of node-type forwarder-evc, forwarder-evc-id of test/evc/one-2 has ben updated with the ivlan property value of 222 ",
				g.V()
				.has(AAIProperties.NODE_TYPE, PNF_NODE_TYPE).has("pnf-name", "pnf1")
				.in("tosca.relationships.network.BindsTo")
				.has(AAIProperties.NODE_TYPE, LAG_INTERFACE_NODE_TYPE).has("interface-name", "lag-interface1")
				.in("org.onap.relationships.inventory.ForwardsTo")
				.where(__.out("org.onap.relationships.inventory.BelongsTo").has("forwarding-path-id", "test/evc/one"))
				.out("org.onap.relationships.inventory.Uses")
				.in("org.onap.relationships.inventory.BelongsTo")
				.has("ivlan", "222").hasNext());
		
		assertTrue("Value of node-type P-INTERFACE with an interface-name of l11111.2 does not exist in Graph. Ivlan not Updated ",
				!g.V()
				.has(AAIProperties.NODE_TYPE, PNF_NODE_TYPE).has("pnf-name", "pnf1")
				.in("tosca.relationships.network.BindsTo")
				.has(AAIProperties.NODE_TYPE, P_INTERFACE_NODE_TYPE).has("interface-name", "11111.2")
				.in("org.onap.relationships.inventory.ForwardsTo")
				.where(__.out("org.onap.relationships.inventory.BelongsTo").has("forwarding-path-id", "test/evc/one"))
				.out("org.onap.relationships.inventory.Uses")
				.in("org.onap.relationships.inventory.BelongsTo")
				.has("ivlan", "333").hasNext());
		
		assertTrue("Value of node-type LAG-INTERFACE with an interface-name of lag-interface2 does not exist in Graph. Ivlan not Updated ",
				!g.V()
				.has(AAIProperties.NODE_TYPE, PNF_NODE_TYPE).has("pnf-name", "pnf1")
				.in("tosca.relationships.network.BindsTo")
				.has(AAIProperties.NODE_TYPE, LAG_INTERFACE_NODE_TYPE).has("interface-name", "lag-interface2")
				.in("org.onap.relationships.inventory.ForwardsTo")
				.where(__.out("org.onap.relationships.inventory.BelongsTo").has("forwarding-path-id", "test/evc/one"))
				.out("org.onap.relationships.inventory.Uses")
				.in("org.onap.relationships.inventory.BelongsTo")
				.has("ivlan", "444").hasNext());
		
		
		assertTrue("Value of node-type P-INTERFACE with an interface-name of 11111.3 and evc of test/evc/one_2 does not exist in Graph. Ivlan not Updated ",
				!g.V()
				.has(AAIProperties.NODE_TYPE, PNF_NODE_TYPE).has("pnf-name", "pnf1")
				.in("tosca.relationships.network.BindsTo")
				.has(AAIProperties.NODE_TYPE, P_INTERFACE_NODE_TYPE).has("interface-name", "11111.3")
				.in("org.onap.relationships.inventory.ForwardsTo")
				.where(__.out("org.onap.relationships.inventory.BelongsTo").has("forwarding-path-id", "test/evc/one_2"))
				.out("org.onap.relationships.inventory.Uses")
				.in("org.onap.relationships.inventory.BelongsTo")
				.has("ivlan", "555").hasNext());
		
		assertTrue("Value of node-type LAG-INTERFACE with an interface-name of lag-interface3 and evc of test/evc/one_2 does not exist in Graph. Ivlan not Updated ",
				!g.V()
				.has(AAIProperties.NODE_TYPE, PNF_NODE_TYPE).has("pnf-name", "pnf1")
				.in("tosca.relationships.network.BindsTo")
				.has(AAIProperties.NODE_TYPE, LAG_INTERFACE_NODE_TYPE).has("interface-name", "lag-interface3")
				.in("org.onap.relationships.inventory.ForwardsTo")
				.where(__.out("org.onap.relationships.inventory.BelongsTo").has("forwarding-path-id", "test/evc/one_2"))
				.out("org.onap.relationships.inventory.Uses")
				.in("org.onap.relationships.inventory.BelongsTo")
				.has("ivlan", "666").hasNext());	
		
		assertTrue("Value of node-type PNF with an pnf-name of pnf2 does not exist in Graph. Ivlan not Updated ",
				!g.V()
				.has(AAIProperties.NODE_TYPE, PNF_NODE_TYPE).has("pnf-name", "pnf2")
				.in("tosca.relationships.network.BindsTo")
				.has(AAIProperties.NODE_TYPE, P_INTERFACE_NODE_TYPE).has("interface-name", "22222.2")
				.in("org.onap.relationships.inventory.ForwardsTo")
				.where(__.out("org.onap.relationships.inventory.BelongsTo").has("forwarding-path-id", "test/evc/two"))
				.out("org.onap.relationships.inventory.Uses")
				.in("org.onap.relationships.inventory.BelongsTo")
				.has("ivlan", "777").hasNext());
		
		assertTrue("Value of node-type PNF with an pnf-name of pnf2 does not exist in Graph. Ivlan not Updated ",
				!g.V()
				.has(AAIProperties.NODE_TYPE, PNF_NODE_TYPE).has("pnf-name", "pnf2")
				.in("tosca.relationships.network.BindsTo")
				.has(AAIProperties.NODE_TYPE, LAG_INTERFACE_NODE_TYPE).has("interface-name", "lag-interface2")
				.in("org.onap.relationships.inventory.ForwardsTo")
				.where(__.out("org.onap.relationships.inventory.BelongsTo").has("forwarding-path-id", "test/evc/two"))
				.out("org.onap.relationships.inventory.Uses")
				.in("org.onap.relationships.inventory.BelongsTo")
				.has("ivlan", "888").hasNext());
		
	}
	
}
