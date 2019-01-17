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

import org.onap.aai.AAISetup;
import org.onap.aai.edges.EdgeIngestor;
import org.onap.aai.introspection.LoaderFactory;
import org.onap.aai.serialization.db.EdgeSerializer;
import org.onap.aai.serialization.engines.TransactionalGraphEngine;


import org.janusgraph.core.JanusGraph;
import org.janusgraph.core.JanusGraphFactory;
import org.janusgraph.core.JanusGraphTransaction;
import org.apache.tinkerpop.gremlin.process.traversal.dsl.graph.GraphTraversalSource;
import org.junit.Before;
import org.junit.Test;
import org.onap.aai.dbmap.DBConnectionType;
import org.onap.aai.introspection.Loader;
import org.onap.aai.introspection.ModelType;
import org.onap.aai.setup.SchemaVersions;
import org.onap.aai.setup.SchemaVersion;
import org.onap.aai.migration.Status;
import org.onap.aai.migration.v15.MigrateBooleanDefaultsToFalse;
import org.onap.aai.serialization.engines.QueryStyle;
import org.onap.aai.serialization.engines.JanusGraphDBEngine;

import static org.mockito.Mockito.spy;
import static org.mockito.Mockito.when;

import java.util.Optional;

import static org.junit.Assert.assertTrue;

public class MigrateBooleanDefaultsToFalseTest extends AAISetup {

	public static class BooleanDefaultMigrator extends MigrateBooleanDefaultsToFalse {
        public BooleanDefaultMigrator(TransactionalGraphEngine engine, LoaderFactory loaderFactory, EdgeIngestor edgeIngestor, EdgeSerializer edgeSerializer, SchemaVersions schemaVersions){
            super(engine, loaderFactory, edgeIngestor, edgeSerializer, schemaVersions);
        }
        @Override
        public Status getStatus() {
            return Status.SUCCESS;
        }
        @Override
        public Optional<String[]> getAffectedNodeTypes() {
        	return Optional.of(new String[]{VNF_NODE_TYPE,VSERVER_NODE_TYPE,VNFC_NODE_TYPE,L3NETWORK_NODE_TYPE,SUBNET_NODE_TYPE,LINTERFACE_NODE_TYPE,VFMODULE_NODE_TYPE});
        }
        @Override
        public String getMigrationName() {
            return "MockBooleanDefaultMigrator";
        }
    }

    private final static ModelType introspectorFactoryType = ModelType.MOXY;
    private final static QueryStyle queryStyle = QueryStyle.TRAVERSAL;
    private final static DBConnectionType type = DBConnectionType.REALTIME;
    private Loader loader;
    private TransactionalGraphEngine dbEngine;
    private BooleanDefaultMigrator migration;
    private GraphTraversalSource g;

    @Before
    public void setup() throws Exception{
        g = tx.traversal();
        loader = loaderFactory.createLoaderForVersion(introspectorFactoryType, schemaVersions.getDefaultVersion());
        dbEngine = new JanusGraphDBEngine(
                queryStyle,
                type,
                loader);

        //generic-vnf
        g.addV().property("aai-node-type", "generic-vnf")
                .property("vnf-id", "generic-vnf0")
                .next();
        g.addV().property("aai-node-type", "generic-vnf")
                .property("vnf-id", "generic-vnf1")
                .property("is-closed-loop-disabled", "")
                .next();
        g.addV().property("aai-node-type", "generic-vnf")
                .property("vnf-id", "generic-vnf2")
                .property("is-closed-loop-disabled", true)
                .next();
        g.addV().property("aai-node-type", "generic-vnf")
        		.property("vnf-id", "generic-vnf3")
        		.property("is-closed-loop-disabled", false)
        		.next();
        //vnfc
        g.addV().property("aai-node-type", "vnfc")
                .property("vnfc-name", "vnfc0")
                .next();
        g.addV().property("aai-node-type", "vnfc")
                .property("vnfc-name", "vnfc1")
                .property("is-closed-loop-disabled", "")
                .next();
        g.addV().property("aai-node-type", "vnfc")
                .property("vnfc-name", "vnfc2")
                .property("is-closed-loop-disabled", true)
                .next();
        g.addV().property("aai-node-type", "vnfc")
        		.property("vnfc-name", "vnfc3")
        		.property("is-closed-loop-disabled", false)
        		.next();
        //vserver
        g.addV().property("aai-node-type", "vserver")
                .property("vserver-id", "vserver0")
                .next();
        g.addV().property("aai-node-type", "vserver")
                .property("vserver-id", "vserver1")
                .property("is-closed-loop-disabled", "")
                .next();
        g.addV().property("aai-node-type", "vserver")
                .property("vserver-id", "vserver2")
                .property("is-closed-loop-disabled", true)
                .next();
        g.addV().property("aai-node-type", "vserver")
        		.property("vserver-id", "vserver3")
        		.property("is-closed-loop-disabled", false)
        		.next();        
      //l3-network
        g.addV().property("aai-node-type", "l3-network")
                .property("network-id", "l3-network0")
				.property("network-name", "l3-network-name0")
                .next();
        g.addV().property("aai-node-type", "l3-network")
                .property("network-id", "l3-network1")
				.property("network-name", "l3-network-name1")
                .property("is-bound-to-vpn", "")
                .property("is-provider-network", "")
				.property("is-shared-network", "")
				.property("is-external-network", "")
                .next();
        g.addV().property("aai-node-type", "l3-network")
                .property("network-id", "l3-network2")
				.property("network-name", "l3-network-name2")
                .property("is-bound-to-vpn", true)
                .property("is-provider-network", true)
				.property("is-shared-network", true)
				.property("is-external-network", true)
                .next();
        g.addV().property("aai-node-type", "l3-network")
        		.property("network-id", "l3-network3")
				.property("network-name", "l3-network-name3")
        		.property("is-bound-to-vpn", false)
        		.property("is-provider-network", false)
				.property("is-shared-network", false)
				.property("is-external-network", false)
        		.next();       
        //subnet
        g.addV().property("aai-node-type", "subnet")
                .property("subnet-id", "subnet0")
                .next();
        g.addV().property("aai-node-type", "subnet")
                .property("subnet-id", "subnet1")
                .property("dhcp-enabled", "")
                .next();
        g.addV().property("aai-node-type", "subnet")
                .property("subnet-id", "subnet2")
                .property("dhcp-enabled", true)
                .next();
        g.addV().property("aai-node-type", "subnet")
        		.property("subnet-id", "subnet3")
        		.property("dhcp-enabled", false)
        		.next();
      //l-interface
        g.addV().property("aai-node-type", "l-interface")
                .property("interface-name", "l-interface0")
				.property("in-maint", false)
                .next();
        g.addV().property("aai-node-type", "l-interface")
                .property("interface-name", "l-interface1")
                .property("in-maint", false)
				.property("is-port-mirrored", "")
				.property("is-ip-unnumbered", "")
                .next();
        g.addV().property("aai-node-type", "l-interface")
                .property("interface-name", "l-interface2")
                .property("in-maint", false)
				.property("is-port-mirrored", true)
				.property("is-ip-unnumbered", true)
                .next();
        g.addV().property("aai-node-type", "l-interface")
        		.property("interface-name", "l-interface3")
        		.property("in-maint", false)
				.property("is-port-mirrored", false)
				.property("is-ip-unnumbered", false)
        		.next(); 
      //vf-module
        g.addV().property("aai-node-type", "vf-module")
                .property("vf-module-id", "vf-module0")
                .next();
        g.addV().property("aai-node-type", "vf-module")
                .property("vf-module-id", "vf-module1")
				.property("is-base-vf-module", "")
                .next();
        g.addV().property("aai-node-type", "vf-module")
                .property("vf-module-id", "vf-module2")
				.property("is-base-vf-module", true)
                .next();
        g.addV().property("aai-node-type", "vf-module")
        		.property("vf-module-id", "vf-module3")
				.property("is-base-vf-module", false)				
        		.next(); 
                     
      //vlan
        g.addV().property("aai-node-type", "vlan")
                .property("vlan-interface", "vlan0")
                .next();
        g.addV().property("aai-node-type", "vlan")
                .property("vlan-interface", "vlan1")
				.property("is-ip-unnumbered", "")
                .next();
        g.addV().property("aai-node-type", "vlan")
                .property("vlan-interface", "vlan2")
				.property("is-ip-unnumbered", true)
                .next();
        g.addV().property("aai-node-type", "vlan")
        		.property("vlan-interface", "vlan3")
				.property("is-ip-unnumbered", false)				
        		.next();
        
        TransactionalGraphEngine spy = spy(dbEngine);
        TransactionalGraphEngine.Admin adminSpy = spy(dbEngine.asAdmin());
        GraphTraversalSource traversal = g;
        when(spy.asAdmin()).thenReturn(adminSpy);
        when(adminSpy.getTraversalSource()).thenReturn(traversal);
        migration = new BooleanDefaultMigrator(spy, loaderFactory, edgeIngestor, edgeSerializer, schemaVersions);
        migration.run();
        
    }

    @Test
    public void testMissingProperty(){
    	//is-closed-loop-disabled
        assertTrue("Value of generic-vnf should be updated since the property is-closed-loop-disabled doesn't exist",
                g.V().has("aai-node-type", "generic-vnf").has("vnf-id", "generic-vnf0").has("is-closed-loop-disabled", false).hasNext());
        assertTrue("Value of vnfc should be updated since the property is-closed-loop-disabled doesn't exist",
                g.V().has("aai-node-type", "vnfc").has("vnfc-name", "vnfc0").has("is-closed-loop-disabled", false).hasNext());
        assertTrue("Value of vserver should be updated since the property is-closed-loop-disabled doesn't exist",
                g.V().has("aai-node-type", "vserver").has("vserver-id", "vserver0").has("is-closed-loop-disabled", false).hasNext());
        //dhcp-enabled
        assertTrue("Value of subnet should be updated since the property dhcp-enabled doesn't exist",
                g.V().has("aai-node-type", "subnet").has("subnet-id", "subnet0").has("dhcp-enabled", false).hasNext());
        //l3-network: is-bound-to-vpn, is-shared-network, is-external-network
        assertTrue("Value of l3-network should be updated since the property is-bound-to-vpn doesn't exist",
                g.V().has("aai-node-type", "l3-network").has("network-id", "l3-network0").has("network-name", "l3-network-name0").has("is-bound-to-vpn", false).hasNext());  
        assertTrue("Value of l3-network should be updated since the property is-provider-network doesn't exist",
                g.V().has("aai-node-type", "l3-network").has("network-id", "l3-network0").has("network-name", "l3-network-name0").has("is-provider-network", false).hasNext());  
        assertTrue("Value of l3-network should be updated since the property is-shared-network doesn't exist",
                g.V().has("aai-node-type", "l3-network").has("network-id", "l3-network0").has("network-name", "l3-network-name0").has("is-shared-network", false).hasNext());  
		assertTrue("Value of l3-network should be updated since the property is-external-network doesn't exist",
                g.V().has("aai-node-type", "l3-network").has("network-id", "l3-network0").has("network-name", "l3-network-name0").has("is-external-network", false).hasNext()); 
		//l-interface: is-port-mirrored, is-ip-unnumbered
		assertTrue("Value of l-interface should be updated since the property is-port-mirrored doesn't exist",
                g.V().has("aai-node-type", "l-interface").has("interface-name", "l-interface0").has("is-port-mirrored", false).hasNext());  
		assertTrue("Value of l-interface should be updated since the property is-ip-unnumbered doesn't exist",
                g.V().has("aai-node-type", "l-interface").has("interface-name", "l-interface0").has("is-ip-unnumbered", false).hasNext());
		//vf-module: is-base-vf-module
		assertTrue("Value of vf-module should be updated since the property is-base-vf-module doesn't exist",
                g.V().has("aai-node-type", "vf-module").has("vf-module-id", "vf-module0").has("is-base-vf-module", false).hasNext());  
		//vlan: is-ip-unnumbered
		assertTrue("Value of vlan should be updated since the property is-ip-unnumbered doesn't exist",
                g.V().has("aai-node-type", "vlan").has("vlan-interface", "vlan0").has("is-ip-unnumbered", false).hasNext());
    }

    @Test
    public void testEmptyValue() {                         
      //is-closed-loop-disabled
        assertTrue("Value of generic-vnf should be updated since the value for is-closed-loop-disabled is an empty string",
                g.V().has("aai-node-type", "generic-vnf").has("vnf-id", "generic-vnf1").has("is-closed-loop-disabled", false).hasNext());
        assertTrue("Value of vnfc should be updated since the value for is-closed-loop-disabled is an empty string",
                g.V().has("aai-node-type", "vnfc").has("vnfc-name", "vnfc1").has("is-closed-loop-disabled", false).hasNext());
        assertTrue("Value of vserver should be updated since the value for is-closed-loop-disabled is an empty string",
                g.V().has("aai-node-type", "vserver").has("vserver-id", "vserver1").has("is-closed-loop-disabled", false).hasNext());
        //dhcp-enabled
        assertTrue("Value of subnet should be updated since the value for dhcp-enabled is an empty string",
                g.V().has("aai-node-type", "subnet").has("subnet-id", "subnet1").has("dhcp-enabled", false).hasNext());
        //l3-network: is-bound-to-vpn, is-shared-network, is-external-network
        assertTrue("Value of l3-network should be updated since the value for is-bound-to-vpn is an empty string",
				g.V().has("aai-node-type", "l3-network").has("network-id", "l3-network1").has("network-name", "l3-network-name1").has("is-bound-to-vpn", false).hasNext());         
        assertTrue("Value of l3-network should be updated since the value for is-provider-network is an empty string",
				g.V().has("aai-node-type", "l3-network").has("network-id", "l3-network1").has("network-name", "l3-network-name1").has("is-provider-network", false).hasNext());        
		assertTrue("Value of l3-network should be updated since the value for is-shared-network is an empty string",
				g.V().has("aai-node-type", "l3-network").has("network-id", "l3-network1").has("network-name", "l3-network-name1").has("is-shared-network", false).hasNext());
		assertTrue("Value of l3-network should be updated since the value for is-external-network is an empty string",
				g.V().has("aai-node-type", "l3-network").has("network-id", "l3-network1").has("network-name", "l3-network-name1").has("is-external-network", false).hasNext());
		//l-interface: is-port-mirrored, is-ip-unnumbered
		assertTrue("Value of l-interface should be updated since the property is-port-mirrored  is an empty string",
                g.V().has("aai-node-type", "l-interface").has("interface-name", "l-interface1").has("is-port-mirrored", false).hasNext());  
		assertTrue("Value of l-interface should be updated since the property is-ip-unnumbered  is an empty string",
                g.V().has("aai-node-type", "l-interface").has("interface-name", "l-interface1").has("is-ip-unnumbered", false).hasNext());
		//vf-module: is-base-vf-module, is-ip-unnumbered
		assertTrue("Value of vf-module should be updated since the property is-base-vf-module  is an empty string",
                g.V().has("aai-node-type", "vf-module").has("vf-module-id", "vf-module1").has("is-base-vf-module", false).hasNext());  
		//vlan: is-ip-unnumbered
		assertTrue("Value of vlan should be updated since the property is-ip-unnumbered is an empty string",
                g.V().has("aai-node-type", "vlan").has("vlan-interface", "vlan1").has("is-ip-unnumbered", false).hasNext());
    }
    
    @Test
    public void testExistingTrueValues() {
      //is-closed-loop-disabled
        assertTrue("Value of generic-vnf shouldn't be update since is-closed-loop-disabled already exists",
                g.V().has("aai-node-type", "generic-vnf").has("vnf-id", "generic-vnf2").has("is-closed-loop-disabled", true).hasNext());
        assertTrue("Value of vnfc shouldn't be update since is-closed-loop-disabled already exists",
                g.V().has("aai-node-type", "vnfc").has("vnfc-name", "vnfc2").has("is-closed-loop-disabled", true).hasNext());
        assertTrue("Value of vserver shouldn't be update since is-closed-loop-disabled already exists",
                g.V().has("aai-node-type", "vserver").has("vserver-id", "vserver2").has("is-closed-loop-disabled", true).hasNext());
       //dhcp-enabled
        assertTrue("Value of subnet shouldn't be update since dhcp-enabled already exists",
                g.V().has("aai-node-type", "subnet").has("subnet-id", "subnet2").has("dhcp-enabled", true).hasNext()); 
      //l3-network: is-bound-to-vpn, is-shared-network, is-external-network
        assertTrue("Value of l3-network shouldn't be updated since is-bound-to-vpn already exists",
				g.V().has("aai-node-type", "l3-network").has("network-id", "l3-network2").has("network-name", "l3-network-name2").has("is-bound-to-vpn", true).hasNext());
        assertTrue("Value of l3-network shouldn't be updated since is-provider-network already exists",
				g.V().has("aai-node-type", "l3-network").has("network-id", "l3-network2").has("network-name", "l3-network-name2").has("is-provider-network", true).hasNext());
		assertTrue("Value of l3-network shouldn't be updated since is-shared-network already exists",
				g.V().has("aai-node-type", "l3-network").has("network-id", "l3-network2").has("network-name", "l3-network-name2").has("is-shared-network", true).hasNext());
		assertTrue("Value of l3-network shouldn't be updated since is-external-network already exists",
				g.V().has("aai-node-type", "l3-network").has("network-id", "l3-network2").has("network-name", "l3-network-name2").has("is-external-network", true).hasNext());				
		//l-interface: is-port-mirrored, is-ip-unnumbered
		assertTrue("Value of l-interface shouldn't be updated since is-port-mirrored already exists",
                g.V().has("aai-node-type", "l-interface").has("interface-name", "l-interface2").has("is-port-mirrored", true).hasNext());  
		assertTrue("Value of ll-interface shouldn't be updated since is-ip-unnumbered already exists",
                g.V().has("aai-node-type", "l-interface").has("interface-name", "l-interface2").has("is-ip-unnumbered", true).hasNext());		
		//vf-module: is-base-vf-module
		assertTrue("Value of vf-module shouldn't be updated since is-base-vf-module already exists",
                g.V().has("aai-node-type", "vf-module").has("vf-module-id", "vf-module2").has("is-base-vf-module", true).hasNext());  
		//vlan: is-ip-unnumbered
		assertTrue("Value of vlan shouldn't be updated since is-ip-unnumbered already exists",
                g.V().has("aai-node-type", "vlan").has("vlan-interface", "vlan2").has("is-ip-unnumbered", true).hasNext());
        
    }
    
    @Test
    public void testExistingFalseValues() {
    	//is-closed-loop-disabled
        assertTrue("Value of generic-vnf shouldn't be update since is-closed-loop-disabled already exists",
                g.V().has("aai-node-type", "generic-vnf").has("vnf-id", "generic-vnf3").has("is-closed-loop-disabled", false).hasNext());
        assertTrue("Value of vnfc shouldn't be update since is-closed-loop-disabled already exists",
                g.V().has("aai-node-type", "vnfc").has("vnfc-name", "vnfc3").has("is-closed-loop-disabled", false).hasNext());
        assertTrue("Value of vserver shouldn't be update since is-closed-loop-disabled already exists",
                g.V().has("aai-node-type", "vserver").has("vserver-id", "vserver3").has("is-closed-loop-disabled", false).hasNext());
        //dhcp-enabled
        assertTrue("Value of subnet shouldn't be update since dhcp-enabled already exists",
                g.V().has("aai-node-type", "subnet").has("subnet-id", "subnet3").has("dhcp-enabled", false).hasNext());
        //l3-network: is-bound-to-vpn, is-shared-network, is-external-network
        assertTrue("Value of l3-network shouldn't be updated since is-bound-to-vpn already exists",
				g.V().has("aai-node-type", "l3-network").has("network-id", "l3-network3").has("network-name", "l3-network-name3").has("is-bound-to-vpn", false).hasNext());  
        assertTrue("Value of l3-network shouldn't be updated since is-provider-network already exists",
				g.V().has("aai-node-type", "l3-network").has("network-id", "l3-network3").has("network-name", "l3-network-name3").has("is-provider-network", false).hasNext());  
        assertTrue("Value of l3-network shouldn't be updated since is-shared-network already exists",
				g.V().has("aai-node-type", "l3-network").has("network-id", "l3-network3").has("network-name", "l3-network-name3").has("is-shared-network", false).hasNext());
		assertTrue("Value of l3-network shouldn't be updated since is-external-network already exists",
				g.V().has("aai-node-type", "l3-network").has("network-id", "l3-network3").has("network-name", "l3-network-name3").has("is-external-network", false).hasNext());			
		//l-interface: is-port-mirrored, is-ip-unnumbered
		assertTrue("Value of l-interface shouldn't be updated since is-port-mirrored already exists",
                g.V().has("aai-node-type", "l-interface").has("interface-name", "l-interface3").has("is-port-mirrored", false).hasNext());  
		assertTrue("Value of ll-interface shouldn't be updated since is-ip-unnumbered already exists",
                g.V().has("aai-node-type", "l-interface").has("interface-name", "l-interface3").has("is-ip-unnumbered", false).hasNext());				
		//vf-module: is-base-vf-module
		assertTrue("Value of vf-module shouldn't be updated since is-base-vf-module already exists",
                g.V().has("aai-node-type", "vf-module").has("vf-module-id", "vf-module3").has("is-base-vf-module", false).hasNext());  
		//vlan: is-ip-unnumbered
		assertTrue("Value of vlan shouldn't be updated since is-ip-unnumbered already exists",
                g.V().has("aai-node-type", "vlan").has("vlan-interface", "vlan3").has("is-ip-unnumbered", false).hasNext());
    } 
}