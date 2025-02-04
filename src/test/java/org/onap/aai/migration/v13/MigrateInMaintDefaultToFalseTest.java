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

import org.onap.aai.edges.EdgeIngestor;
import org.onap.aai.serialization.db.EdgeSerializer;
import org.onap.aai.serialization.engines.TransactionalGraphEngine;

import org.janusgraph.core.JanusGraph;
import org.janusgraph.core.JanusGraphFactory;
import org.janusgraph.core.JanusGraphTransaction;
import org.janusgraph.core.schema.JanusGraphManagement;
import org.apache.tinkerpop.gremlin.process.traversal.dsl.graph.GraphTraversalSource;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

import org.onap.aai.AAISetup;
import org.onap.aai.dbmap.DBConnectionType;
import org.onap.aai.introspection.Loader;
import org.onap.aai.introspection.LoaderFactory;
import org.onap.aai.introspection.ModelType;
import org.onap.aai.setup.SchemaVersions;
import org.onap.aai.migration.Status;
import org.onap.aai.serialization.engines.QueryStyle;
import org.onap.aai.serialization.engines.JanusGraphDBEngine;

import static org.junit.jupiter.api.Assertions.assertTrue;
import static org.mockito.Mockito.spy;
import static org.mockito.Mockito.when;
import java.util.Optional;

public class MigrateInMaintDefaultToFalseTest extends
		AAISetup {
	protected static final String VNF_NODE_TYPE = "generic-vnf";
	protected static final String LINTERFACE_NODE_TYPE = "l-interface";
	protected static final String LAG_INTERFACE_NODE_TYPE = "lag-interface";
	protected static final String LOGICAL_LINK_NODE_TYPE = "logical-link";
	protected static final String PINTERFACE_NODE_TYPE = "p-interface";
	protected static final String VLAN_NODE_TYPE = "vlan";
	protected static final String VNFC_NODE_TYPE = "vnfc";
	protected static final String VSERVER_NODE_TYPE = "vserver";
	protected static final String PSERVER_NODE_TYPE = "pserver";
	protected static final String PNF_NODE_TYPE = "pnf";
	protected static final String NOS_SERVER_NODE_TYPE = "nos-server";

	public static class InMaintDefaultMigrator extends MigrateInMaintDefaultToFalse {
        public InMaintDefaultMigrator(TransactionalGraphEngine engine, LoaderFactory loaderFactory, EdgeIngestor edgeIngestor, EdgeSerializer edgeSerializer, SchemaVersions schemaVersions){
            super(engine, loaderFactory, edgeIngestor, edgeSerializer, schemaVersions);
        }
        @Override
        public Status getStatus() {
            return Status.SUCCESS;
        }
        @Override
        public Optional<String[]> getAffectedNodeTypes() {
        	return Optional.of(new String[]{VNF_NODE_TYPE,LINTERFACE_NODE_TYPE,LAG_INTERFACE_NODE_TYPE,LOGICAL_LINK_NODE_TYPE,PINTERFACE_NODE_TYPE,VLAN_NODE_TYPE,VNFC_NODE_TYPE,VSERVER_NODE_TYPE,PSERVER_NODE_TYPE,PNF_NODE_TYPE,NOS_SERVER_NODE_TYPE});
        }
        @Override
        public String getMigrationName() {
            return "MockInMaintDefaultMigrator";
        }
    }

    private final static ModelType introspectorFactoryType = ModelType.MOXY;
    private final static QueryStyle queryStyle = QueryStyle.TRAVERSAL;
    private final static DBConnectionType type = DBConnectionType.REALTIME;
    private Loader loader;
    private TransactionalGraphEngine dbEngine;
    private InMaintDefaultMigrator migration;
    private GraphTraversalSource g;

    @BeforeEach
    public void setup() throws Exception{
        g = tx.traversal();
        loader = loaderFactory.createLoaderForVersion(introspectorFactoryType, schemaVersions.getDefaultVersion());
        dbEngine = new JanusGraphDBEngine(
                queryStyle,
                loader);

        //generic-vnf
        g.addV().property("aai-node-type", "generic-vnf")
                .property("vnf-id", "generic-vnf0")
                .next();
        g.addV().property("aai-node-type", "generic-vnf")
                .property("vnf-id", "generic-vnf1")
                .property("in-maint", "")
                .next();
        g.addV().property("aai-node-type", "generic-vnf")
                .property("vnf-id", "generic-vnf2")
                .property("in-maint", true)
                .next();
        g.addV().property("aai-node-type", "generic-vnf")
        		.property("vnf-id", "generic-vnf3")
        		.property("in-maint", false)
        		.next();
      //l-interface
        g.addV().property("aai-node-type", "l-interface")
                .property("interface-name", "l-interface0")
                .next();
        g.addV().property("aai-node-type", "l-interface")
                .property("interface-name", "l-interface1")
                .property("in-maint", "")
                .next();
        g.addV().property("aai-node-type", "l-interface")
                .property("interface-name", "l-interface2")
                .property("in-maint", true)
                .next();
        g.addV().property("aai-node-type", "l-interface")
        		.property("interface-name", "l-interface3")
        		.property("in-maint", false)
        		.next();
      //lag-interface
        g.addV().property("aai-node-type", "lag-interface")
                .property("interface-name", "lag-interface0")
                .next();
        g.addV().property("aai-node-type", "lag-interface")
                .property("interface-name", "lag-interface1")
                .property("in-maint", "")
                .next();
        g.addV().property("aai-node-type", "lag-interface")
                .property("interface-name", "lag-interface2")
                .property("in-maint", true)
                .next();
        g.addV().property("aai-node-type", "lag-interface")
        		.property("interface-name", "lag-interface3")
        		.property("in-maint", false)
        		.next();
      //logical-link
        g.addV().property("aai-node-type", "logical-link")
                .property("link-name", "logical-link0")
                .next();
        g.addV().property("aai-node-type", "logical-link")
                .property("link-name", "logical-link1")
                .property("in-maint", "")
                .next();
        g.addV().property("aai-node-type", "logical-link")
                .property("link-name", "logical-link2")
                .property("in-maint", true)
                .next();
        g.addV().property("aai-node-type", "logical-link")
        		.property("link-name", "logical-link3")
        		.property("in-maint", false)
        		.next();
      //p-interface
        g.addV().property("aai-node-type", "p-interface")
                .property("interface-name", "p-interface0")
                .next();
        g.addV().property("aai-node-type", "p-interface")
                .property("interface-name", "p-interface1")
                .property("in-maint", "")
                .next();
        g.addV().property("aai-node-type", "p-interface")
                .property("interface-name", "p-interface2")
                .property("in-maint", true)
                .next();
        g.addV().property("aai-node-type", "p-interface")
        		.property("interface-name", "p-interface3")
        		.property("in-maint", false)
        		.next();
      //pnf
        g.addV().property("aai-node-type", "pnf")
                .property("pnf-name", "pnf0")
                .next();
        g.addV().property("aai-node-type", "pnf")
                .property("pnf-name", "pnf1")
                .property("in-maint", "")
                .next();
        g.addV().property("aai-node-type", "pnf")
                .property("pnf-name", "pnf2")
                .property("in-maint", true)
                .next();
        g.addV().property("aai-node-type", "pnf")
        		.property("pnf-name", "pnf3")
        		.property("in-maint", false)
        		.next();
      //pserver
        g.addV().property("aai-node-type", "pserver")
                .property("pserver-id", "pserver0")
                .next();
        g.addV().property("aai-node-type", "pserver")
                .property("pserver-id", "pserver1")
                .property("in-maint", "")
                .next();
        g.addV().property("aai-node-type", "pserver")
                .property("pserver-id", "pserver2")
                .property("in-maint", true)
                .next();
        g.addV().property("aai-node-type", "pserver")
        		.property("pserver-id", "pserver3")
        		.property("in-maint", false)
        		.next();
      //vlan
        g.addV().property("aai-node-type", "vlan")
                .property("vlan-interface", "vlan0")
                .next();
        g.addV().property("aai-node-type", "vlan")
                .property("vlan-interface", "vlan1")
                .property("in-maint", "")
                .next();
        g.addV().property("aai-node-type", "vlan")
                .property("vlan-interface", "vlan2")
                .property("in-maint", true)
                .next();
        g.addV().property("aai-node-type", "vlan")
        		.property("vlan-interface", "vlan3")
        		.property("in-maint", false)
        		.next();
      //vnfc
        g.addV().property("aai-node-type", "vnfc")
                .property("vnfc-name", "vnfc0")
                .next();
        g.addV().property("aai-node-type", "vnfc")
                .property("vnfc-name", "vnfc1")
                .property("in-maint", "")
                .next();
        g.addV().property("aai-node-type", "vnfc")
                .property("vnfc-name", "vnfc2")
                .property("in-maint", true)
                .next();
        g.addV().property("aai-node-type", "vnfc")
        		.property("vnfc-name", "vnfc3")
        		.property("in-maint", false)
        		.next();
      //vserver
        g.addV().property("aai-node-type", "vserver")
                .property("vserver-id", "vserver0")
                .next();
        g.addV().property("aai-node-type", "vserver")
                .property("vserver-id", "vserver1")
                .property("in-maint", "")
                .next();
        g.addV().property("aai-node-type", "vserver")
                .property("vserver-id", "vserver2")
                .property("in-maint", true)
                .property("is-closed-loop-disabled", true)
                .next();
        g.addV().property("aai-node-type", "vserver")
        		.property("vserver-id", "vserver3")
        		.property("in-maint", false)
        		.next();
      //nos-server
        g.addV().property("aai-node-type", "nos-server")
                .property("nos-server-id", "nos-server0")
				.property("nos-server-name", "nos-server-name0")
				.property("vendor", "vendor0")
				.property("nos-server-selflink", "nos-server-selflink0")
                .next();
        g.addV().property("aai-node-type", "nos-server")
                .property("nos-server-id", "nos-server1")
				.property("nos-server-name", "nos-server-name1")
				.property("vendor", "vendor1")
				.property("nos-server-selflink", "nos-server-selflink1")
                .property("in-maint", "")
                .next();
        g.addV().property("aai-node-type", "nos-server")
                .property("nos-server-id", "nos-server2")
				.property("nos-server-name", "nos-server-name2")
				.property("vendor", "vendor2")
				.property("nos-server-selflink", "nos-server-selflink2")
                .property("in-maint", true)
                .next();
        g.addV().property("aai-node-type", "nos-server")
        		.property("nos-server-id", "nos-server3")
				.property("nos-server-name", "nos-server-name3")
				.property("vendor", "vendor3")
				.property("nos-server-selflink", "nos-server-selflink3")
        		.property("in-maint", false)
        		.next();

        TransactionalGraphEngine spy = spy(dbEngine);
        TransactionalGraphEngine.Admin adminSpy = spy(dbEngine.asAdmin());
        GraphTraversalSource traversal = g;
        when(spy.asAdmin()).thenReturn(adminSpy);
        when(adminSpy.getTraversalSource()).thenReturn(traversal);
        migration = new InMaintDefaultMigrator(spy, loaderFactory, edgeIngestor, edgeSerializer, schemaVersions);
        migration.run();
    }

    @Test
    public void testMissingProperty(){
        assertTrue(g.V().has("aai-node-type", "generic-vnf").has("vnf-id", "generic-vnf0").has("in-maint", false).hasNext(),
                "Value of generic-vnf should be updated since the property in-maint doesn't exist");
        assertTrue(g.V().has("aai-node-type", "l-interface").has("interface-name", "l-interface0").has("in-maint", false).hasNext(),
                "Value of l-interface should be updated since the property in-maint doesn't exist");
        assertTrue(g.V().has("aai-node-type", "lag-interface").has("interface-name", "lag-interface0").has("in-maint", false).hasNext(),
                "Value of lag-interface should be updated since the property in-maint doesn't exist");
        assertTrue(g.V().has("aai-node-type", "logical-link").has("link-name", "logical-link0").has("in-maint", false).hasNext(),
                "Value of logical-link should be updated since the property in-maint doesn't exist");
        assertTrue(g.V().has("aai-node-type", "p-interface").has("interface-name", "p-interface0").has("in-maint", false).hasNext(),
                "Value of p-interface should be updated since the property in-maint doesn't exist");
        assertTrue(g.V().has("aai-node-type", "pnf").has("pnf-name", "pnf0").has("in-maint", false).hasNext(),
                "Value of pnf should be updated since the property in-maint doesn't exist");
        assertTrue(g.V().has("aai-node-type", "pserver").has("pserver-id", "pserver0").has("in-maint", false).hasNext(),
                "Value of pserver should be updated since the property in-maint doesn't exist");
        assertTrue(g.V().has("aai-node-type", "vlan").has("vlan-interface", "vlan0").has("in-maint", false).hasNext(),
                "Value of vlan should be updated since the property in-maint doesn't exist");
        assertTrue(g.V().has("aai-node-type", "vnfc").has("vnfc-name", "vnfc0").has("in-maint", false).hasNext(),
                "Value of vnfc should be updated since the property in-maint doesn't exist");
        assertTrue(g.V().has("aai-node-type", "vserver").has("vserver-id", "vserver0").has("in-maint", false).hasNext(),
                "Value of vserver should be updated since the property in-maint doesn't exist");
        assertTrue(g.V().has("aai-node-type", "nos-server").has("nos-server-id", "nos-server0").has("in-maint", false).hasNext(),
                "Value of nos-server should be updated since the property in-maint doesn't exist");
    }

    @Test
    public void testEmptyValue() {
        assertTrue(g.V().has("aai-node-type", "generic-vnf").has("vnf-id", "generic-vnf1").has("in-maint", false).hasNext(),
                "Value of generic-vnf should be updated since the value for in-maint is an empty string");
        assertTrue(g.V().has("aai-node-type", "l-interface").has("interface-name", "l-interface1").has("in-maint", false).hasNext(),
                "Value of l-interface should be updated since the value for in-maint is an empty string");
        assertTrue(g.V().has("aai-node-type", "lag-interface").has("interface-name", "lag-interface1").has("in-maint", false).hasNext(),
                "Value of lag-interface should be updated since the value for in-maint is an empty string");
        assertTrue(g.V().has("aai-node-type", "logical-link").has("link-name", "logical-link1").has("in-maint", false).hasNext(),
                "Value of logical-link should be updated since the value for in-maint is an empty string");
        assertTrue(g.V().has("aai-node-type", "p-interface").has("interface-name", "p-interface1").has("in-maint", false).hasNext(),
                "Value of p-interface should be updated since the value for in-maint is an empty string");
        assertTrue(g.V().has("aai-node-type", "pnf").has("pnf-name", "pnf1").has("in-maint", false).hasNext(),
                "Value of pnf should be updated since the value for in-maint is an empty string");
        assertTrue(g.V().has("aai-node-type", "pserver").has("pserver-id", "pserver1").has("in-maint", false).hasNext(),
                "Value of pserver should be updated since the value for in-maint is an empty string");
        assertTrue(g.V().has("aai-node-type", "vlan").has("vlan-interface", "vlan1").has("in-maint", false).hasNext(),
                "Value of vlan should be updated since the value for in-maint is an empty string");
        assertTrue(g.V().has("aai-node-type", "vnfc").has("vnfc-name", "vnfc1").has("in-maint", false).hasNext(),
                "Value of vnfc should be updated since the value for in-maint is an empty string");
        assertTrue(g.V().has("aai-node-type", "vserver").has("vserver-id", "vserver1").has("in-maint", false).hasNext(),
                "Value of vserver should be updated since the value for in-maint is an empty string");
        assertTrue(g.V().has("aai-node-type", "nos-server").has("nos-server-id", "nos-server1").has("in-maint", false).hasNext(),
                "Value of nos-server should be updated since the value for in-maint is an empty string");
    }

    @Test
    public void testExistingTrueValues() {
        assertTrue(g.V().has("aai-node-type", "generic-vnf").has("vnf-id", "generic-vnf2").has("in-maint", true).hasNext(),
                "Value of generic-vnf shouldn't be updated since in-maint already exists");
        assertTrue(g.V().has("aai-node-type", "l-interface").has("interface-name", "l-interface2").has("in-maint", true).hasNext(),
                "Value of l-interface shouldn't be updated since in-maint already exists");
        assertTrue(g.V().has("aai-node-type", "lag-interface").has("interface-name", "lag-interface2").has("in-maint", true).hasNext(),
                "Value of lag-interface shouldn't be updated since in-maint already exists");
        assertTrue(g.V().has("aai-node-type", "logical-link").has("link-name", "logical-link2").has("in-maint", true).hasNext(),
                "Value of logical-link shouldn't be updated since in-maint already exists");
        assertTrue(g.V().has("aai-node-type", "p-interface").has("interface-name", "p-interface2").has("in-maint", true).hasNext(),
                "Value of p-interface shouldn't be updated since in-maint already exists");
        assertTrue(g.V().has("aai-node-type", "pnf").has("pnf-name", "pnf2").has("in-maint", true).hasNext(),
                "Value of pnf shouldn't be updated since in-maint already exists");
        assertTrue(g.V().has("aai-node-type", "pserver").has("pserver-id", "pserver2").has("in-maint", true).hasNext(),
                "Value of pserver shouldn't be updated since in-maint already exists");
        assertTrue(g.V().has("aai-node-type", "vlan").has("vlan-interface", "vlan2").has("in-maint", true).hasNext(),
                "Value of vlan shouldn't be updated since in-maint already exists");
        assertTrue(g.V().has("aai-node-type", "vnfc").has("vnfc-name", "vnfc2").has("in-maint", true).hasNext(),
                "Value of vnfc shouldn't be updated since in-maint already exists");
        assertTrue(g.V().has("aai-node-type", "vserver").has("vserver-id", "vserver2").has("in-maint", true).hasNext(),
                "Value of vserver shouldn't be updated since in-maint already exists");
        assertTrue(g.V().has("aai-node-type", "nos-server").has("nos-server-id", "nos-server2").has("in-maint", true).hasNext(),
                "Value of nos-server shouldn't be updated since in-maint already exists");
    }

    @Test
    public void testExistingFalseValues() {
        assertTrue(g.V().has("aai-node-type", "generic-vnf").has("vnf-id", "generic-vnf3").has("in-maint", false).hasNext(),
                "Value of generic-vnf shouldn't be updated since in-maint already exists");
        assertTrue(g.V().has("aai-node-type", "l-interface").has("interface-name", "l-interface3").has("in-maint", false).hasNext(),
                "Value of l-interface shouldn't be updated since in-maint already exists");
        assertTrue(g.V().has("aai-node-type", "lag-interface").has("interface-name", "lag-interface3").has("in-maint", false).hasNext(),
                "Value of lag-interface shouldn't be updated since in-maint already exists");
        assertTrue(g.V().has("aai-node-type", "logical-link").has("link-name", "logical-link3").has("in-maint", false).hasNext(),
                "Value of logical-link shouldn't be updated since in-maint already exists");
        assertTrue(g.V().has("aai-node-type", "p-interface").has("interface-name", "p-interface3").has("in-maint", false).hasNext(),
                "Value of p-interface shouldn't be updated since in-maint already exists");
        assertTrue(g.V().has("aai-node-type", "pnf").has("pnf-name", "pnf3").has("in-maint", false).hasNext(),
                "Value of pnf shouldn't be updated since in-maint already exists");
        assertTrue(g.V().has("aai-node-type", "pserver").has("pserver-id", "pserver3").has("in-maint", false).hasNext(),
                "Value of pserver shouldn't be updated since in-maint already exists");
        assertTrue(g.V().has("aai-node-type", "vlan").has("vlan-interface", "vlan3").has("in-maint", false).hasNext(),
                "Value of vlan shouldn't be updated since in-maint already exists");
        assertTrue(g.V().has("aai-node-type", "vnfc").has("vnfc-name", "vnfc3").has("in-maint", false).hasNext(),
                "Value of vnfc shouldn't be updated since in-maint already exists");
        assertTrue(g.V().has("aai-node-type", "vserver").has("vserver-id", "vserver3").has("in-maint", false).hasNext(),
                "Value of vserver shouldn't be updated since in-maint already exists");
        assertTrue(g.V().has("aai-node-type", "nos-server").has("nos-server-id", "nos-server3").has("in-maint", false).hasNext(),
                "Value of nos-server shouldn't be updated since in-maint already exists");
    }
}
