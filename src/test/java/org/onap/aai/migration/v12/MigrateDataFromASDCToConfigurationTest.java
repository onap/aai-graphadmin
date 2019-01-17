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
import org.janusgraph.core.JanusGraphTransaction;
import org.apache.tinkerpop.gremlin.process.traversal.dsl.graph.GraphTraversalSource;
import org.apache.tinkerpop.gremlin.structure.Vertex;
import org.junit.After;
import org.junit.Before;
import org.junit.Test;
import org.onap.aai.AAISetup;
import org.onap.aai.dbmap.DBConnectionType;
import org.onap.aai.introspection.Loader;
import org.onap.aai.introspection.ModelType;
import org.onap.aai.setup.SchemaVersions;
import org.onap.aai.setup.SchemaVersion;
import org.onap.aai.serialization.engines.QueryStyle;
import org.onap.aai.serialization.engines.JanusGraphDBEngine;
import org.onap.aai.serialization.engines.TransactionalGraphEngine;

import static org.junit.Assert.assertEquals;
import static org.mockito.Mockito.spy;
import static org.mockito.Mockito.when;

public class MigrateDataFromASDCToConfigurationTest extends AAISetup {

    private final static ModelType introspectorFactoryType = ModelType.MOXY;
    private final static QueryStyle queryStyle = QueryStyle.TRAVERSAL;
    private final static DBConnectionType type = DBConnectionType.REALTIME;

    private Loader loader;
    private TransactionalGraphEngine dbEngine;
    private JanusGraph graph;
    private MigrateDataFromASDCToConfiguration migration;
    private GraphTraversalSource g;
    private JanusGraphTransaction tx;

    Vertex configuration;
    Vertex configuration2;
    Vertex configuration3;
    Vertex configuration4;
    Vertex configuration5;

    private boolean success = true;
    private  String entitlementPoolUuid = "";
    private final String PARENT_NODE_TYPE = "generic-vnf";
    private String VNT = "";

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
        
        System.setProperty("BUNDLECONFIG_DIR", "src/test/resources");
        Vertex genericvnf1 = g.addV().property("aai-node-type", "generic-vnf")
                .property("vnf-id", "vnfId1")
                .property("vnf-type","HN")
                .next();

        Vertex genericvnf2 = g.addV().property("aai-node-type", "generic-vnf")
                .property("vnf-id", "vnfId2")
                .property("vnf-type","HN")
                .next();

        Vertex genericvnf_wrongtype = g.addV().property("aai-node-type", "generic-vnf")
                .property("vnf-id", "vnfIdWrong")
                .property("vnf-type","vHNF")
                .next();

        Vertex entitlement1 = g.addV().property("aai-node-type", "entitlement")
                .property("group-uuid", "599a2d74-cfbd-413d-aedb-ec4875817313")
                .next();

        Vertex entitlement2 = g.addV().property("aai-node-type", "entitlement")
                .property("group-uuid", "ea9a547e-137b-48e9-a788-c3fb4e631a2a")
                .next();

        Vertex serviceInstance1 = g.addV().property("aai-node-type", "service-instance")
                .property("service-instance-id", "servinstanceTestId1")
                .next();

        Vertex serviceInstance2 = g.addV().property("aai-node-type", "service-instance")
                .property("service-instance-id", "servinstanceTestId2")
                .next();

        configuration =  g.addV().property("aai-node-type", "configuration")
                .property("configuration-id", "configurationIdGraph")
                .property("vendor-allowed-max-bandwidth", "20")
                .next();
        configuration3 =  g.addV().property("aai-node-type", "configuration")
                .property("configuration-id", "configurationIdGraph3")
                .property("vendor-allowed-max-bandwidth", "15")
                .next();
        configuration2 =  g.addV().property("aai-node-type", "configuration")
                .property("configuration-id", "configurationIdGraph2")
                .property("vendor-allowed-max-bandwidth", "25")
                .next();
        configuration4 =  g.addV().property("aai-node-type", "configuration")
                .property("configuration-id", "configurationIdGraph4")
                .property("vendor-allowed-max-bandwidth", "50")
                .next();
        configuration5 =  g.addV().property("aai-node-type", "configuration")
                .property("configuration-id", "configurationIdGraph4")
                .property("vendor-allowed-max-bandwidth", "75")
                .next();

        edgeSerializer.addTreeEdge(g, genericvnf1, entitlement1);
        edgeSerializer.addEdge(g, genericvnf1, serviceInstance1);
        edgeSerializer.addEdge(g, serviceInstance1, configuration);
        edgeSerializer.addEdge(g, serviceInstance1, configuration3);


        edgeSerializer.addEdge(g, genericvnf2, configuration2, "org.onap.relationships.inventory.Uses");

        edgeSerializer.addTreeEdge(g, genericvnf_wrongtype, entitlement2);
        edgeSerializer.addEdge(g, genericvnf_wrongtype, serviceInstance2);
        edgeSerializer.addEdge(g, serviceInstance2, configuration5);

        TransactionalGraphEngine spy = spy(dbEngine);
        TransactionalGraphEngine.Admin adminSpy = spy(dbEngine.asAdmin());
        GraphTraversalSource traversal = g;
        when(spy.asAdmin()).thenReturn(adminSpy);
        when(adminSpy.getTraversalSource()).thenReturn(traversal);
        migration = new MigrateDataFromASDCToConfiguration(spy, loaderFactory, edgeIngestor, edgeSerializer, schemaVersions);
        migration.run();
    }


    @After
    public void cleanUp() {
        tx.rollback();
        graph.close();
    }


    /***
     * checks if the VNt value was updated and if theres a second configuration object it is also to be modified
     */

    @Test
    public void confirmVNtValueChanged() {

        assertEquals("1000",configuration.property("vendor-allowed-max-bandwidth").value());
        assertEquals("1000",configuration3.property("vendor-allowed-max-bandwidth").value());

    }

    /***
     * checks to see if the entitlement object is missing the configuration objects should not be modified at all
     */
    @Test
    public void missingEntitlementObject() {
        assertEquals("25",configuration2.property("vendor-allowed-max-bandwidth").value());
    }
    /***
     * checks to see if there's a configuration object not connected to anything it should not be modified at all
     */

    @Test
    public void confirmConfiguration4notchanged() {
        assertEquals("50",configuration4.property("vendor-allowed-max-bandwidth").value());
    }
    /***
     * checks that a configuration object not linked to a "HN" vnf-type should not be changed
     */
    @Test
    public void differentVNFType() {
        assertEquals("75",configuration5.property("vendor-allowed-max-bandwidth").value());
    }




}
