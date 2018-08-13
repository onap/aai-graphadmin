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
import org.apache.tinkerpop.gremlin.structure.Vertex;
import org.junit.After;
import org.junit.Before;
import org.junit.Test;
import org.onap.aai.AAISetup;
import org.onap.aai.dbmap.DBConnectionType;
import org.onap.aai.introspection.Loader;
import org.onap.aai.introspection.LoaderFactory;
import org.onap.aai.introspection.ModelType;
import org.onap.aai.setup.SchemaVersions;
import org.onap.aai.setup.SchemaVersion;
import org.onap.aai.serialization.engines.QueryStyle;
import org.onap.aai.serialization.engines.JanusGraphDBEngine;
import org.onap.aai.serialization.engines.TransactionalGraphEngine;

import org.janusgraph.core.JanusGraphFactory;
import org.janusgraph.core.JanusGraph;
import org.janusgraph.core.JanusGraphTransaction;


public class MigratePServerAndPnfEquipTypeTest extends AAISetup{

    private final static ModelType introspectorFactoryType = ModelType.MOXY;
    private final static QueryStyle queryStyle = QueryStyle.TRAVERSAL;
    private final static DBConnectionType type = DBConnectionType.REALTIME;
    private Loader loader;
    private TransactionalGraphEngine dbEngine;
    private JanusGraph graph;
    private MigratePserverAndPnfEquipType migration;
    private GraphTraversalSource g;
    private JanusGraphTransaction tx;
    Vertex pserver1;
    Vertex pserver2;
    Vertex pnf1;
    Vertex pserver3;
    Vertex pnf2;
    Vertex pnf22;


    @Before
    public void setUp() throws Exception {
        graph = JanusGraphFactory.build().set("storage.backend", "inmemory").open();
        tx = graph.newTransaction();
        g = tx.traversal();
        loader = loaderFactory.createLoaderForVersion(introspectorFactoryType, schemaVersions.getDefaultVersion());
        dbEngine = new JanusGraphDBEngine(
                queryStyle,
                type,
                loader);
         pserver1 = g.addV().property("aai-node-type", MigratePserverAndPnfEquipType.PSERVER_NODE_TYPE)
                .property( MigratePserverAndPnfEquipType.EQUIP_TYPE_PROPERTY, "Server")
                .next();
         
         pserver2 = g.addV().property("aai-node-type", MigratePserverAndPnfEquipType.PSERVER_NODE_TYPE)
                 .property( MigratePserverAndPnfEquipType.EQUIP_TYPE_PROPERTY, "server")
                 .next();
         
         pnf1 = g.addV().property("aai-node-type", MigratePserverAndPnfEquipType.PNF_NODE_TYPE)
                 .property( MigratePserverAndPnfEquipType.EQUIP_TYPE_PROPERTY, "Switch")
                 .next();
         pnf22 = g.addV().property("aai-node-type", MigratePserverAndPnfEquipType.PNF_NODE_TYPE)
                 .property( MigratePserverAndPnfEquipType.EQUIP_TYPE_PROPERTY, "switch")
                 .next();

         pserver3 = g.addV().property("aai-node-type", MigratePserverAndPnfEquipType.PSERVER_NODE_TYPE)
                 .property( MigratePserverAndPnfEquipType.EQUIP_TYPE_PROPERTY, "server1")
                 .next();
         
         pnf2 = g.addV().property("aai-node-type", MigratePserverAndPnfEquipType.PNF_NODE_TYPE)
                 .property( MigratePserverAndPnfEquipType.EQUIP_TYPE_PROPERTY, "Switch1")
                 .next();

        TransactionalGraphEngine spy = spy(dbEngine);
        TransactionalGraphEngine.Admin adminSpy = spy(dbEngine.asAdmin());
        GraphTraversalSource traversal = g;
        when(spy.asAdmin()).thenReturn(adminSpy);
        when(adminSpy.getTraversalSource()).thenReturn(traversal);
        migration = new MigratePserverAndPnfEquipType(spy, loaderFactory, edgeIngestor, edgeSerializer, schemaVersions);
        migration.run();
    }

    @After
    public void cleanUp() {
        tx.rollback();
        graph.close();
    }


    /***
     * checks if the Equip Type value was changed
     */

    @Test
    public void confirmEquipTypeChanged() {

        assertEquals("SERVER",pserver1.property(MigratePserverAndPnfEquipType.EQUIP_TYPE_PROPERTY).value());
        assertEquals("SERVER",pserver2.property(MigratePserverAndPnfEquipType.EQUIP_TYPE_PROPERTY).value());
        assertEquals("SWITCH",pnf1.property(MigratePserverAndPnfEquipType.EQUIP_TYPE_PROPERTY).value());
        assertEquals("SWITCH",pnf22.property(MigratePserverAndPnfEquipType.EQUIP_TYPE_PROPERTY).value());
    }
    
    @Test
    public void verifyEquipTypeIsNotChanged() {
    	assertEquals("server1",pserver3.property(MigratePserverAndPnfEquipType.EQUIP_TYPE_PROPERTY).value());
        assertEquals("Switch1",pnf2.property(MigratePserverAndPnfEquipType.EQUIP_TYPE_PROPERTY).value());
    }
    
    
    


}