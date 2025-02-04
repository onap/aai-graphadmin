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

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.mockito.Mockito.spy;
import static org.mockito.Mockito.when;

import org.apache.tinkerpop.gremlin.process.traversal.dsl.graph.GraphTraversalSource;
import org.apache.tinkerpop.gremlin.structure.Vertex;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

import org.onap.aai.AAISetup;
import org.onap.aai.introspection.Loader;
import org.onap.aai.introspection.ModelType;
import org.onap.aai.serialization.engines.QueryStyle;
import org.onap.aai.serialization.engines.JanusGraphDBEngine;
import org.onap.aai.serialization.engines.TransactionalGraphEngine;

import org.janusgraph.core.JanusGraphFactory;
import org.janusgraph.core.JanusGraph;
import org.janusgraph.core.JanusGraphTransaction;


public class MigrateCloudRegionUpgradeCycleTest extends AAISetup{

    private final static ModelType introspectorFactoryType = ModelType.MOXY;
    private final static QueryStyle queryStyle = QueryStyle.TRAVERSAL;
    private Loader loader;
    private TransactionalGraphEngine dbEngine;
    private JanusGraph graph;
    private MigrateCloudRegionUpgradeCycle migration;
    private GraphTraversalSource g;
    private JanusGraphTransaction tx;
    Vertex cloudRegion1;
    Vertex cloudRegion2;
    Vertex cloudRegion3;
   

    @BeforeEach
    public void setUp() throws Exception {
    	
    	graph = JanusGraphFactory.build().set("storage.backend", "inmemory").open();
        tx = graph.newTransaction();
        g = tx.traversal();
        loader = loaderFactory.createLoaderForVersion(introspectorFactoryType, schemaVersions.getDefaultVersion());
        dbEngine = new JanusGraphDBEngine(
                queryStyle,
                loader);
        
        System.setProperty("BUNDLECONFIG_DIR", "src/test/resources");
		
        cloudRegion1 = g.addV().property("aai-node-type", MigrateCloudRegionUpgradeCycle.CLOUD_REGION_NODE_TYPE)
        		.property(MigrateCloudRegionUpgradeCycle.CLOUD_REGION_ID, "akr1")
        		.property(MigrateCloudRegionUpgradeCycle.CLOUD_OWNER, "att-aic")
                .property( MigrateCloudRegionUpgradeCycle.UPGRADE_CYCLE, "Test")
                .next();
         
         cloudRegion2 = g.addV().property("aai-node-type", MigrateCloudRegionUpgradeCycle.CLOUD_REGION_NODE_TYPE)
        		 .property(MigrateCloudRegionUpgradeCycle.CLOUD_REGION_ID, "amsnl1b")
        		 .property(MigrateCloudRegionUpgradeCycle.CLOUD_OWNER, "att-aic")
                 //.property( MigrateCloudRegionUpgradeCycle.UPGRADE_CYCLE, "server")
                 .next();
         
         cloudRegion3 = g.addV().property("aai-node-type", MigrateCloudRegionUpgradeCycle.CLOUD_REGION_NODE_TYPE)
        		 .property(MigrateCloudRegionUpgradeCycle.CLOUD_REGION_ID, "alp1")
        		 .property(MigrateCloudRegionUpgradeCycle.CLOUD_OWNER, "Test")
                 .property( MigrateCloudRegionUpgradeCycle.UPGRADE_CYCLE, "server1")
                 .next();
        
        TransactionalGraphEngine spy = spy(dbEngine);
        TransactionalGraphEngine.Admin adminSpy = spy(dbEngine.asAdmin());
        GraphTraversalSource traversal = g;
        when(spy.asAdmin()).thenReturn(adminSpy);
        when(adminSpy.getTraversalSource()).thenReturn(traversal);
        migration = new MigrateCloudRegionUpgradeCycle(spy, loaderFactory, edgeIngestor, edgeSerializer, schemaVersions);
        migration.run();
    }

    @AfterEach
    public void cleanUp() {
        tx.rollback();
        graph.close();
    }


    /***
     * checks if the upgrade cycle value was changed
     */

    @Test
    public void confirmUpgradeCycleChanged() {

        assertEquals("E",cloudRegion1.property(MigrateCloudRegionUpgradeCycle.UPGRADE_CYCLE).value());
        assertEquals("B",cloudRegion2.property(MigrateCloudRegionUpgradeCycle.UPGRADE_CYCLE).value());
        assertEquals("server1",cloudRegion3.property(MigrateCloudRegionUpgradeCycle.UPGRADE_CYCLE).value());//Not changed
    }
}