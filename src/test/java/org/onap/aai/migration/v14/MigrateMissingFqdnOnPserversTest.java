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

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.mockito.Mockito.spy;
import static org.mockito.Mockito.when;

import org.apache.tinkerpop.gremlin.process.traversal.dsl.graph.GraphTraversalSource;
import org.apache.tinkerpop.gremlin.structure.Vertex;
import org.janusgraph.core.JanusGraph;
import org.janusgraph.core.JanusGraphFactory;
import org.janusgraph.core.JanusGraphTransaction;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

import org.onap.aai.AAISetup;
import org.onap.aai.introspection.Loader;
import org.onap.aai.introspection.ModelType;
import org.onap.aai.serialization.engines.JanusGraphDBEngine;
import org.onap.aai.serialization.engines.QueryStyle;
import org.onap.aai.serialization.engines.TransactionalGraphEngine;


public class MigrateMissingFqdnOnPserversTest extends AAISetup{

    private final static ModelType introspectorFactoryType = ModelType.MOXY;
    private final static QueryStyle queryStyle = QueryStyle.TRAVERSAL;
    private Loader loader;
    private TransactionalGraphEngine dbEngine;
    private JanusGraph graph;
    private MigrateMissingFqdnOnPservers migration;
    private GraphTraversalSource g;
    private JanusGraphTransaction tx;
    Vertex pserver1;
    Vertex pserver2;
    Vertex pserver3;
    Vertex pserver4;
    Vertex pserver5;
    

    @BeforeEach
    public void setUp() throws Exception {
    	
    	graph = JanusGraphFactory.build().set("storage.backend", "inmemory").open();
        tx = graph.newTransaction();
        g = tx.traversal();
        loader = loaderFactory.createLoaderForVersion(introspectorFactoryType, schemaVersions.getDefaultVersion());
        dbEngine = new JanusGraphDBEngine(
                queryStyle,
                loader);
        
        
        pserver1 = g.addV().property("aai-node-type", MigrateMissingFqdnOnPservers.PSERVER_NODE_TYPE)
        		.property(MigrateMissingFqdnOnPservers.PSERVER_HOSTNAME, "hostname1.com")
        		.property(MigrateMissingFqdnOnPservers.PSERVER_SOURCEOFTRUTH, "RO")
                .next();
         
        pserver2 = g.addV().property("aai-node-type", MigrateMissingFqdnOnPservers.PSERVER_NODE_TYPE)
        		.property(MigrateMissingFqdnOnPservers.PSERVER_HOSTNAME, "hostname2.com")
        		.property(MigrateMissingFqdnOnPservers.PSERVER_FQDN, "")
                .next();
         
        pserver3 = g.addV().property("aai-node-type", MigrateMissingFqdnOnPservers.PSERVER_NODE_TYPE)
        		.property(MigrateMissingFqdnOnPservers.PSERVER_HOSTNAME, "akr1")
        		.property(MigrateMissingFqdnOnPservers.PSERVER_SOURCEOFTRUTH, "RO")
        		.property(MigrateMissingFqdnOnPservers.PSERVER_FQDN, "akr1")
                .next();
        
        pserver4 = g.addV().property("aai-node-type", MigrateMissingFqdnOnPservers.PSERVER_NODE_TYPE)
        		.property(MigrateMissingFqdnOnPservers.PSERVER_HOSTNAME, "hostname1")
        		.property(MigrateMissingFqdnOnPservers.PSERVER_SOURCEOFTRUTH, "RO")
        		.property(MigrateMissingFqdnOnPservers.PSERVER_FQDN, "")
                .next();
        
        pserver5 = g.addV().property("aai-node-type", MigrateMissingFqdnOnPservers.PSERVER_NODE_TYPE)
        		.property(MigrateMissingFqdnOnPservers.PSERVER_HOSTNAME, "hostname2")
        		.property(MigrateMissingFqdnOnPservers.PSERVER_SOURCEOFTRUTH, "RO")
        		.next();
        
        TransactionalGraphEngine spy = spy(dbEngine);
        TransactionalGraphEngine.Admin adminSpy = spy(dbEngine.asAdmin());
        GraphTraversalSource traversal = g;
        when(spy.asAdmin()).thenReturn(adminSpy);
        when(adminSpy.getTraversalSource()).thenReturn(traversal);
        migration = new MigrateMissingFqdnOnPservers(spy, loaderFactory, edgeIngestor, edgeSerializer, schemaVersions);
        migration.run();
    }

    @AfterEach
    public void cleanUp() {
        tx.rollback();
        graph.close();
    }


    /***
     * checks if the fqdn value was changed
     */

    @Test
    public void confirmFQDNValueChanged() {

        assertEquals("hostname1.com",pserver1.property(MigrateMissingFqdnOnPservers.PSERVER_FQDN).value());//created fqdn property
        assertEquals("hostname2.com",pserver2.property(MigrateMissingFqdnOnPservers.PSERVER_FQDN).value());//updated empty fqdn
        assertEquals("akr1",pserver3.property(MigrateMissingFqdnOnPservers.PSERVER_FQDN).value());//Not changed
        assertEquals("",pserver4.property(MigrateMissingFqdnOnPservers.PSERVER_FQDN).value());//Not changed
    }
}