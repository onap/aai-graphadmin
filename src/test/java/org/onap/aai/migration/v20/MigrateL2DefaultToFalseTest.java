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
package org.onap.aai.migration.v20;

import org.apache.tinkerpop.gremlin.process.traversal.dsl.graph.GraphTraversalSource;
import org.junit.Before;
import org.junit.Test;
import org.onap.aai.AAISetup;
import org.onap.aai.edges.EdgeIngestor;
import org.onap.aai.introspection.Loader;
import org.onap.aai.introspection.LoaderFactory;
import org.onap.aai.introspection.ModelType;
import org.onap.aai.serialization.db.EdgeSerializer;
import org.onap.aai.serialization.engines.JanusGraphDBEngine;
import org.onap.aai.serialization.engines.QueryStyle;
import org.onap.aai.serialization.engines.TransactionalGraphEngine;
import org.onap.aai.setup.SchemaVersions;

import java.util.Arrays;

import static org.junit.Assert.assertTrue;
import static org.mockito.Mockito.spy;
import static org.mockito.Mockito.when;

public class MigrateL2DefaultToFalseTest extends
		AAISetup {
	
	protected static final String L_INTERFACE_NODE_TYPE = "l-interface";
	protected static final String L2_MULTI_PROPERTY = "l2-multicasting";

	public static class L2DefaultMigrator extends MigrateL2DefaultToFalse {
        public L2DefaultMigrator(TransactionalGraphEngine engine, LoaderFactory loaderFactory, EdgeIngestor edgeIngestor, EdgeSerializer edgeSerializer, SchemaVersions schemaVersions){
            super(engine, loaderFactory, edgeIngestor, edgeSerializer, schemaVersions);
        }
    }

    private final static ModelType introspectorFactoryType = ModelType.MOXY;
    private final static QueryStyle queryStyle = QueryStyle.TRAVERSAL;
    private Loader loader;
    private TransactionalGraphEngine dbEngine;
    private L2DefaultMigrator migration;
    private GraphTraversalSource g;

    @Before
    public void setup() throws Exception{
        g = tx.traversal();
        loader = loaderFactory.createLoaderForVersion(introspectorFactoryType, schemaVersions.getDefaultVersion());
        dbEngine = new JanusGraphDBEngine(
                queryStyle,
                loader);

        g.addV().property("aai-node-type", L_INTERFACE_NODE_TYPE)
                .property("interface-name", "no-value")
                .property("interface-id", "1")
                .next();
        g.addV().property("aai-node-type", L_INTERFACE_NODE_TYPE)
        		.property("interface-name", "empty-value")
        		.property("interface-id", "2")
        		.property(L2_MULTI_PROPERTY, "")
        		.next();
        g.addV().property("aai-node-type", L_INTERFACE_NODE_TYPE)
        		.property("interface-name", "true-value")
        		.property("interface-id", "3")
        		.property(L2_MULTI_PROPERTY, "true")        		
        		.next();
        g.addV().property("aai-node-type", L_INTERFACE_NODE_TYPE)
        		.property("interface-name", "false-value")
        		.property("interface-id", "4")
        		.property(L2_MULTI_PROPERTY, "false")          		
        		.next();
        g.addV().property("aai-node-type", L_INTERFACE_NODE_TYPE)
        		.property("interface-name", "extra")
        		.property("interface-id", "5")
        		.next();      
        
        TransactionalGraphEngine spy = spy(dbEngine);
        TransactionalGraphEngine.Admin adminSpy = spy(dbEngine.asAdmin());
        GraphTraversalSource traversal = g;
        when(spy.asAdmin()).thenReturn(adminSpy);
        when(adminSpy.getTraversalSource()).thenReturn(traversal);
        migration = new L2DefaultMigrator(spy, loaderFactory, edgeIngestor, edgeSerializer, schemaVersions);
        migration.run();
    }
    
    @Test
    public void testAllValuesSet() {
    	assertTrue("Value of node 1 \"no-value\" should have " + L2_MULTI_PROPERTY + " of false ",
    			g.V().has("aai-node-type", L_INTERFACE_NODE_TYPE).has("interface-name", "no-value")
    			.has(L2_MULTI_PROPERTY,false).hasNext());
    	
    	assertTrue("Value of node 2 \"empty-value\" should have " + L2_MULTI_PROPERTY + " of false ",
    			g.V().has("aai-node-type", L_INTERFACE_NODE_TYPE).has("interface-name", "empty-value")
    			.has(L2_MULTI_PROPERTY,false).hasNext());
    	
    	assertTrue("Value of node 3 \"true-value\" should have " + L2_MULTI_PROPERTY + " of true ",
    			g.V().has("aai-node-type", L_INTERFACE_NODE_TYPE).has("interface-name", "true-value")
    			.has(L2_MULTI_PROPERTY,true).hasNext()); 
    	
    	assertTrue("Value of node 4 \"false-value\" should have " + L2_MULTI_PROPERTY + " of false ",
    			g.V().has("aai-node-type", L_INTERFACE_NODE_TYPE).has("interface-name", "false-value")
    			.has(L2_MULTI_PROPERTY,false).hasNext()); 

    	assertTrue("Value of node 5 \"extra\" should have " + L2_MULTI_PROPERTY + " of false ",
    			g.V().has("aai-node-type", L_INTERFACE_NODE_TYPE).has("interface-name", "extra")
    			.has(L2_MULTI_PROPERTY,false).hasNext());     	
    }
    
    @Test
    public void testOtherMethods() {
    	assertTrue("getStatus function works", migration.getStatus().toString().contains("SUCCESS"));
    	
    	assertTrue("getAffectedNodeTypes returns " + L_INTERFACE_NODE_TYPE, 
    			Arrays.asList(migration.getAffectedNodeTypes().get()).contains(L_INTERFACE_NODE_TYPE));

    	assertTrue("getMigrationName returns MigrateL2DefaultToFalse", 
    			migration.getMigrationName().equals("MigrateL2DefaultToFalse"));
    }
}