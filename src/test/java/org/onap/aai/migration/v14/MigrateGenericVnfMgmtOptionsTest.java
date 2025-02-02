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

import org.onap.aai.AAISetup;
import org.onap.aai.edges.EdgeIngestor;
import org.onap.aai.introspection.LoaderFactory;
import org.onap.aai.serialization.db.EdgeSerializer;
import org.onap.aai.serialization.engines.TransactionalGraphEngine;


import org.janusgraph.core.JanusGraph;
import org.janusgraph.core.JanusGraphFactory;
import org.janusgraph.core.JanusGraphTransaction;
import org.apache.tinkerpop.gremlin.process.traversal.dsl.graph.GraphTraversalSource;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

import org.onap.aai.dbmap.DBConnectionType;
import org.onap.aai.introspection.Loader;
import org.onap.aai.introspection.ModelType;
import org.onap.aai.setup.SchemaVersions;
import org.onap.aai.setup.SchemaVersion;
import org.onap.aai.migration.Status;
import org.onap.aai.serialization.engines.QueryStyle;
import org.onap.aai.serialization.engines.JanusGraphDBEngine;

import static org.junit.jupiter.api.Assertions.assertTrue;
import static org.mockito.Mockito.spy;
import static org.mockito.Mockito.when;

import java.util.Optional;

import jakarta.validation.constraints.AssertFalse;

public class MigrateGenericVnfMgmtOptionsTest extends AAISetup {
	
	protected static final String VNF_NODE_TYPE = "generic-vnf";

	public static class MigrateVnfType extends MigrateGenericVnfMgmtOptions {
        public MigrateVnfType(TransactionalGraphEngine engine, LoaderFactory loaderFactory, EdgeIngestor edgeIngestor, EdgeSerializer edgeSerializer, SchemaVersions schemaVersions){
            super(engine, loaderFactory, edgeIngestor, edgeSerializer, schemaVersions);
        }
        @Override
        public Status getStatus() {
            return Status.SUCCESS;
        }
        @Override
        public Optional<String[]> getAffectedNodeTypes() {
        	return Optional.of(new String[]{VNF_NODE_TYPE});
        }
        @Override
        public String getMigrationName() {
            return "MockMigrateVnfType";
        }
    }

    private final static ModelType introspectorFactoryType = ModelType.MOXY;
    private final static QueryStyle queryStyle = QueryStyle.TRAVERSAL;
    private Loader loader;
    private TransactionalGraphEngine dbEngine;
    private MigrateVnfType migration;
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
                .property("vnf-type", "HN")
                .next();
        g.addV().property("aai-node-type", "generic-vnf")
                .property("vnf-id", "generic-vnf1")
                .property("vnf-type", "HN")
                .property("management-option", "")
                .next();
        g.addV().property("aai-node-type", "generic-vnf")
                .property("vnf-id", "generic-vnf2")
                .property("vnf-type", "HN")
                .property("management-option", "existingOption")
                .next();
        
        g.addV().property("aai-node-type", "generic-vnf")
        		.property("vnf-id", "generic-vnf10")
        		.property("vnf-type", "HP")
        		.next();
        g.addV().property("aai-node-type", "generic-vnf")
        		.property("vnf-id", "generic-vnf11")
        		.property("vnf-type", "HP")
        		.property("management-option", "")
        		.next();
        g.addV().property("aai-node-type", "generic-vnf")
        		.property("vnf-id", "generic-vnf12")
        		.property("vnf-type", "HP")
        		.property("management-option", "existingOption")
        		.next();
        
        g.addV().property("aai-node-type", "generic-vnf")
				.property("vnf-id", "generic-vnf20")
				.property("vnf-type", "HG")
				.next();
        g.addV().property("aai-node-type", "generic-vnf")
				.property("vnf-id", "generic-vnf21")
				.property("vnf-type", "HG")
				.property("management-option", "")
				.next();
        g.addV().property("aai-node-type", "generic-vnf")
				.property("vnf-id", "generic-vnf22")
				.property("vnf-type", "HG")
				.property("management-option", "existingOption")
				.next();        
        
        // Non-eligible migration conditions - vnf-type = XX
        g.addV().property("aai-node-type", "generic-vnf")
        		.property("vnf-id", "generic-vnf30")
        		.property("vnf-type", "XX")
        		.next();
        g.addV().property("aai-node-type", "generic-vnf")
        		.property("vnf-id", "generic-vnf31")
        		.property("vnf-type", "XX")
        		.property("management-option", "")
        		.next();
        g.addV().property("aai-node-type", "generic-vnf")
				.property("vnf-id", "generic-vnf32")
				.property("vnf-type", "XX")
				.property("management-option", "existingOption")
				.next(); 
        // Non-eligible migration conditions - vnf-type = missing
        g.addV().property("aai-node-type", "generic-vnf")
        		.property("vnf-id", "generic-vnf40")
        		.next();
        g.addV().property("aai-node-type", "generic-vnf")
        		.property("vnf-id", "generic-vnf41")
        		 .property("management-option", "")
        		.next();
        g.addV().property("aai-node-type", "generic-vnf")
				.property("vnf-id", "generic-vnf42")
				.property("management-option", "existingOption")
				.next(); 
        
        TransactionalGraphEngine spy = spy(dbEngine);
        TransactionalGraphEngine.Admin adminSpy = spy(dbEngine.asAdmin());
        GraphTraversalSource traversal = g;
        when(spy.asAdmin()).thenReturn(adminSpy);
        when(adminSpy.getTraversalSource()).thenReturn(traversal);
        migration = new MigrateVnfType(spy, loaderFactory, edgeIngestor, edgeSerializer, schemaVersions);
        migration.run();
        
    }

    @Test
    public void testMissingProperty(){
    	//management-option
        assertTrue(g.V().has("aai-node-type", "generic-vnf").has("vnf-id", "generic-vnf0").has("management-option", "AT&T Managed-Basic").hasNext(),
                "Value of generic-vnf should be updated since the property management-option doesn't exist");      
        assertTrue(g.V().has("aai-node-type", "generic-vnf").has("vnf-id", "generic-vnf10").has("management-option", "AT&T Managed-Basic").hasNext(),
                "Value of generic-vnf  should be updated since the property management-option doesn't exist");
        assertTrue(g.V().has("aai-node-type", "generic-vnf").has("vnf-id", "generic-vnf20").has("management-option", "AT&T Managed-Basic").hasNext(),
                "Value of generic-vnf  should be updated since the property management-option doesn't exist");              
    }

    @Test
    public void testEmptyValue() {                         
      //management-option
        assertTrue(g.V().has("aai-node-type", "generic-vnf").has("vnf-id", "generic-vnf1").has("management-option", "AT&T Managed-Basic").hasNext(),
                "Value of generic-vnf should be updated since the value for management-option is an empty string");
        assertTrue(g.V().has("aai-node-type", "generic-vnf").has("vnf-id", "generic-vnf11").has("management-option", "AT&T Managed-Basic").hasNext(),
                "Value of generic-vnf should be updated since the value for management-option is an empty string");
        assertTrue(g.V().has("aai-node-type", "generic-vnf").has("vnf-id", "generic-vnf21").has("management-option", "AT&T Managed-Basic").hasNext(),
                "Value of generic-vnf should be updated since the value for management-option is an empty string");
    
    }
    
    @Test
    public void testExistingValues() {
      //management-option
        assertTrue(!g.V().has("aai-node-type", "generic-vnf").has("vnf-id", "generic-vnf2").has("management-option", "AT&T Managed-Basic").hasNext(),
                "Value of generic-vnf shouldn't be updated since management-option already exists");
        assertTrue(!g.V().has("aai-node-type", "generic-vnf").has("vnf-id", "generic-vnf12").has("management-option", "AT&T Managed-Basic").hasNext(),
                "Value of generic-vnf shouldn't be updated since management-option already exists");
        assertTrue(!g.V().has("aai-node-type", "generic-vnf").has("vnf-id", "generic-vnf22").has("management-option", "AT&T Managed-Basic").hasNext(),
                "Value of generic-vnf shouldn't be updated since management-option already exists");
       
        
    }
    
   @Test
    public void testExistingVnfsNotMigrated() {
    	//management-option
        assertTrue(!g.V().has("aai-node-type", "generic-vnf").has("vnf-id", "generic-vnf30").has("management-option", "AT&T Managed-Basic").hasNext(),
                "Value of generic-vnf shouldn't be updated since vnf-type is not affected");
        assertTrue(!g.V().has("aai-node-type", "generic-vnf").has("vnf-id", "generic-vnf31").has("management-option", "AT&T Managed-Basic").hasNext(),
                "Value of generic-vnf  shouldn't be updated since vnf-type is not affected");
        assertTrue(!g.V().has("aai-node-type", "generic-vnf").has("vnf-id", "generic-vnf32").has("management-option", "AT&T Managed-Basic").hasNext(),
                "Value of generic-vnf  shouldn't be updated since vnf-type is not affected and management-option already exists");
        
        assertTrue(!g.V().has("aai-node-type", "generic-vnf").has("vnf-id", "generic-vnf40").has("management-option", "AT&T Managed-Basic").hasNext(),
                "Value of generic-vnf shouldn't be updated since vnf-type is not present");
        assertTrue(!g.V().has("aai-node-type", "generic-vnf").has("vnf-id", "generic-vnf41").has("management-option", "AT&T Managed-Basic").hasNext(),
                "Value of generic-vnf  shouldn't be updated since vnf-type is not present");
        assertTrue(!g.V().has("aai-node-type", "generic-vnf").has("vnf-id", "generic-vnf42").has("management-option", "AT&T Managed-Basic").hasNext(),
                "Value of generic-vnf  shouldn't be updated since vnf-type is not present and management-option already exists");
      
    } 
}