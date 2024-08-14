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
package org.onap.aai.migration.v16;

import org.apache.tinkerpop.gremlin.process.traversal.dsl.graph.GraphTraversalSource;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

import org.onap.aai.AAISetup;
import org.onap.aai.edges.EdgeIngestor;
import org.onap.aai.introspection.Loader;
import org.onap.aai.introspection.LoaderFactory;
import org.onap.aai.introspection.ModelType;
import org.onap.aai.migration.Status;
import org.onap.aai.serialization.db.EdgeSerializer;
import org.onap.aai.serialization.engines.JanusGraphDBEngine;
import org.onap.aai.serialization.engines.QueryStyle;
import org.onap.aai.serialization.engines.TransactionalGraphEngine;
import org.onap.aai.setup.SchemaVersions;

import java.util.Optional;

import static org.junit.jupiter.api.Assertions.assertTrue;
import static org.mockito.Mockito.spy;
import static org.mockito.Mockito.when;

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
        	return Optional.of(new String[]{CLOUD_REGION_NODE_TYPE});
        }
        @Override
        public String getMigrationName() {
            return "MockBooleanDefaultMigrator";
        }
    }

    private final static ModelType introspectorFactoryType = ModelType.MOXY;
    private final static QueryStyle queryStyle = QueryStyle.TRAVERSAL;
    private Loader loader;
    private TransactionalGraphEngine dbEngine;
    private BooleanDefaultMigrator migration;
    private GraphTraversalSource g;

    @BeforeEach
    public void setup() throws Exception{
        g = tx.traversal();
        loader = loaderFactory.createLoaderForVersion(introspectorFactoryType, schemaVersions.getDefaultVersion());
        dbEngine = new JanusGraphDBEngine(
                queryStyle,
                loader);

        //cloud-region
        g.addV().property("aai-node-type", "cloud-region")
                .property("cloud-region-id", "cloud-region0")
                .next();
        g.addV().property("aai-node-type", "cloud-region")
                .property("cloud-region-id", "cloud-region1")
                .property("orchestration-disabled", "")
                .next();
        g.addV().property("aai-node-type", "cloud-region")
                .property("cloud-region-id", "cloud-region2")
                .property("orchestration-disabled", true)
                .next();
        g.addV().property("aai-node-type", "cloud-region")
        		.property("cloud-region-id", "cloud-region3")
        		.property("orchestration-disabled", false)
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
    	//orchestration-disabled
        assertTrue(g.V().has("aai-node-type", "cloud-region").has("cloud-region-id", "cloud-region0").has("orchestration-disabled", false).hasNext(),
                "Value of cloud-region should be updated since the property orchestration-disabled doesn't exist");
    }

    @Test
    public void testEmptyValue() {                         
      //orchestration-disabled
        assertTrue(g.V().has("aai-node-type", "cloud-region").has("cloud-region-id", "cloud-region1").has("orchestration-disabled", false).hasNext(),
                "Value of cloud-region should be updated since the value for orchestration-disabled is an empty string");
    }
    
    @Test
    public void testExistingTrueValues() {
      //orchestration-disabled
        assertTrue(g.V().has("aai-node-type", "cloud-region").has("cloud-region-id", "cloud-region2").has("orchestration-disabled", true).hasNext(),
                "Value of cloud-region shouldn't be update since orchestration-disabled already exists");
        
    }
    
    @Test
    public void testExistingFalseValues() {
    	//orchestration-disabled
        assertTrue(g.V().has("aai-node-type", "cloud-region").has("cloud-region-id", "cloud-region3").has("orchestration-disabled", false).hasNext(),
                "Value of cloud-region shouldn't be update since orchestration-disabled already exists");
    } 
}