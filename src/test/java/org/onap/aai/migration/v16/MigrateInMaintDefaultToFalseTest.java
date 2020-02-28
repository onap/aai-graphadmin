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
import org.junit.Before;
import org.junit.Test;
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

import static org.junit.Assert.assertTrue;
import static org.mockito.Mockito.spy;
import static org.mockito.Mockito.when;

public class MigrateInMaintDefaultToFalseTest extends
		AAISetup {
	
	protected static final String ZONE_NODE_TYPE = "zone";
	protected static final String CLOUD_REGION_NODE_TYPE = "cloud-region";

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
        	return Optional.of(new String[]{ZONE_NODE_TYPE,CLOUD_REGION_NODE_TYPE});
        }
        @Override
        public String getMigrationName() {
            return "MockInMaintDefaultMigrator";
        }
    }

    private final static ModelType introspectorFactoryType = ModelType.MOXY;
    private final static QueryStyle queryStyle = QueryStyle.TRAVERSAL;
    private Loader loader;
    private TransactionalGraphEngine dbEngine;
    private InMaintDefaultMigrator migration;
    private GraphTraversalSource g;

    @Before
    public void setup() throws Exception{
        g = tx.traversal();
        loader = loaderFactory.createLoaderForVersion(introspectorFactoryType, schemaVersions.getDefaultVersion());
        dbEngine = new JanusGraphDBEngine(
                queryStyle,
                loader);

        //zone
        g.addV().property("aai-node-type", "zone")
                .property("zone-id", "zone0")
                .next();
        g.addV().property("aai-node-type", "zone")
                .property("zone-id", "zone1")
                .property("in-maint", "")
                .next();
        g.addV().property("aai-node-type", "zone")
                .property("zone-id", "zone2")
                .property("in-maint", true)
                .next();
        g.addV().property("aai-node-type", "zone")
        		.property("zone-id", "zone3")
        		.property("in-maint", false)
        		.next();        
      //cloud-region
        g.addV().property("aai-node-type", "cloud-region")
                .property("cloud-region-id", "cloud-region0")
                .next();
        g.addV().property("aai-node-type", "cloud-region")
                .property("cloud-region-id", "cloud-region1")
                .property("in-maint", "")
                .next();
        g.addV().property("aai-node-type", "cloud-region")
                .property("cloud-region-id", "cloud-region2")
                .property("in-maint", true)
                .next();
        g.addV().property("aai-node-type", "cloud-region")
        		.property("cloud-region-id", "cloud-region3")
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
        assertTrue("Value of zone should be updated since the property in-maint doesn't exist",
                g.V().has("aai-node-type", "zone").has("zone-id", "zone0").has("in-maint", false).hasNext());
        assertTrue("Value of cloud-region should be updated since the property in-maint doesn't exist",
                g.V().has("aai-node-type", "cloud-region").has("cloud-region-id", "cloud-region0").has("in-maint", false).hasNext());
        
    }

    @Test
    public void testEmptyValue() {                
        assertTrue("Value of zone should be updated since the value for in-maint is an empty string",
                g.V().has("aai-node-type", "zone").has("zone-id", "zone1").has("in-maint", false).hasNext());
        assertTrue("Value of cloud-region should be updated since the value for in-maint is an empty string",
                g.V().has("aai-node-type", "cloud-region").has("cloud-region-id", "cloud-region1").has("in-maint", false).hasNext());
        
    }
    
    @Test
    public void testExistingTrueValues() {
        assertTrue("Value of zone shouldn't be updated since in-maint already exists",
                g.V().has("aai-node-type", "zone").has("zone-id", "zone2").has("in-maint", true).hasNext());
        assertTrue("Value of cloud-region shouldn't be updated since in-maint already exists",
                g.V().has("aai-node-type", "cloud-region").has("cloud-region-id", "cloud-region2").has("in-maint", true).hasNext());
        
    }
    
    @Test
    public void testExistingFalseValues() {
        assertTrue("Value of zone shouldn't be updated since in-maint already exists",
                g.V().has("aai-node-type", "zone").has("zone-id", "zone3").has("in-maint", false).hasNext());
        assertTrue("Value of cloud-region shouldn't be updated since in-maint already exists",
                g.V().has("aai-node-type", "cloud-region").has("cloud-region-id", "cloud-region3").has("in-maint", false).hasNext());
        
    }
}