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
package org.onap.aai.migration;

import org.apache.tinkerpop.gremlin.process.traversal.dsl.graph.GraphTraversalSource;
import org.janusgraph.core.JanusGraph;
import org.janusgraph.core.JanusGraphFactory;
import org.janusgraph.core.JanusGraphTransaction;
import org.junit.Before;
import org.junit.Test;
import org.springframework.test.annotation.DirtiesContext;
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

import java.util.HashMap;
import java.util.Map;
import java.util.Optional;

import static org.junit.Assert.assertTrue;
import static org.mockito.Mockito.spy;
import static org.mockito.Mockito.when;

@DirtiesContext(classMode = DirtiesContext.ClassMode.BEFORE_CLASS)
public class ValueMigratorTest extends AAISetup{

    public static class SampleValueMigrator extends ValueMigrator {
        public SampleValueMigrator(TransactionalGraphEngine engine, LoaderFactory loaderFactory, EdgeIngestor edgeIngestor, EdgeSerializer edgeSerializer, SchemaVersions schemaVersions, Map map, Boolean updateExistingValues){
            super(engine, loaderFactory, edgeIngestor, edgeSerializer, schemaVersions, map, updateExistingValues);
        }
        @Override
        public Status getStatus() {
            return Status.SUCCESS;
        }
        @Override
        public Optional<String[]> getAffectedNodeTypes() {
            return null;
        }
        @Override
        public String getMigrationName() {
            return "SampleValueMigrator";
        }
    }

    private final static ModelType introspectorFactoryType = ModelType.MOXY;
    private final static QueryStyle queryStyle = QueryStyle.TRAVERSAL;
    private Loader loader;
    private TransactionalGraphEngine dbEngine;
    private JanusGraph graph;
    private SampleValueMigrator migration;
    private GraphTraversalSource g;
    private JanusGraphTransaction tx;
    private SampleValueMigrator existingValuesMigration;

    @Before
    public void setup() throws Exception{
        graph = JanusGraphFactory.build().set("storage.backend", "inmemory").open();
        tx = graph.newTransaction();
        g = tx.traversal();
        loader = loaderFactory.createLoaderForVersion(introspectorFactoryType, schemaVersions.getDefaultVersion());
        dbEngine = new JanusGraphDBEngine(
                queryStyle,
                loader);
        Map<String, Map> map = new HashMap<>();
        Map<String, Boolean> pair = new HashMap<>();
        pair.put("in-maint", true);
        map.put("pserver", pair);
        map.put("pnf", pair);
        g.addV().property("aai-node-type", "pserver")
                .property("pserver-id", "pserver0")
                .next();
        g.addV().property("aai-node-type", "pserver")
                .property("pserver-id", "pserver1")
                .property("in-maint", "")
                .next();
        g.addV().property("aai-node-type", "pserver")
                .property("pserver-id", "pserver2")
                .property("in-maint", false)
                .next();
        g.addV().property("aai-node-type", "pnf")
                .property("pnf-name","pnf1" )
                .property("in-maint", false)
                .next();
        TransactionalGraphEngine spy = spy(dbEngine);
        TransactionalGraphEngine.Admin adminSpy = spy(dbEngine.asAdmin());
        GraphTraversalSource traversal = g;
        when(spy.asAdmin()).thenReturn(adminSpy);
        when(adminSpy.getTraversalSource()).thenReturn(traversal);
        migration = new SampleValueMigrator(spy, loaderFactory, edgeIngestor, edgeSerializer, schemaVersions, map, false);
        migration.run();

        map = new HashMap<>();
        pair = new HashMap<>();
        pair.put("in-maint", true);
        map.put("pnf", pair);
        existingValuesMigration = new SampleValueMigrator(spy, loaderFactory, edgeIngestor, edgeSerializer, schemaVersions, map, true);
        existingValuesMigration.run();
    }

    @Test
    public void testMissingProperty(){
        assertTrue("Value of pnf should be updated since the property doesn't exist",
                g.V().has("aai-node-type", "pserver").has("pserver-id", "pserver0").has("in-maint", true).hasNext());
    }

    @Test
    public void testExistingValue() {
        assertTrue("Value of pserver shouldn't be updated since it already exists",
                g.V().has("aai-node-type", "pserver").has("pserver-id", "pserver2").has("in-maint", false).hasNext());
    }

    @Test
    public void testEmptyValue() {
        assertTrue("Value of pserver should be updated since the value is an empty string",
                g.V().has("aai-node-type", "pserver").has("pserver-id", "pserver1").has("in-maint", true).hasNext());
    }

    @Test
    public void testUpdateExistingValues() {
        assertTrue("Value of pnf should be updated even though it already exists",
                g.V().has("aai-node-type", "pnf").has("pnf-name", "pnf1").has("in-maint", true).hasNext());
    }
}
