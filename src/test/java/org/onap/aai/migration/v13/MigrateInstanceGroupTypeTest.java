/**
 * ============LICENSE_START=======================================================
 * org.onap.aai
 * ================================================================================
 * Copyright © 2017 AT&T Intellectual Property. All rights reserved.
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
 *
 * ECOMP is a trademark and service mark of AT&T Intellectual Property.
 */
package org.onap.aai.migration.v13;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertFalse;
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


public class MigrateInstanceGroupTypeTest extends AAISetup{

    private static final String TYPE_VALUE = "TypeValue";
    private final static ModelType introspectorFactoryType = ModelType.MOXY;
    private final static QueryStyle queryStyle = QueryStyle.TRAVERSAL;
    private final static DBConnectionType type = DBConnectionType.REALTIME;
    private Loader loader;
    private TransactionalGraphEngine dbEngine;
    private JanusGraph graph;
    private MigrateInstanceGroupType migration;
    private GraphTraversalSource g;
    private JanusGraphTransaction tx;
    Vertex instanceGroup;
    Vertex instanceGroupWithoutType;


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
         instanceGroup = g.addV().property("aai-node-type", MigrateInstanceGroupType.INSTANCE_GROUP_NODE_TYPE)
                .property( MigrateInstanceGroupType.TYPE_PROPERTY, TYPE_VALUE)                
                .next();
         
         instanceGroupWithoutType = g.addV().property("aai-node-type", MigrateInstanceGroupType.INSTANCE_GROUP_NODE_TYPE)
                 .next();
         
        TransactionalGraphEngine spy = spy(dbEngine);
        TransactionalGraphEngine.Admin adminSpy = spy(dbEngine.asAdmin());
        GraphTraversalSource traversal = g;
        when(spy.asAdmin()).thenReturn(adminSpy);
        when(adminSpy.getTraversalSource()).thenReturn(traversal);
        migration = new MigrateInstanceGroupType(spy, loaderFactory, edgeIngestor, edgeSerializer, schemaVersions);
    }

    @After
    public void cleanUp() {
        tx.rollback();
        graph.close();
    }


    /***
     * checks if the type/subtype property were renamed
     */

    @Test
    public void confirmTypeAndSubTypeWereRenamed() {
        migration.run();

        //instance group with type
        assertEquals(TYPE_VALUE, instanceGroup.property(MigrateInstanceGroupType.INSTANCE_GROUP_TYPE_PROPERTY).value());
        assertFalse(instanceGroup.property(MigrateInstanceGroupType.TYPE_PROPERTY).isPresent());

        //instance group without type
        assertFalse(instanceGroupWithoutType.property(MigrateInstanceGroupType.INSTANCE_GROUP_TYPE_PROPERTY).isPresent());
        assertFalse(instanceGroupWithoutType.property(MigrateInstanceGroupType.TYPE_PROPERTY).isPresent());
    }
}