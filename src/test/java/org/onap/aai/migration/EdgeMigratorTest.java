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
import org.apache.tinkerpop.gremlin.structure.Edge;
import org.apache.tinkerpop.gremlin.structure.Graph;
import org.apache.tinkerpop.gremlin.structure.Vertex;
import org.janusgraph.core.JanusGraph;
import org.janusgraph.core.JanusGraphFactory;
import org.janusgraph.core.schema.JanusGraphManagement;
import org.javatuples.Pair;
import org.junit.After;
import org.junit.Before;
import org.junit.Test;
import org.mockito.Mockito;
import org.onap.aai.AAISetup;
import org.onap.aai.db.props.AAIProperties;
import org.onap.aai.edges.EdgeIngestor;
import org.onap.aai.edges.enums.AAIDirection;
import org.onap.aai.edges.enums.EdgeProperty;
import org.onap.aai.introspection.Loader;
import org.onap.aai.introspection.LoaderFactory;
import org.onap.aai.introspection.ModelType;
import org.onap.aai.serialization.db.EdgeSerializer;
import org.onap.aai.serialization.engines.JanusGraphDBEngine;
import org.onap.aai.serialization.engines.QueryStyle;
import org.onap.aai.serialization.engines.TransactionalGraphEngine;
import org.onap.aai.setup.SchemaVersions;

import java.util.*;

import static org.junit.Assert.*;
import static org.mockito.Mockito.spy;
import static org.mockito.Mockito.when;

public class EdgeMigratorTest extends AAISetup {
    
    public static class EdgeMigratorImpl extends EdgeMigrator {

        public EdgeMigratorImpl(TransactionalGraphEngine engine, LoaderFactory loaderFactory, EdgeIngestor edgeIngestor, EdgeSerializer edgeSerializer, SchemaVersions schemaVersions) {
            super(engine, loaderFactory, edgeIngestor, edgeSerializer, schemaVersions);
        }

        private static final Map<String, String> LABEL_MAP = getLabelMap();

        private static Map<String, String> getLabelMap() {
            Map<String, String> labelMap = new HashMap<>();
            labelMap.put("usesLogicalLink", "tosca.relationships.network.LinksTo");
            labelMap.put("sourceLInterface", "org.onap.relationships.inventory.Source");
            labelMap.put("targetLInterface", "org.onap.relationships.inventory.Destination");
            labelMap.put("isMemberOf", "org.onap.relationships.inventory.MemberOf");
            labelMap.put("isA", "org.onap.relationships.inventory.IsA");
            labelMap.put("has", "org.onap.relationships.inventory.Uses");
            return labelMap;
        }
        
        @Override
        protected String selectLabel(Edge edge, Set<String> edgeLabels) {
            return ( edgeLabels.contains(LABEL_MAP.get(edge.label())) ) ? LABEL_MAP.get(edge.label()) : null;
        }

        @Override
        public List<Pair<String, String>> getAffectedNodePairTypes() {
            return Collections.emptyList();
        }

        @Override
        public Optional<String[]> getAffectedNodeTypes() {
            return Optional.empty();
        }

        @Override
        public String getMigrationName() {
            return "RebuildAllEdges";
        }
    }

    private final static ModelType introspectorFactoryType = ModelType.MOXY;
    private final static QueryStyle queryStyle = QueryStyle.TRAVERSAL;
    private JanusGraph graph;
    private GraphTraversalSource g;
    private Graph tx;
    private EdgeMigratorImpl edgeMigrator;

    @Before
    public void setUp() {
        graph = JanusGraphFactory.build().set("storage.backend","inmemory").open();
        JanusGraphManagement janusgraphManagement = graph.openManagement();
        tx = graph.newTransaction();
        g = tx.traversal();
        Loader loader =
                loaderFactory.createLoaderForVersion(introspectorFactoryType, schemaVersions.getDefaultVersion());
        TransactionalGraphEngine dbEngine = new JanusGraphDBEngine(
                queryStyle,
                loader);

        Vertex gvnf = g.addV().property(AAIProperties.NODE_TYPE, "generic-vnf")
                .property("vnf-id", "toscaMigration-test-vnf")
                .next();

        Vertex lInterface = g.addV().property(AAIProperties.NODE_TYPE, "l-interface")
                .property("interface-name", "toscaMigration-test-lint")
                .next();

        Vertex logicalLink = g.addV().property(AAIProperties.NODE_TYPE, "logical-link")
                .property("link-name", "toscaMigration-logical-link")
                .next();
        
        gvnf.addEdge("has", lInterface, EdgeProperty.CONTAINS.toString(), AAIDirection.OUT.toString(),
                EdgeProperty.DELETE_OTHER_V.toString(), AAIDirection.NONE.toString());

        lInterface.addEdge("usesLogicalLink", logicalLink, EdgeProperty.CONTAINS.toString(),
                AAIDirection.NONE.toString(),EdgeProperty.DELETE_OTHER_V.toString(), AAIDirection.NONE.toString());
        lInterface.addEdge("sourceLInterface", logicalLink, EdgeProperty.CONTAINS.toString(),
                AAIDirection.NONE.toString(),EdgeProperty.DELETE_OTHER_V.toString(), AAIDirection.NONE.toString());

        Vertex pserver = g.addV().next();
        pserver.property("aai-node-type","pserver","hostname","a-name");
        Vertex vnfc = g.addV().next();
        vnfc.property("aai-node-type","vnfc","vnfc-name","a-name");
        pserver.addEdge("blah", vnfc, EdgeProperty.CONTAINS.toString(), AAIDirection.NONE.toString(),
                EdgeProperty.DELETE_OTHER_V.toString(), AAIDirection.NONE.toString());

        TransactionalGraphEngine spy = spy(dbEngine);
        TransactionalGraphEngine.Admin adminSpy = spy(dbEngine.asAdmin());
        GraphTraversalSource traversal = g;
        when(spy.asAdmin()).thenReturn(adminSpy);
        when(adminSpy.getTraversalSource()).thenReturn(traversal);
        Mockito.doReturn(janusgraphManagement).when(adminSpy).getManagementSystem();
        edgeMigrator = new EdgeMigratorImpl(spy, loaderFactory, edgeIngestor, edgeSerializer, schemaVersions);
    }

    @After
    public void cleanUp() {
        tx.tx().rollback();
        graph.close();
    }

    @Test
    public void verifyVnfHasOnlyNewEdgeTest() {
        edgeMigrator.rebuildEdges(g.E().toSet());
        assertTrue("edge direction and label were migrated", g.V().has(AAIProperties.NODE_TYPE,
                "generic-vnf").has("vnf-id", "toscaMigration-test-vnf").inE()
                .hasLabel("org.onap.relationships.inventory.BelongsTo").hasNext());


        assertFalse("if we look for old edge, it should be gone", g.V().has(AAIProperties.NODE_TYPE,
                "generic-vnf").has("vnf-id", "toscaMigration-test-vnf").outE()
                .hasLabel("hasLInterface").hasNext());
    }

    @Test
    public void verifyGraphHasNoOldEdgeLabelsTest() {
        edgeMigrator.rebuildEdges(g.E().toSet());
        assertEquals("Graph should have none of the old edge label"
                , Long.valueOf(0)
                , g.E().hasLabel("hasLInterface","usesLogicalLink").count().next());
        assertEquals("Graph should have none of the old edge label"
                , Long.valueOf(3)
                , g.E().hasLabel("org.onap.relationships.inventory.BelongsTo",
                        "tosca.relationships.network.LinksTo","org.onap.relationships.inventory.Source")
                        .count().next());
    }

    @Test
    public void verifyGenericVnfHas1EdgeTest() {
        edgeMigrator.rebuildEdges(g.E().toSet());
        assertEquals("Generic vnf should have 1 edge"
                , Long.valueOf(1)
                , g.V().has(AAIProperties.NODE_TYPE, "generic-vnf")
                        .both()
                        .count().next());

    }

    @Test
    public void verifyLogicalLinkHas2EdgesTest() {
        edgeMigrator.rebuildEdges(g.E().toSet());
        assertEquals("Logical Link should have 2 edges"
                , Long.valueOf(2)
                , g.V().has(AAIProperties.NODE_TYPE, "logical-link")
                        .both()
                        .count().next());

        assertTrue("Logical Link has source edge"
                , g.V().has(AAIProperties.NODE_TYPE, "logical-link")
                        .bothE("org.onap.relationships.inventory.Source").hasNext());

        assertTrue("Logical Link has default edge"
                , g.V().has(AAIProperties.NODE_TYPE, "logical-link")
                        .bothE("tosca.relationships.network.LinksTo").hasNext());

    }

    @Test
    public void checkThatEdgeWithNoRulesDoesNotGetMigratedTest() {
        edgeMigrator.rebuildEdges(g.E().toSet());
        assertTrue("Edge with no rule did not get migrated ", g.E().hasLabel("blah").hasNext());
    }
    
    @Test
    public void rebuildEdgesWithMulplicityExceptionTest() {
        Vertex sriovvf = g.addV().property(AAIProperties.NODE_TYPE, "sriov-vf").next();
        Vertex lInterface = g.addV().property(AAIProperties.NODE_TYPE, "l-interface")
                .property("interface-name1", "toscaMigration-test-lint")
                .next();

        sriovvf.addEdge("test1",lInterface,EdgeProperty.CONTAINS.toString(), AAIDirection.OUT.toString(),
                EdgeProperty.DELETE_OTHER_V.toString(), AAIDirection.NONE.toString());
        sriovvf.addEdge("test2",lInterface,EdgeProperty.CONTAINS.toString(), AAIDirection.OUT.toString(),
                EdgeProperty.DELETE_OTHER_V.toString(), AAIDirection.NONE.toString());
        edgeMigrator.rebuildEdges(g.E().toSet());
        assertEquals(1, edgeMigrator.edgeMultiplicityExceptionCtr.size());
        assertEquals(0, edgeMigrator.edgeMissingParentProperty.size());
        assertEquals(1, edgeMigrator.edgeMultiplicityExceptionCtr.values().stream().mapToInt(Number::intValue).sum());
    }
}