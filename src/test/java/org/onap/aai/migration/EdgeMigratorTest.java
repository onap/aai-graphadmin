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
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
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

import static org.junit.jupiter.api.Assertions.*;
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

    @BeforeEach
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

    @AfterEach
    public void cleanUp() {
        tx.tx().rollback();
        graph.close();
    }

    @Test
    public void verifyVnfHasOnlyNewEdgeTest() {
        edgeMigrator.rebuildEdges(g.E().toSet());
        assertTrue(g.V().has(AAIProperties.NODE_TYPE,
                "generic-vnf").has("vnf-id", "toscaMigration-test-vnf").inE()
                .hasLabel("org.onap.relationships.inventory.BelongsTo").hasNext(), "edge direction and label were migrated");


        assertFalse(g.V().has(AAIProperties.NODE_TYPE,
                "generic-vnf").has("vnf-id", "toscaMigration-test-vnf").outE()
                .hasLabel("hasLInterface").hasNext(), "if we look for old edge, it should be gone");
    }

    @Test
    public void verifyGraphHasNoOldEdgeLabelsTest() {
        edgeMigrator.rebuildEdges(g.E().toSet());
        assertEquals(Long.valueOf(0)
                , g.E().hasLabel("hasLInterface","usesLogicalLink").count().next(), "Graph should have none of the old edge label");
        assertEquals(Long.valueOf(3)
                , g.E().hasLabel("org.onap.relationships.inventory.BelongsTo",
                        "tosca.relationships.network.LinksTo","org.onap.relationships.inventory.Source")
                        .count().next(), "Graph should have none of the old edge label");
    }

    @Test
    public void verifyGenericVnfHas1EdgeTest() {
        edgeMigrator.rebuildEdges(g.E().toSet());
        assertEquals(Long.valueOf(1)
                , g.V().has(AAIProperties.NODE_TYPE, "generic-vnf")
                        .both()
                        .count().next(), "Generic vnf should have 1 edge");

    }

    @Test
    public void verifyLogicalLinkHas2EdgesTest() {
        edgeMigrator.rebuildEdges(g.E().toSet());
        assertEquals(Long.valueOf(2)
                , g.V().has(AAIProperties.NODE_TYPE, "logical-link")
                        .both()
                        .count().next(), "Logical Link should have 2 edges");

        assertTrue(g.V().has(AAIProperties.NODE_TYPE, "logical-link")
                        .bothE("org.onap.relationships.inventory.Source").hasNext(), "Logical Link has source edge");

        assertTrue(g.V().has(AAIProperties.NODE_TYPE, "logical-link")
                        .bothE("tosca.relationships.network.LinksTo").hasNext(), "Logical Link has default edge");

    }

    @Test
    public void checkThatEdgeWithNoRulesDoesNotGetMigratedTest() {
        edgeMigrator.rebuildEdges(g.E().toSet());
        assertTrue(g.E().hasLabel("blah").hasNext(), "Edge with no rule did not get migrated ");
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