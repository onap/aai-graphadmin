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

import java.time.Instant;
import java.util.Collections;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.Set;

import org.apache.tinkerpop.gremlin.process.traversal.dsl.graph.GraphTraversalSource;
import org.apache.tinkerpop.gremlin.structure.Edge;
import org.javatuples.Pair;
import org.onap.aai.edges.EdgeIngestor;
import org.onap.aai.introspection.LoaderFactory;
import org.onap.aai.serialization.db.EdgeSerializer;
import org.onap.aai.serialization.engines.TransactionalGraphEngine;
import org.onap.aai.setup.SchemaVersions;

@MigrationPriority(-1)
public class RebuildAllEdges extends EdgeMigrator {
    
    private static final Map<String, String> LABEL_MAP = getLabelMap();
    
    private static Map<String, String> getLabelMap() {
        Map<String, String> labelMap = new HashMap<>();
        labelMap.put("isMemberOf", "org.onap.relationships.inventory.MemberOf");
        labelMap.put("isA", "org.onap.relationships.inventory.IsA");
        labelMap.put("has", "org.onap.relationships.inventory.Uses");
        labelMap.put("usesLogicalLink", "tosca.relationships.network.LinksTo");
        labelMap.put("sourceLInterface", "org.onap.relationships.inventory.Source");
        labelMap.put("targetLInterface", "org.onap.relationships.inventory.Destination");
        return labelMap;
    }
    
    public RebuildAllEdges(TransactionalGraphEngine engine, LoaderFactory loaderFactory, EdgeIngestor edgeIngestor, 
                           EdgeSerializer edgeSerializer, SchemaVersions schemaVersions) {
        super(engine, loaderFactory, edgeIngestor, edgeSerializer, schemaVersions);
    }

    @Override
    protected void executeModifyOperation() {
        Instant started = Instant.now();
        logger.info("Started at: " + started);
        GraphTraversalSource graphTraversalSource = engine.asAdmin().getTraversalSource();
        Set<Edge> edges = graphTraversalSource.E().toSet();
        rebuildEdges(edges);
        Instant completed = Instant.now();
        logger.info("Completed at: " + completed + ". Total time taken in ms : "
                + (completed.toEpochMilli() - started.toEpochMilli()));
        logger.info(MIGRATION_SUMMARY_COUNT + " Total Edges : " + edges.size() + " . Processed count " + processed
                + " . Skipped count: " + skipped + ".");
        logger.info(MIGRATION_SUMMARY_COUNT + "Edge Missing Parent Property Count: " 
                + edgeMissingParentProperty.size());
        logger.info(MIGRATION_ERROR + "Edge Multiplicity Exception Count : "
                + edgeMultiplicityExceptionCtr.values().stream().mapToInt(Number::intValue).sum());
        logger.info(MIGRATION_ERROR + "Edge Multiplicity Exception Breakdown : " + edgeMultiplicityExceptionCtr);
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
