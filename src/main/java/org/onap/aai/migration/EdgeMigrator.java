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

import com.google.common.collect.Multimap;

import java.util.ArrayList;
import java.util.Collection;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.stream.Collectors;

import org.apache.tinkerpop.gremlin.process.traversal.dsl.graph.GraphTraversal;
import org.apache.tinkerpop.gremlin.process.traversal.dsl.graph.GraphTraversalSource;
import org.apache.tinkerpop.gremlin.process.traversal.dsl.graph.__;
import org.apache.tinkerpop.gremlin.structure.Edge;
import org.apache.tinkerpop.gremlin.structure.Vertex;
import org.javatuples.Pair;
import org.onap.aai.db.props.AAIProperties;
import org.onap.aai.edges.EdgeIngestor;
import org.onap.aai.edges.EdgeRule;
import org.onap.aai.edges.EdgeRuleQuery;
import org.onap.aai.edges.enums.EdgeType;
import org.onap.aai.exceptions.AAIException;
import org.onap.aai.introspection.LoaderFactory;
import org.onap.aai.serialization.db.EdgeSerializer;
import org.onap.aai.serialization.db.exceptions.EdgeMultiplicityException;
import org.onap.aai.serialization.engines.TransactionalGraphEngine;
import org.onap.aai.setup.SchemaVersions;


/**
 * A migration template for migrating all edge properties between "from" and "to" node from the DbedgeRules.json
 * 
 */
@MigrationPriority(0)
@MigrationDangerRating(1)
public abstract class EdgeMigrator extends Migrator {

    protected int processed = 0;
    protected int skipped = 0;
    protected Map<String, Integer> edgeMultiplicityExceptionCtr = new HashMap<>();
    protected List<String> edgeMissingParentProperty = new ArrayList<>();
    
    private boolean success = true;

    public EdgeMigrator(TransactionalGraphEngine engine, LoaderFactory loaderFactory, EdgeIngestor edgeIngestor, 
                        EdgeSerializer edgeSerializer, SchemaVersions schemaVersions) {
        super(engine, loaderFactory, edgeIngestor, edgeSerializer, schemaVersions);
    }
    
    /**
     * Do not override this method as an inheritor of this class.
     */
    @Override
    public void run() {

        executeModifyOperation();

    }

    /**
     * This is where inheritors should add their logic.
     */
    protected void executeModifyOperation() {
        
        changeEdgeProperties();
        
    }

    protected void changeEdgeLabels() {
    //TODO: when json file has edge label as well as edge property changes
    }
    
    
    
    protected void changeEdgeProperties() {
        try {
            List<Pair<String, String>> nodePairList = this.getAffectedNodePairTypes();
            for (Pair<String, String> nodePair : nodePairList) {
                
                String NODE_A = nodePair.getValue0();
                String NODE_B = nodePair.getValue1();
                Multimap<String, EdgeRule> result = edgeIngestor.getRules(new EdgeRuleQuery.Builder(NODE_A, NODE_B).build());

                GraphTraversal<Vertex, Vertex> g = this.engine.asAdmin().getTraversalSource().V();
                /*
                 * Find Out-Edges from Node A to Node B and change them
                 * Also Find Out-Edges from Node B to Node A and change them 
                 */
                g.union(__.has(AAIProperties.NODE_TYPE, NODE_A).outE().where(__.inV().has(AAIProperties.NODE_TYPE, NODE_B)),
                        __.has(AAIProperties.NODE_TYPE, NODE_B).outE().where(__.inV().has(AAIProperties.NODE_TYPE, NODE_A)))
                        .sideEffect(t -> {
                            Edge e = t.get();
                            try {
                                Vertex out = e.outVertex();
                                Vertex in = e.inVertex();
                                if (out == null || in == null) {
                                    logger.error(
                                            e.id() + " invalid because one vertex was null: out=" + out + " in=" + in);
                                } else {
                                    if (result.containsKey(e.label())) {
                                        EdgeRule rule = result.get(e.label()).iterator().next();
                                        e.properties().forEachRemaining(prop -> prop.remove());
                                        edgeSerializer.addProperties(e, rule);
                                    } else {
                                        logger.debug("found vertices connected by unkwown label: out=" + out + " label="
                                                + e.label() + " in=" + in);
                                    }
                                }
                            } catch (Exception e1) {
                                throw new RuntimeException(e1);
                            }
                        }).iterate();
            }

        } catch (Exception e) {
            logger.error("error encountered", e);
            success = false;
        }
    }
  
    @Override
    public Status getStatus() {
        if (success) {
            return Status.SUCCESS;
        } else {
            return Status.FAILURE;
        }
    }

    /**
     * List of node pairs("from" and "to"), you would like EdgeMigrator to migrate from json files.
     * @return
     */
    public abstract List<Pair<String, String>> getAffectedNodePairTypes() ;

    /**
     * Takes the set of edges, and rebuild them according to current edgeRules schema.
     * @param edges takes set of edges as input.
     */
    protected void rebuildEdges(Set<Edge> edges) {
        Vertex out = null;
        Vertex in = null;
        String oldEdgeString = null;
        edgeMultiplicityExceptionCtr = new HashMap<>();
        edgeMissingParentProperty = new ArrayList<>();
        GraphTraversalSource graphTraversalSource = engine.asAdmin().getTraversalSource();
        try {
            for (Edge edge : edges) {
                oldEdgeString = toStringForPrinting(edge, 1);
                out = edge.outVertex();
                in = edge.inVertex();
                rebuildEdge(edge, graphTraversalSource);
            }
        } catch (Exception ex) {
            logger.error(MIGRATION_ERROR + "exception occurred during migration, failing: out=" + out + " in=" + in
                    + "edge=" + oldEdgeString, ex);
            success = false;
        }
    }

    private void rebuildEdge(Edge edge, GraphTraversalSource graphTraversalSource)
            throws AAIException {
        boolean isCousin = false;
        Vertex out = edge.outVertex();
        Vertex in = edge.inVertex();
        if (out == null || in == null) {
            logger.error(edge.id() + " invalid because one vertex was null: out="
                    + edge.outVertex() + " in=" + edge.inVertex());
            skipped++;
            return;
        }
        
        if (edge.property("contains-other-v").isPresent()) {
            isCousin = "NONE".equals(edge.property("contains-other-v").value());
        } else if (edge.property("isParent").isPresent()) {
            isCousin = !(Boolean) edge.property("isParent").value();
        } else {
            edgeMissingParentProperty.add(this.toStringForPrinting(edge, 1));
        }

        String inVertexNodeType = in.value(AAIProperties.NODE_TYPE);
        String outVertexNodeType = out.value(AAIProperties.NODE_TYPE);
        String label = null;

        try {
            Collection<EdgeRule> edgeRules = edgeIngestor.getRules(new EdgeRuleQuery.Builder(inVertexNodeType,
                    outVertexNodeType).build()).values();
            Set<String> edgeLabels = edgeRules.stream().map(EdgeRule::getLabel).collect(Collectors.toSet());
            if (edgeLabels.size() > 1) {
                label = selectLabel(edge, edgeLabels);
                if (label == null) {
                    logger.warn("For Multiple EdgeRules between " + "out=" + outVertexNodeType + " in="
                            + inVertexNodeType + ": did not find label for edge :" + edge.id());
                }
            }
        } catch (Exception e) {
            logger.error(edge.id() + " did not migrate as no edge rule found for: out=" + outVertexNodeType
                    + " in=" + inVertexNodeType);
            skipped++;
            return;
        }

        try {
            edge.remove();
            if (isCousin) {
                edgeSerializer.addEdgeIfPossible(graphTraversalSource, in, out, label);
            } else {
                edgeSerializer.addTreeEdge(graphTraversalSource, out, in);
            }
            processed++;
        } catch (EdgeMultiplicityException edgeMultiplicityException) {
            logger.warn("Edge Multiplicity Exception: "
                    + "\nInV:\n" + this.toStringForPrinting(in, 1)
                    + "Edge:\n" + this.toStringForPrinting(edge, 1)
                    + "OutV:\n" + this.toStringForPrinting(out, 1)
            );

            final String mapKey = "OUT:" + outVertexNodeType + " "
                    + (isCousin ? EdgeType.COUSIN.toString() : EdgeType.TREE.toString()) + " "
                    + "IN:" + inVertexNodeType;
            if (edgeMultiplicityExceptionCtr.containsKey(mapKey)) {
                edgeMultiplicityExceptionCtr.put(mapKey, edgeMultiplicityExceptionCtr.get(mapKey) + 1);
            } else {
                edgeMultiplicityExceptionCtr.put(mapKey, 1);
            }
        }
    }

    /**
     * For selecting label from multiple EdgeLabels
     * (where labels got changed between edgeRules' versions),
     * you should override this method in inheritor class.
     * @param edge Edge
     * @param edgeLabels set of edgeLabels
     * @return
     */
    protected String selectLabel(Edge edge, Set<String> edgeLabels) {
        return null;
    }
}
