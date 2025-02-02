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
package org.onap.aai.migration.v13;
import java.util.HashMap;
import java.util.Iterator;
import java.util.List;
import java.util.Map;
import java.util.Optional;

import org.apache.tinkerpop.gremlin.structure.Vertex;
import org.onap.aai.db.props.AAIProperties;
import org.onap.aai.edges.EdgeIngestor;
import org.onap.aai.introspection.LoaderFactory;
import org.onap.aai.migration.Enabled;
import org.onap.aai.migration.MigrationDangerRating;
import org.onap.aai.migration.MigrationPriority;
import org.onap.aai.migration.Migrator;
import org.onap.aai.migration.Status;
import org.onap.aai.serialization.db.EdgeSerializer;
import org.onap.aai.serialization.engines.TransactionalGraphEngine;
import org.onap.aai.setup.SchemaVersions;

@MigrationPriority(20)
@MigrationDangerRating(2)
@Enabled
public class MigrateModelVer extends Migrator{

    protected static final String MODELINVARIANTID = "model-invariant-id";
    protected static final String MODELVERSIONID = "model-version-id";
    protected static final String MODELINVARIANTIDLOCAL = "model-invariant-id-local";
    protected static final String MODELVERSIONIDLOCAL = "model-version-id-local";
    
    protected static final String MODELVER = "model-ver";
    protected static final String MODEL = "model";

    protected static final String CONNECTOR_NODETYPE = "connector";
    protected static final String SERVICEINSTANCE_NODETYPE = "service-instance";
    protected static final String CONFIGURATION_NODETYPE = "configuration";
    protected static final String LOGICALLINK_NODETYPE = "logical-link";
    protected static final String VNFC_NODETYPE = "vnfc";
    protected static final String L3NETWORK_NODETYPE = "l3-network";
    protected static final String GENERICVNF_NODETYPE = "generic-vnf";
    protected static final String PNF_NODETYPE = "pnf";
    protected static final String VFMODULE_NODETYPE = "vf-module";
    protected static final String INSTANCEGROUP_NODETYPE = "instance-group";
    protected static final String ALLOTTEDRESOURCE_NODETYPE = "allotted-resource";
    protected static final String COLLECTION_NODETYPE = "collection";

    private boolean success = true;
    
    private static Map<String, String> NODETYPEKEYMAP = new HashMap<String, String>();
    
    static {
    	NODETYPEKEYMAP.put(CONNECTOR_NODETYPE,"resource-instance-id");
    	NODETYPEKEYMAP.put(SERVICEINSTANCE_NODETYPE,"service-instance-id");
    	NODETYPEKEYMAP.put(CONFIGURATION_NODETYPE, "configuration-id");
    	NODETYPEKEYMAP.put(LOGICALLINK_NODETYPE,"link-name");
    	NODETYPEKEYMAP.put(VNFC_NODETYPE, "vnfc-name");
    	NODETYPEKEYMAP.put(L3NETWORK_NODETYPE, "network-id");
    	NODETYPEKEYMAP.put(GENERICVNF_NODETYPE,"vnf-id");
    	NODETYPEKEYMAP.put(PNF_NODETYPE,"pnf-name");
    	NODETYPEKEYMAP.put(VFMODULE_NODETYPE,"vf-module-id");
    	NODETYPEKEYMAP.put(INSTANCEGROUP_NODETYPE,"id");
    	NODETYPEKEYMAP.put(ALLOTTEDRESOURCE_NODETYPE,"id");
    	NODETYPEKEYMAP.put(COLLECTION_NODETYPE,"collection-id");
    }

    public MigrateModelVer(TransactionalGraphEngine engine, LoaderFactory loaderFactory, EdgeIngestor edgeIngestor, EdgeSerializer edgeSerializer, SchemaVersions schemaVersions) {
        super(engine, loaderFactory, edgeIngestor, edgeSerializer, schemaVersions);
    }

    @Override
    public void run() {

        List<Vertex> vertextList = this.engine.asAdmin().getTraversalSource().V().has(AAIProperties.NODE_TYPE, CONNECTOR_NODETYPE).has(MODELINVARIANTIDLOCAL).has(MODELVERSIONIDLOCAL).toList();
        createEdges(vertextList, CONNECTOR_NODETYPE);

        vertextList = this.engine.asAdmin().getTraversalSource().V().has(AAIProperties.NODE_TYPE, SERVICEINSTANCE_NODETYPE).has(MODELINVARIANTIDLOCAL).has(MODELVERSIONIDLOCAL).toList();
        createEdges(vertextList, SERVICEINSTANCE_NODETYPE);

        vertextList = this.engine.asAdmin().getTraversalSource().V().has(AAIProperties.NODE_TYPE, CONFIGURATION_NODETYPE).has(MODELINVARIANTIDLOCAL).has(MODELVERSIONIDLOCAL).toList();
        createEdges(vertextList, CONFIGURATION_NODETYPE);

        vertextList = this.engine.asAdmin().getTraversalSource().V().has(AAIProperties.NODE_TYPE, LOGICALLINK_NODETYPE).has(MODELINVARIANTIDLOCAL).has(MODELVERSIONIDLOCAL).toList();
        createEdges(vertextList, LOGICALLINK_NODETYPE);

        vertextList = this.engine.asAdmin().getTraversalSource().V().has(AAIProperties.NODE_TYPE, VNFC_NODETYPE).has(MODELINVARIANTIDLOCAL).has(MODELVERSIONIDLOCAL).toList();
        createEdges(vertextList, VNFC_NODETYPE);

        vertextList = this.engine.asAdmin().getTraversalSource().V().has(AAIProperties.NODE_TYPE, L3NETWORK_NODETYPE).has(MODELINVARIANTIDLOCAL).has(MODELVERSIONIDLOCAL).toList();
        createEdges(vertextList, L3NETWORK_NODETYPE);

        vertextList = this.engine.asAdmin().getTraversalSource().V().has(AAIProperties.NODE_TYPE, GENERICVNF_NODETYPE).has(MODELINVARIANTIDLOCAL).has(MODELVERSIONIDLOCAL).toList();
        createEdges(vertextList, GENERICVNF_NODETYPE);

        vertextList = this.engine.asAdmin().getTraversalSource().V().has(AAIProperties.NODE_TYPE, PNF_NODETYPE).has(MODELINVARIANTIDLOCAL).has(MODELVERSIONIDLOCAL).toList();
        createEdges(vertextList, PNF_NODETYPE);

        vertextList = this.engine.asAdmin().getTraversalSource().V().has(AAIProperties.NODE_TYPE, VFMODULE_NODETYPE).has(MODELINVARIANTIDLOCAL).has(MODELVERSIONIDLOCAL).toList();
        createEdges(vertextList, VFMODULE_NODETYPE);

        vertextList = this.engine.asAdmin().getTraversalSource().V().has(AAIProperties.NODE_TYPE, INSTANCEGROUP_NODETYPE).has(MODELINVARIANTIDLOCAL).has(MODELVERSIONIDLOCAL).toList();
        createEdges(vertextList, INSTANCEGROUP_NODETYPE);

        vertextList = this.engine.asAdmin().getTraversalSource().V().has(AAIProperties.NODE_TYPE, ALLOTTEDRESOURCE_NODETYPE).has(MODELINVARIANTIDLOCAL).has(MODELVERSIONIDLOCAL).toList();
        createEdges(vertextList, ALLOTTEDRESOURCE_NODETYPE);

        vertextList = this.engine.asAdmin().getTraversalSource().V().has(AAIProperties.NODE_TYPE, COLLECTION_NODETYPE).has(MODELINVARIANTIDLOCAL).has(MODELVERSIONIDLOCAL).toList();
        createEdges(vertextList, COLLECTION_NODETYPE);
    }

    private void createEdges(List<Vertex> sourceVertexList, String nodeTypeString)
    {
        int modelVerEdgeCount = 0;
        int modelVerEdgeErrorCount = 0;

        logger.info("---------- Start Creating an Edge for " + nodeTypeString + " nodes with Model Invariant Id and Model Version Id to the model-ver  ----------");
        Map<String, Vertex> modelVerUriVtxIdMap = new HashMap<String, Vertex>();
        for (Vertex vertex : sourceVertexList) {
            String currentValueModelVersionID = null;
            String currrentValueModelInvariantID = null;
            try {
                currentValueModelVersionID = getModelVersionIdNodeValue(vertex);
                currrentValueModelInvariantID = getModelInvariantIdNodeValue(vertex);
                
                String uri = "/service-design-and-creation/models/model/%s/model-vers/model-ver/%s".formatted(currrentValueModelInvariantID, currentValueModelVersionID);
                String propertyKey = NODETYPEKEYMAP.get(nodeTypeString);
                String propertyValue = vertex != null ? vertex.value(propertyKey).toString() : "";
                logger.info("Processing "+nodeTypeString+ " vertex with key "+ propertyValue);
                Vertex modelVerVertex = null;
                
                if (modelVerUriVtxIdMap.containsKey(uri)){
                	modelVerVertex = modelVerUriVtxIdMap.get(uri);
                } else {
                	List<Vertex> modelverList = this.engine.asAdmin().getTraversalSource().V().has(MODELINVARIANTID,currrentValueModelInvariantID).has(AAIProperties.NODE_TYPE, MODEL).in()
                    		.has(AAIProperties.NODE_TYPE, "model-ver" ).has("aai-uri", uri).toList();
                	if (modelverList != null && !modelverList.isEmpty()) {
                        modelVerVertex = modelverList.get(0);
                        modelVerUriVtxIdMap.put(uri, modelVerVertex);
                	}
                }
                
                if (modelVerVertex != null && modelVerVertex.property("model-version-id").isPresent() ) {
                    boolean edgePresent = false;
                    //Check if edge already exists for each of the source vertex
                    List<Vertex> outVertexList = this.engine.asAdmin().getTraversalSource().V(modelVerVertex).in().has("aai-node-type", nodeTypeString).has(propertyKey, propertyValue).toList();
                    Iterator<Vertex> vertexItr = outVertexList.iterator();
                    if (outVertexList != null &&  !outVertexList.isEmpty()  && vertexItr.hasNext()){
                    	logger.info("\t Edge already exists from " + nodeTypeString + " node to models-ver with model-invariant-id :" + currrentValueModelInvariantID + " and model-version-id :" + currentValueModelVersionID);
            			edgePresent = true;
            			continue;
                    }
                    // Build edge from vertex to modelVerVertex
                    if (!edgePresent) {
                    	this.createPrivateEdge(vertex, modelVerVertex);
                    	modelVerEdgeCount++;
                    }
                } else
                {
                    modelVerEdgeErrorCount++;
                    logger.info("\t" + MIGRATION_ERROR + "Unable to create edge. No model-ver vertex found with model-invariant-id :" + currrentValueModelInvariantID + " and model-version-id :" + currentValueModelVersionID);

                }
            } catch (Exception e) {
                success = false;
                modelVerEdgeErrorCount++;
                logger.error("\t" + MIGRATION_ERROR + "encountered exception from " + nodeTypeString + " node when trying to create edge to models-ver with model-invariant-id :" + currrentValueModelInvariantID + " and model-version-id :" + currentValueModelVersionID, e);
            }
        }

        logger.info ("\n \n ******* Summary " + nodeTypeString + " Nodes: Finished creating an Edge for " + nodeTypeString + " nodes with Model Invariant Id and Model Version Id to the model-ver Migration ********* \n");
        logger.info(MIGRATION_SUMMARY_COUNT+"Number of ModelVer edge created from " + nodeTypeString + " nodes: " + modelVerEdgeCount +"\n");
        logger.info(MIGRATION_SUMMARY_COUNT+"Number of ModelVer edge failed to create the edge from the " + nodeTypeString + " nodes due to error : "+ modelVerEdgeErrorCount  +"\n");


    }
    private String getModelInvariantIdNodeValue(Vertex vertex) {
        String propertyValue = "";
        if(vertex != null && vertex.property(MODELINVARIANTIDLOCAL).isPresent()){
            propertyValue = vertex.value(MODELINVARIANTIDLOCAL).toString();
        }
        return propertyValue;
    }

    private String getModelVersionIdNodeValue(Vertex vertex) {
        String propertyValue = "";
        if(vertex != null && vertex.property(MODELVERSIONIDLOCAL).isPresent()){
            propertyValue = vertex.value(MODELVERSIONIDLOCAL).toString();
        }
        return propertyValue;
    }

    @Override
    public Status getStatus() {
        if (success) {
            return Status.SUCCESS;
        } else {
            return Status.FAILURE;
        }
    }

    @Override
    public Optional<String[]> getAffectedNodeTypes() {
        return Optional.of(new String[]{MODELVER});
    }

    @Override
    public String getMigrationName() {
        return "MigrateModelVer";
    }

}
