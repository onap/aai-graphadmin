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
import java.util.ArrayList;
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
//@Enabled
public class MigrateNetworkTechToCloudRegion extends Migrator{

    protected static final String CLOUDREGION_NODETYPE = "cloud-region";
    protected static final String CLOUD_OWNER = "cloud-owner";
    protected static final String NETWORK_TECHNOLOGY_NODETYPE = "network-technology";
    protected static final String NETWORK_TECHNOLOGY_ID = "network-technology-id";
    protected static final String NETWORK_TECHNOLOGY_NAME = "network-technology-name";
    

    private boolean success = true;
    
    private static List<String> dmaapMsgList = new ArrayList<String>();
    
     
    public MigrateNetworkTechToCloudRegion(TransactionalGraphEngine engine, LoaderFactory loaderFactory, EdgeIngestor edgeIngestor, EdgeSerializer edgeSerializer, SchemaVersions schemaVersions) {
        super(engine, loaderFactory, edgeIngestor, edgeSerializer, schemaVersions);
    }

    @Override
    public void run() {

    	List<Vertex> cloudRegionVertextList = this.engine.asAdmin().getTraversalSource().V().has(AAIProperties.NODE_TYPE, CLOUDREGION_NODETYPE).has(CLOUD_OWNER,"att-aic").toList();
    	logger.info("Number of cloud-region with cloud-owner att-aic : " + cloudRegionVertextList.size());
    	createEdges(cloudRegionVertextList, "CONTRAIL");
        createEdges(cloudRegionVertextList, "AIC_SR_IOV");
        
        cloudRegionVertextList = this.engine.asAdmin().getTraversalSource().V().has(AAIProperties.NODE_TYPE, CLOUDREGION_NODETYPE).has(CLOUD_OWNER,"att-nc").toList();
        logger.info("Number of cloud-region with cloud-owner att-nc : " + cloudRegionVertextList.size());
    	createEdges(cloudRegionVertextList, "OVS");
        createEdges(cloudRegionVertextList, "STANDARD-SR-IOV");

        }

    private void createEdges(List<Vertex> sourceVertexList, String networkTechName)
	{
		int networkTechEdgeCount = 0;
		int networkTechEdgeErrorCount = 0;

		List<Vertex> networkTechVertexList = this.engine.asAdmin().getTraversalSource().V()
				.has(AAIProperties.NODE_TYPE, NETWORK_TECHNOLOGY_NODETYPE).has(NETWORK_TECHNOLOGY_NAME, networkTechName)
				.toList();
		
		logger.info("---------- Start Creating an Edge from cloud-region to network-technology nodes with network-technology-name " + networkTechName + "  ----------");

		for (Vertex cloudRegionVertex : sourceVertexList) {

			try {

				for (Vertex networkVertex : networkTechVertexList) {
					if (networkVertex != null) {
						boolean edgePresent = false;
						// Check if edge already exists for each of the source vertex
						List<Vertex> outVertexList = this.engine.asAdmin().getTraversalSource().V(cloudRegionVertex)
								.out().has(AAIProperties.NODE_TYPE, NETWORK_TECHNOLOGY_NODETYPE)
								.has(NETWORK_TECHNOLOGY_NAME, networkTechName).has(NETWORK_TECHNOLOGY_ID,
										networkVertex.property(NETWORK_TECHNOLOGY_ID).value().toString())
								.toList();
						Iterator<Vertex> vertexItr = outVertexList.iterator();
						if (outVertexList != null && !outVertexList.isEmpty() && vertexItr.hasNext()) {
							logger.info("\t Edge already exists from " + CLOUDREGION_NODETYPE + " with " + CLOUD_OWNER
									+ " and cloud-region-id "
									+ cloudRegionVertex.property("cloud-region-id").value().toString() + " to "
									+ NETWORK_TECHNOLOGY_NODETYPE + " nodes with " + NETWORK_TECHNOLOGY_NAME + " "
									+ networkTechName);
							edgePresent = true;
							continue;
						}
						// Build edge from vertex to modelVerVertex
						if (!edgePresent) {
							this.createCousinEdge(cloudRegionVertex, networkVertex);
							updateDmaapList(cloudRegionVertex);
							networkTechEdgeCount++;
						}
					} else {
						networkTechEdgeErrorCount++;
						logger.info("\t" + MIGRATION_ERROR + "Unable to create edge from " + CLOUDREGION_NODETYPE
								+ " with " + CLOUD_OWNER + " to " + NETWORK_TECHNOLOGY_NODETYPE + " nodes with "
								+ NETWORK_TECHNOLOGY_NAME + " " + networkTechName);

					}
				}
			} catch (Exception e) {
				success = false;
				networkTechEdgeErrorCount++;
				logger.error("\t" + MIGRATION_ERROR + "encountered exception from " + NETWORK_TECHNOLOGY_NODETYPE
						+ " node when trying to create edge to " + CLOUDREGION_NODETYPE, e);
			}
		}

		logger.info("\n \n ******* Summary " + NETWORK_TECHNOLOGY_NODETYPE + " Nodes: Finished creating an Edge from "
				+ CLOUDREGION_NODETYPE + " with " + CLOUD_OWNER + " to " + NETWORK_TECHNOLOGY_NODETYPE + " nodes with "
				+ NETWORK_TECHNOLOGY_NAME + " " + networkTechName + "  ********* \n");
		logger.info(MIGRATION_SUMMARY_COUNT + "Number of edges created from cloud-region to "+networkTechName +" network-technology : " + networkTechEdgeCount + "\n");
		logger.info(MIGRATION_SUMMARY_COUNT + "Number of edges failed from cloud-region to "+networkTechName +" network-technology : " + networkTechEdgeErrorCount + "\n");

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
        return Optional.of(new String[]{NETWORK_TECHNOLOGY_NODETYPE});
    }

    @Override
    public String getMigrationName() {
        return "MigrateNetworkTech";
    }
    
    private void updateDmaapList(Vertex v){
    	String dmaapMsg = System.nanoTime() + "_" + v.id().toString() + "_"	+ v.value("resource-version").toString();
        dmaapMsgList.add(dmaapMsg);
        logger.info("\tAdding Updated "+ CLOUDREGION_NODETYPE +" Vertex " + v.id().toString() + " to dmaapMsgList....");
    }
	
    @Override
	public void commit() {
		engine.commit();
		createDmaapFiles(dmaapMsgList);
	}

}
