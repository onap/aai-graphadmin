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

import java.io.BufferedReader;
import java.io.FileNotFoundException;
import java.io.FileReader;
import java.io.IOException;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.Iterator;
import java.util.List;
import java.util.Optional;
import org.apache.tinkerpop.gremlin.process.traversal.dsl.graph.GraphTraversal;
import org.apache.tinkerpop.gremlin.process.traversal.dsl.graph.GraphTraversalSource;
import org.apache.tinkerpop.gremlin.structure.Direction;
import org.apache.tinkerpop.gremlin.structure.Edge;
import org.apache.tinkerpop.gremlin.structure.Vertex;
import org.javatuples.Pair;
import org.onap.aai.db.props.AAIProperties;
import org.onap.aai.edges.EdgeIngestor;
import org.onap.aai.introspection.LoaderFactory;
import org.onap.aai.migration.EdgeSwingMigrator;
import org.onap.aai.migration.MigrationDangerRating;
import org.onap.aai.migration.MigrationPriority;
import org.onap.aai.migration.Status;
import org.onap.aai.serialization.db.EdgeSerializer;
import org.onap.aai.serialization.engines.TransactionalGraphEngine;
import org.onap.aai.setup.SchemaVersions;


//@Enabled
@MigrationPriority(21)
@MigrationDangerRating(100)
public class MigrateBadWidgetModelsPartTwo extends EdgeSwingMigrator {
	private boolean success = true;
	private final GraphTraversalSource g;
	
	// NOTE -- this migration is for "model-ver" nodes only.  It needs to be run AFTER 
	//   the MigrateWidgetModelsPartOne.
	//  
	
	// migration restrictions that we will use for this migration
	private final String NODE_TYPE_RESTRICTION = "model-element";
	private final String EDGE_LABEL_RESTRICTION = "org.onap.relationships.inventory.IsA";
	private final String EDGE_DIR_RESTRICTION = "IN";
	
	GraphTraversal<Vertex, Vertex> widgetModelTraversal;
	GraphTraversal<Vertex, Vertex> widgetModelVersionTraversal;
	GraphTraversal<Vertex, Vertex> validModVerTraversal;



	public MigrateBadWidgetModelsPartTwo(TransactionalGraphEngine engine, LoaderFactory loaderFactory, EdgeIngestor edgeIngestor, EdgeSerializer edgeSerializer, SchemaVersions schemaVersions) {
		super(engine, loaderFactory, edgeIngestor, edgeSerializer, schemaVersions);
		this.g = this.engine.asAdmin().getTraversalSource();
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
		return Optional.of(new String[]{"model", "model-element", "model-ver"});
	}

	@Override
	public String getMigrationName() {
		return "MigrateBadWidgetModelsPartTwo";
	}
	
	
	/**
	 * Get the List of node pairs("from" and "to"), you would like EdgeSwingMigrator to migrate.
	 * @return
	 */
	@Override
	public List<Pair<Vertex, Vertex>> getAffectedNodePairs() {
		logAndPrintInfo("--------- GET AFFECTED NODE PAIRS -------------");
		// Read the json file to populate the validWidgetModelVesionIdHash and also
		// validWidgetModelInvIdHash which will be used to figure out which data is in the db with
		// an invalid id.
		ArrayList <String> fileLines = readInValidWidgetInfoFile();
		
		// validWidgetModelVersionIdHash:  key = nodeType, value = validModelVersionId for that nodeType
		//       Note - we currently only have one valid version per model for widget models.
		HashMap <String,String> validModelVersionIdHash = getModelVersionIdHash( fileLines ); 
		
		// validWidgetModelVersionIdHash:  key = nodeType, value = validModelVersionId for that nodeType
		//       Note - we currently only have one valid version per model for widget models.
		HashMap <String,String> validModelInvariantIdHash = getModelInvariantIdHash( fileLines );
		
		// Now we will see what is actually in the DB
		List<Pair<Vertex, Vertex>> fromToVertPairList = new ArrayList<Pair<Vertex, Vertex>>();
		widgetModelTraversal = this.engine.asAdmin().getTraversalSource().V()
			.has("aai-node-type", "model")
			.has("model-type", "widget");

		if(!(widgetModelTraversal.hasNext())){
			logAndPrintInfo("unable to find widget models in database. ");
		}
		
		int validModelVerCount = 0;
		while (widgetModelTraversal.hasNext()) {
			Vertex widgetModVertex = widgetModelTraversal.next();
			String invId = widgetModVertex.property("model-invariant-id").value().toString();
			
			// Find the model-version nodes that belong to this model.
			// We expect just one per model, but there could be more.
			widgetModelVersionTraversal = this.engine.asAdmin().getTraversalSource()
                    .V(widgetModVertex)
                    .in("org.onap.relationships.inventory.BelongsTo")
                    .has("aai-node-type", "model-ver");
			
			if(!(widgetModelVersionTraversal.hasNext())){
				logAndPrintInfo("unable to find widget model version in database for model-invariant-id = [" + invId + "].");
			}

			while (widgetModelVersionTraversal.hasNext()) {
				Vertex widgetModVersionVertex = widgetModelVersionTraversal.next();
				String modVersionIdInDb = widgetModVersionVertex.property("model-version-id").value().toString();
				String nodeType = widgetModVersionVertex.property("model-name").value().toString();
				
				if( validModelVersionIdHash.containsKey(nodeType) ){
					// We know what the model-version-id SHOULD be, so make sure we're using it.
					String validModVerId = validModelVersionIdHash.get(nodeType);
					if( !modVersionIdInDb.equals(validModVerId) ){
						logAndPrintInfo(" Bad model-version-id found in DB for model-name = " + nodeType + ", verId = " + modVersionIdInDb );
						validModVerTraversal = this.engine.asAdmin().getTraversalSource()
								.V()
								.has("model-version-id",validModVerId)
								.has("aai-node-type","model-ver");
			            if(!(validModVerTraversal.hasNext())){
							logAndPrintInfo("unable to find widget model version in database for valid model-version-id = [" + validModVerId + "].");
						}
			            int ct = 0;
			            while (validModVerTraversal.hasNext()) {
			            	ct++;
							if( ct > 1 ){
								logAndPrintInfo("ERROR - More than one model-ver found for model-version-id = [" + validModVerId + "].");
								break;
							}
							Vertex toVert = validModVerTraversal.next();
							fromToVertPairList.add(new Pair<>(widgetModVersionVertex, toVert));
			            }
					}
					else {
						validModelVerCount++;
						logAndPrintInfo("Valid model-version-id used in DB for model-name = [" + nodeType + "].");
					}
				}
				else {
					logAndPrintInfo("unable to find a valid widget model-ver in database for model-name = [" + nodeType + "].");
				}
			}
		}
				
		return fromToVertPairList;
	}
	
	
	public String getNodeTypeRestriction(){
		return NODE_TYPE_RESTRICTION;
	}

	public String getEdgeLabelRestriction(){
		return EDGE_LABEL_RESTRICTION;
	}
	
	public String getEdgeDirRestriction(){
		return EDGE_DIR_RESTRICTION;
	}
	
	/**
	 * Get the List of node pairs("from" and "to"), you would like EdgeSwingMigrator to migrate.
	 * @return
	 */
	public void cleanupAsAppropriate(List<Pair<Vertex, Vertex>> nodePairL) {
		
		// The first node in each pair is the model-ver that we were migrating edges AWAY FROM because
		//    it is an invalid model-ver node.
		// Delete those as well as their parent model node (if the parent model node has no other users
		//    and is not on the validModelInvIdList).
		
		int badModelVerCount = 0;
		int modelVerDelCount = 0;
		int modelDelCount = 0;
		int parentPreventValidDelCount = 0;
		
		HashMap <String,String> parentPreventInEdgeIdHash = new HashMap <String,String> (); // using a hash so we can count the # of models, not edges to it.
		HashMap <String,String> parentPreventOutEdgeIdHash = new HashMap <String,String> (); // using a hash so we can count the # of models, not edges to it.
		HashMap <String,String> parentPreventIsaEdgeDelHash = new HashMap <String,String> (); // using a hash so we can count the # of models, not edges to it.
		
		ArrayList <String> fileLines = readInValidWidgetInfoFile();
		// validWidgetModelVersionIdHash:  key = nodeType, value = validModelVersionId for that nodeType
		//       Note - we currently only have one valid version per model for widget models.
		HashMap <String,String> validModelInvariantIdHash = getModelInvariantIdHash( fileLines );
		
		try {
			for (Pair<Vertex, Vertex> nodePair : nodePairL) {
				// The "fromNode" is the "bad/old" model-ver node that we moved off of
				badModelVerCount++;
				Vertex oldNode = nodePair.getValue0();  
				String oldModVerId = oldNode.property("model-version-id").value().toString();
				Vertex parentModelNode = null;
				
				//DOUBLE CHECK THAT THIS IS NOT a valid model-version-id
				
				
				boolean okToDelete = true;
				//---- delete the oldNode if the only edge it has is its "belongsTo/OUT" edge to its parent model.
				//     AND if its parent node does not have any named-query edges ("IsA" edges) pointing to it.
				Iterator <Edge> edgeInIter = oldNode.edges(Direction.IN);
				while( edgeInIter.hasNext() ){
					Edge inE = edgeInIter.next();
					Vertex otherSideNode4ThisEdge = inE.inVertex();
					String otherSideNodeType = otherSideNode4ThisEdge.value(AAIProperties.NODE_TYPE);
					// If there are any IN edges, we won't delete this thing.
					okToDelete = false;
					logAndPrintInfo("We will not delete old model-ver node because it still has IN edges. This model-version-id = [" 
							+ oldModVerId + "], has IN edge from a [" + otherSideNodeType + "] node. ");
				}
				if( okToDelete ){
					// there were no OUT edges, make sure the only OUT edge is to it's parent
					Iterator <Edge> edgeOutIter = oldNode.edges(Direction.OUT);
					int edgeCount = 0;
					while( edgeOutIter.hasNext() ){
						Edge badModVerE = edgeOutIter.next();
						edgeCount++;
						if( edgeCount > 1 ){
							// If there are more than one OUT edges, we won't delete this thing.
							okToDelete = false;
							parentModelNode = null;
							logAndPrintInfo("We will not delete old model-ver node because it still has > 1 OUT-edges.  model-version-id = [" + oldModVerId + "].");
						}
						else {
							String eLabel = badModVerE.label().toString();
							Vertex otherSideNode4ThisEdge = badModVerE.inVertex();
							String otherSideNodeType = otherSideNode4ThisEdge.value(AAIProperties.NODE_TYPE);
							if( ! eLabel.equals("org.onap.relationships.inventory.BelongsTo") ){
								logAndPrintInfo("We will not delete old model-ver node because it still has a non 'belongsTo' OUT-edge.  model-version-id = [" 
										+ oldModVerId + "], edgeLabel = [" + eLabel + "] edge goes to a [" + otherSideNodeType + "]. ");
								okToDelete = false;
							}
							else {
								if( ! otherSideNodeType.equals("model") ){
									logAndPrintInfo("We will not delete old model-ver node (model-version-id = [" + oldModVerId + "]) "
										+ " because it still has an OUT edge to a [" + otherSideNodeType + "] node. ");
									okToDelete = false;
									parentModelNode = null;
								}
								else {
									parentModelNode = otherSideNode4ThisEdge;
									String parentInvId = parentModelNode.property("model-invariant-id").value().toString();
									Iterator <Edge> pInIter = parentModelNode.edges(Direction.IN);
									while( pInIter.hasNext() ){
										Edge inE = pInIter.next();
										String inELabel = inE.label().toString();
										if( ! inELabel.equals("org.onap.relationships.inventory.BelongsTo") ){
											Vertex otherSideNode = inE.outVertex();
											String otherSideNT = otherSideNode.value(AAIProperties.NODE_TYPE);
											// If there are any IN edges still on the parent,
											//   we won't delete this model-ver since once the model-ver
											//   is gone, its hard to know what nodeType the model was
											//   for - so it would be hard to know what valid model-invariant-id
											//   to migrate its edges to.
											okToDelete = false;
											parentPreventIsaEdgeDelHash.put(parentInvId,"");
											logAndPrintInfo("We will not delete old model-ver node because its"
													+ " parent model still has IN edges. The model with model-invariant-id = [" 
													+ parentInvId + "], has an non-belongsTo IN edge, label = [" 
													+ inELabel + "] from a [" + otherSideNT + "] node. ");
										}
									}
								}
							}
						}
					}
				}
						
				if( okToDelete ){
					logAndPrintInfo(" >>> DELETEING model-ver node with model-version-id = [" + oldModVerId + "]" );
					modelVerDelCount++;
					oldNode.remove();
				}
				
				if( parentModelNode != null && okToDelete ){
					// Delete the corresponding parent model IF it now has no 
					//     edges anymore (and is not in our known valid model list)
					//     and we were deleting the model-ver also.
					boolean okToDelParent = true;
					String parentModInvId = parentModelNode.property("model-invariant-id").value().toString();
					
					if( validModelInvariantIdHash.containsValue(parentModInvId) ){
						okToDelParent = false;
						logAndPrintInfo("We will not delete old model node because it is on our valid widget list. "
								+ " model-invariant-id = [" + parentModInvId + "] ");
						parentPreventValidDelCount++;
					}	
					else {
						Iterator <Edge> pInIter = parentModelNode.edges(Direction.IN);
						while( pInIter.hasNext() ){
							Edge inE = pInIter.next();
							String inELabel = inE.label().toString();
							Vertex otherSideNode4ThisEdge = inE.outVertex();
							String otherSideNodeType = otherSideNode4ThisEdge.value(AAIProperties.NODE_TYPE);
							// If there are any IN edges, we won't delete this thing.
							okToDelParent = false;
							parentPreventInEdgeIdHash.put(parentModInvId, "");
							logAndPrintInfo("We will not delete old model node (yet) because it still has IN edges. This model-invariant-id = [" 
								+ parentModInvId + "], has IN edge, label = [" 
								+ inELabel + "] from a [" + otherSideNodeType + "] node. ");
						}
						Iterator <Edge> pOutIter = parentModelNode.edges(Direction.OUT);
						while( pOutIter.hasNext() ){
							Edge outE = pOutIter.next();
							String outELabel = outE.label().toString();
							Vertex otherSideNode4ThisEdge = outE.inVertex();
							String otherSideNodeType = otherSideNode4ThisEdge.value(AAIProperties.NODE_TYPE);
							// If there are any OUT edges, we won't delete this thing.
							okToDelParent = false;
							parentPreventOutEdgeIdHash.put(parentModInvId, "");
							logAndPrintInfo("We will not delete old model node because it still has OUT edges. This model-invariant-id = [" 
								+ parentModInvId + "], has OUT edge, label = [" 
								+ outELabel + "]  to a [" + otherSideNodeType + "] node. ");
						}
					}
				
					if( okToDelParent ){
						if( parentPreventInEdgeIdHash.containsKey(parentModInvId) ){
							// This parent had been prevented from being deleted until all its 
							// child model-ver's were deleted (it must have had more than one).
							// So we can now remove it from the list of parent guys that
							// could not be deleted.
							parentPreventInEdgeIdHash.remove(parentModInvId);
						}
						logAndPrintInfo(" >>> DELETEING model node which was the parent of model-ver with model-version-id = [" 
								+ oldModVerId + "]. This model-invariant-id = [" + parentModInvId + "]" );
						modelDelCount++;
						parentModelNode.remove();
					}
				}
			}
			
			logAndPrintInfo(" >>> SUMMARY: total number of bad model-ver nodes found = " + badModelVerCount );
			logAndPrintInfo(" >>> SUMMARY: number of model-ver nodes deleted = " + modelVerDelCount );
			logAndPrintInfo(" >>> SUMMARY: number of model nodes deleted = " + modelDelCount );
			logAndPrintInfo(" >>> SUMMARY: number of model-ver nodes not deleted because their PARENT still had IsA edges = " 
					+ parentPreventIsaEdgeDelHash.size() );
			logAndPrintInfo(" >>> SUMMARY: number of model nodes not deleted because they were valid = " 
					+ parentPreventValidDelCount);
			logAndPrintInfo(" >>> SUMMARY: number of model nodes not deleted because they had IN edges = " 
					+ parentPreventInEdgeIdHash.size() );
			logAndPrintInfo(" >>> SUMMARY: number of model nodes not deleted because they had OUT edges = " 
					+ parentPreventOutEdgeIdHash.size() );
			
			
		} catch (Exception e) {
			logger.error("error encountered", e );
			success = false;
		}	
		
	}
	
	private ArrayList <String> readInValidWidgetInfoFile(){
		
		ArrayList <String> fileLines = new ArrayList <String> ();
		String homeDir = System.getProperty("AJSC_HOME");
		String configDir = System.getProperty("BUNDLECONFIG_DIR");
		if (homeDir == null) {
			logAndPrintInfo("ERROR: Could not find sys prop AJSC_HOME");
			success = false;
			return fileLines;
		}
		if (configDir == null) {
			logAndPrintInfo("ERROR: Could not find sys prop BUNDLECONFIG_DIR");
			success = false;
			return fileLines;
		}
		String fileName = homeDir + "/" + configDir + "/" + "migration-input-files/widget-model-migration-data/widget-model-migration-input.csv";
		try (BufferedReader br = new BufferedReader(new FileReader(fileName))) {
			String modelInfoLine;
			while ((modelInfoLine = br.readLine()) != null) {
				modelInfoLine = modelInfoLine.replace("\n", "").replace("\r", "");
				if (!modelInfoLine.isEmpty()) {
					fileLines.add(modelInfoLine);
				}
			}
		} 
		catch (FileNotFoundException e) {
			logger.error("ERROR: Could not find file " + fileName, e);
			success = false;
		} catch (IOException e) {
			logger.error("ERROR: Issue reading file " + fileName, e);
			success = false;
		} catch (Exception e) {
			logger.error("encountered exception", e);
			success = false;
		}
		return fileLines;
	}
	
	
	HashMap <String,String> getModelVersionIdHash( ArrayList <String> fileLines ){
		
		HashMap <String, String> versionIdHash = new HashMap <String,String> ();
		
		if( fileLines == null ){
			logAndPrintInfo("ERROR: null fileLines array passed to getModelVersionIdHash");
			success = false;
			return versionIdHash;
		}
		
		for(int i = 0; i < fileLines.size(); i++ ){
			String mLine = fileLines.get(i);
			String[] fields = mLine.split("\\,");
			if (fields.length != 3) {
				logAndPrintInfo("ERROR: row in data file did not contain 3 elements. should have: model-name,model-version-id,model-invariant-id on each line.");
				success = false;
			}
			else {
				versionIdHash.put(fields[0],fields[1]);
			}
		}
		
		// Because of some bad data in the db, we will manually map the nodeType of "vdc" to what is 
		//   the correct model info for "virtual-data-center".  Problem is that there is no vdc nodeType, but
		//   there are named-queries pointing at a bad widget-model for "vdc".
		String virtDataCenterVerId = versionIdHash.get("virtual-data-center");
		if( virtDataCenterVerId != null ){
			versionIdHash.put("vdc",virtDataCenterVerId );
		}
		
		return versionIdHash;
	}
			
	
	HashMap <String,String> getModelInvariantIdHash( ArrayList <String> fileLines ){
		HashMap <String, String> invIdHash = new HashMap <String,String> ();
		
		if( fileLines == null ){
			logAndPrintInfo("ERROR: null fileLines array passed to getModelVersionIdHash");
			success = false;
			return invIdHash;
		}
		
		for(int i = 0; i < fileLines.size(); i++ ){
			String mLine = fileLines.get(i);
			String[] fields = mLine.split("\\,");
			if (fields.length != 3) {
				logAndPrintInfo("ERROR: row in data file did not contain 3 elements. should have: model-name,model-version-id,model-invariant-id on each line.");
				success = false;
			}
			else {
				invIdHash.put(fields[0],fields[2]);
			}
		}
		
		// Because of some bad data in the db, we will manually map the nodeType of "vdc" to what is 
		//   the correct model info for "virtual-data-center".  Problem is that there is no vdc nodeType, but
		//   there are named-queries pointing at a bad widget-model for "vdc".
		String virtDataCenterInvId = invIdHash.get("virtual-data-center");
		if( invIdHash != null ){
			invIdHash.put("vdc",virtDataCenterInvId );
		}
		return invIdHash;
	}
	
	/**
	 * Log and print.
	 *
	 * @param msg
	 *            the msg
	 */
	protected void logAndPrintInfo(String msg) {
		System.out.println(msg);
		logger.info(msg);
	}

		
			
}