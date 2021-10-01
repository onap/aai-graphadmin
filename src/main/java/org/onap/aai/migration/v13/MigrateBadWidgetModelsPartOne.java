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
import org.apache.tinkerpop.gremlin.structure.Vertex;
import org.javatuples.Pair;
import org.onap.aai.db.props.AAIProperties;
import org.onap.aai.edges.EdgeIngestor;
import org.onap.aai.introspection.LoaderFactory;
import org.onap.aai.migration.*;
import org.onap.aai.serialization.db.EdgeSerializer;
import org.onap.aai.serialization.engines.TransactionalGraphEngine;
import org.onap.aai.setup.SchemaVersions;


//@Enabled
@MigrationPriority(20)
@MigrationDangerRating(100)
public class MigrateBadWidgetModelsPartOne extends EdgeSwingMigrator {
	private boolean success = true;
	private final GraphTraversalSource g;
	private int candidateCount = 0;
	private int nqEdgeCount = 0;
	
	// migration restrictions that we will use for this migration
	private final String NODE_TYPE_RESTRICTION = "named-query-element";
	private final String EDGE_LABEL_RESTRICTION = "org.onap.relationships.inventory.IsA";
	private final String EDGE_DIR_RESTRICTION = "IN";
	
	GraphTraversal<Vertex, Vertex> widgetModelTraversal;
	GraphTraversal<Vertex, Vertex> widgetModelVersionTraversal;
	GraphTraversal<Vertex, Vertex> validModVerTraversal;
	GraphTraversal<Vertex, Vertex> widgetModelNqEdgeTraversal;



	public MigrateBadWidgetModelsPartOne(TransactionalGraphEngine engine, LoaderFactory loaderFactory, EdgeIngestor edgeIngestor, EdgeSerializer edgeSerializer, SchemaVersions schemaVersions) {
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
		return Optional.of(new String[]{"model", "named-query-element"});
	}

	@Override
	public String getMigrationName() {
		return "MigrateBadWidgetModelsPartOne";
	}
	
	
	/**
	 * Get the List of node pairs("from" and "to"), you would like EdgeSwingMigrator to migrate.
	 * @return
	 */
	@Override
	public List<Pair<Vertex, Vertex>> getAffectedNodePairs() {
		
		List<Pair<Vertex, Vertex>> fromToVertPairList = new ArrayList<Pair<Vertex, Vertex>>();
		ArrayList <Vertex> badModVtxList = new <Vertex> ArrayList ();
		
		logAndPrintInfo("--------- GET AFFECTED NODE PAIRS -------------");
		// Read the json file to populate the validWidgetModelVesionIdHash and also
		// validWidgetModelInvIdHash which will be used to figure out which data is in the db with
		// an invalid id.
		ArrayList <String> fileLines = readInValidWidgetInfoFile();
		
		// validWidgetModelVersionIdHash:  key = nodeType, value = validModelVersionId for that nodeType
		//       Note - we currently only have one valid version per model for widget models.
		HashMap <String,String> validModelInvariantIdHash = getModelInvariantIdHash( fileLines );
		
		// See what (widget) models are being used in the DB
		widgetModelTraversal = this.engine.asAdmin().getTraversalSource().V()
			.has("aai-node-type", "model")
			.has("model-type", "widget");

		if(!(widgetModelTraversal.hasNext())){
			logAndPrintInfo("unable to find widget models in database. ");
		}
		
		while (widgetModelTraversal.hasNext()) {
			Vertex widgetModVertexInDb = widgetModelTraversal.next();
			String invId = widgetModVertexInDb.property("model-invariant-id").value().toString();
			if( validModelInvariantIdHash.containsValue(invId) ){
				// This is a valid model, we don't need to do anything with it.
				continue;
			}
			// For this bad widget model, need to look at the model-version node to
			//   find out what type of widget it is supposed to be so we can look up the correct invId.  
			// Note - We expect just one per model, but there could be more.
			logAndPrintInfo(" Found invalid widget model-invariant-id = [" + invId + "].");
			
			// We're using badModIdList to help us figure out how many bad edges go with the 
			//  bad model nodes - which is really just for logging purposes.
			badModVtxList.add(widgetModVertexInDb);
			
			widgetModelVersionTraversal = this.engine.asAdmin().getTraversalSource()
                    .V(widgetModVertexInDb)
                    .in("org.onap.relationships.inventory.BelongsTo")
                    .has("aai-node-type", "model-ver");
			
			if(!(widgetModelVersionTraversal.hasNext())){
				logAndPrintInfo("unable to find widget model version in database for model-invariant-id = [" + invId + "].");
			}

			while (widgetModelVersionTraversal.hasNext()) {
				Vertex widgetModVersionVertex = widgetModelVersionTraversal.next();
				String nodeType = widgetModVersionVertex.property("model-name").value().toString();
				logAndPrintInfo(" nodeType that goes with invalid widget model-invariant-id = [" + invId + "] is: [" + nodeType + "].");
				
				// Now we can use the nodeType to find the correct/valid model-invariant-id to use
				if( validModelInvariantIdHash.containsKey(nodeType) ){
					// We know what the model-invariant-id SHOULD be, so swing edges from the invalid node to this valid one.
					String validModInvId = validModelInvariantIdHash.get(nodeType);
					Iterator<Vertex> toVtxItr= 
							this.g.V().has("model-invariant-id",validModInvId).has(AAIProperties.NODE_TYPE, "model");
					int ct = 0;
					while(toVtxItr.hasNext()) {
						Vertex toValidVert = toVtxItr.next();
						ct++;
						if( ct == 1 ){
							fromToVertPairList.add(new Pair<>(widgetModVertexInDb, toValidVert));
						}
						else {
							logAndPrintInfo("ERROR - More than one model node found for model-invariant-id = [" + validModInvId + "].");
						}
					}
					if( ct == 0 ){
						logAndPrintInfo("unable to find model node in database for valid model-invariant-id = [" + validModInvId + "].");
					}
				}
				else {
					logAndPrintInfo("unable to find a valid widget model in database for model-name = [" + nodeType + "].");
				}
			}
		}
		candidateCount = fromToVertPairList.size();
		
		// For each of the bad model nodes, see how many actually have an IN edge from a named-query-element
		for( int i = 0; i < badModVtxList.size(); i++ ){
			widgetModelNqEdgeTraversal = this.engine.asAdmin().getTraversalSource()
				.V(badModVtxList.get(i))
                .in("org.onap.relationships.inventory.IsA")
                .has("aai-node-type", "named-query-element");
			
			if(widgetModelNqEdgeTraversal.hasNext()) {
				nqEdgeCount++;
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
		
		// Cleanup of model nodes will be done by the other migration script after the 
		// model-ver records have edges swung off of them.
		
		// We're just going to give count of how many of these edges were found.
		logAndPrintInfo(" >>>> SUMMARY for Migration of named-query-element to model edges: ");
		logAndPrintInfo(" >>>>    Count of bad widget model nodes found: " + candidateCount );
		logAndPrintInfo(" >>>>    Count of bad widget model nodes that have named-query-element edges: " + nqEdgeCount );
		
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