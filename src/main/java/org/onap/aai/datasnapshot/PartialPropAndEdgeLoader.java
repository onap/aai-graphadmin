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
package org.onap.aai.datasnapshot;

import java.io.BufferedReader;
import java.io.FileReader;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.Iterator;
import java.util.concurrent.Callable;
import org.apache.tinkerpop.gremlin.structure.Vertex;
import org.janusgraph.core.JanusGraph;
import org.apache.tinkerpop.gremlin.structure.Edge;
import org.json.JSONArray;
import org.json.JSONObject;

import com.att.eelf.configuration.EELFLogger;




public class PartialPropAndEdgeLoader implements Callable <ArrayList<String>>{
	
	private EELFLogger LOGGER;

	private JanusGraph jg;
	private String fName;
	private Long edgeAddDelayMs;
	private Long retryDelayMs;
	private Long failureDelayMs;
	private HashMap<String,String> old2NewVidMap;
	private int maxAllowedErrors;
	

		
	public PartialPropAndEdgeLoader (JanusGraph graph, String fn, Long edgeDelay, Long failureDelay, Long retryDelay, 
			 HashMap<String,String> vidMap, int maxErrors, EELFLogger elfLog ){
		jg = graph;
		fName = fn;
		edgeAddDelayMs = edgeDelay;
		failureDelayMs = failureDelay;
		retryDelayMs = retryDelay;
		old2NewVidMap = vidMap;
		maxAllowedErrors = maxErrors;
		LOGGER = elfLog;
	}
	
		
	public ArrayList<String> call() throws Exception  {  
	
		// This is a partner to the "PartialVertexLoader" code.  
		// That code loads in vertex-id's/vertex-label's for a 
		// multi-file data snapshot.
		// This code assumes that the all vertex-id's are now in the target db.
		// This code loads vertex properties and edges for a
		// multi-file data snapshot (the same one that loaded
		// the vertex-ids).
		// 
		
		
		// NOTE - We will be loading parameters and edges for one node at a time so that problems can be 
		//   identified or ignored or re-tried instead of causing the entire load to fail.   
		//
		// Return an arrayList of Strings to give info on what nodes encountered problems
		
		int entryCount = 0;
		int retryCount = 0;
		int failureCount = 0;
		int retryFailureCount = 0;
		HashMap <String,String> failedAttemptHash = new HashMap <String,String> ();
		ArrayList <String> failedAttemptInfo = new ArrayList <String> ();
		
		int passNum = 1;
		try( BufferedReader br = new BufferedReader(new FileReader(fName))) {
			// loop through the file lines and do PUT for each vertex or the edges depending on what the loadtype is
       		for(String origLine; (origLine = br.readLine()) != null; ) {
       			entryCount++;
        		Thread.sleep(edgeAddDelayMs);  // Space the edge requests out a little
        		
        		String errInfoStr = processThisLine(origLine, passNum); 
        		if( !errInfoStr.equals("") ){
        			// There was a problem with this line
        			String vidStr = getTheVidForThisLine(origLine);
        			// We'll use the failedAttemptHash to reTry this item
        			failedAttemptHash.put(vidStr,origLine);
        			failedAttemptInfo.add(errInfoStr);
        			failureCount++;
        			if( failureCount > maxAllowedErrors ) {
        				LOGGER.debug(">>> Abandoning PartialPropAndEdgeLoader() because " +
               					"Max Allowed Error count was exceeded for this thread. (max = " + 
               					maxAllowedErrors + ". ");
        				throw new Exception(" ERROR - Max Allowed Error count exceeded for this thread. (max = " + maxAllowedErrors + ". ");
        			}
        			Thread.sleep(failureDelayMs);  // take a little nap if it failed
        		}
        	} // End of looping over each line
       		if( br != null  ){
	       		br.close();
	       	}
       }
		catch (Exception e) {
	       	LOGGER.debug(" --- Failed in the main loop for Buffered-Reader item # " + entryCount +
	       			", fName = " + fName );
	        LOGGER.debug(" --- msg = " + e.getMessage() );
	        throw e;
		}	
		
		// ---------------------------------------------------------------------------
        // Now Re-Try any failed requests that might have Failed on the first pass.
       	// ---------------------------------------------------------------------------
		passNum++;
       	try {
       		for (String failedVidStr : failedAttemptHash.keySet()) {
        		// Take a little nap, and retry this failed attempt
       			LOGGER.debug("DEBUG >> We will sleep for " + retryDelayMs + " and then RETRY any failed edge/property ADDs. ");
    			Thread.sleep(retryDelayMs);
    			retryCount++;
    			Long failedVidL = Long.parseLong(failedVidStr);
    			// When an Edge/Property Add fails, we store the whole (translated) graphSON line as the data in the failedAttemptHash
    	       	// We're really just doing a GET of this one vertex here...
    			String jsonLineToRetry = failedAttemptHash.get(failedVidStr);
    			String errInfoStr = processThisLine(jsonLineToRetry, passNum); 
            	if( !errInfoStr.equals("") ){
            		// There was a problem with this line
            		String translatedVidStr = getTheVidForThisLine(jsonLineToRetry);
            		failedAttemptHash.put(translatedVidStr,jsonLineToRetry);
            		failedAttemptInfo.add(errInfoStr);
            		retryFailureCount++;
           			if( retryFailureCount > maxAllowedErrors ) {
           				LOGGER.debug(">>> Abandoning PartialPropAndEdgeLoader() because " +
           					"Max Allowed Error count was exceeded while doing retries for this thread. (max = " + 
           					maxAllowedErrors + ". ");
        				throw new Exception(" ERROR - Max Allowed Error count exceeded for this thread. (max = " + maxAllowedErrors + ". ");
        			}
            		Thread.sleep(failureDelayMs);  // take a little nap if it failed
           		}
            } // End of looping over each failed line
        }
       	catch (Exception e) {
       		LOGGER.debug(" -- error in RETRY block. ErrorMsg = [" + e.getMessage() + "]" );
       		throw e;
       	}	
     
       	LOGGER.debug(">>> After Processing in PartialPropAndEdgeLoader() " +
			entryCount + " records processed.  " + failureCount + " records failed. " +
			retryCount + " RETRYs processed.  " + retryFailureCount + " RETRYs failed. ");
        		
       	return failedAttemptInfo;
	        
	}// end of call()  

	
	
	private String translateThisVid(String oldVid) throws Exception {
		
		if( old2NewVidMap == null ){
			throw new Exception(" ERROR - null old2NewVidMap found in translateThisVid. ");
		}
		
		if( old2NewVidMap.containsKey(oldVid) ){
			return old2NewVidMap.get(oldVid);
		}
		else {
			throw new Exception(" ERROR - could not find VID translation for original VID = " + oldVid );
		}
	}
	
	
	private String getTheVidForThisLine(String graphSonLine) throws Exception {
		
		if( graphSonLine == null ){
			throw new Exception(" ERROR - null graphSonLine passed to getTheVidForThisLine. ");
		}
		
		// We are assuming that the graphSonLine has the vertexId as the first ID:
		// {"id":100995128,"label":"vertex","inE":{"hasPinterface":[{"id":"7lgg0e-2... etc...
		
		 // The vertexId for this line is the numeric part after the initial {"id":xxxxx  up to the first comma
		int x = graphSonLine.indexOf(':') + 1;
		int y = graphSonLine.indexOf(',');
		String initialVid = graphSonLine.substring(x,y);
		if( initialVid != null && !initialVid.isEmpty() && initialVid.matches("^[0-9]+$") ){
			return initialVid;
		}
		else {
			throw new Exception(" ERROR - could not determine initial VID for graphSonLine: " + graphSonLine );
		}
	}
		
	
	private String processThisLine(String graphSonLine, int passNum){
		
		String passInfo = ""; 
		if( passNum > 1 ) {
			passInfo = " >> RETRY << pass # " + passNum + " ";
		}

		JSONObject jObj = new JSONObject();
		String originalVid = "";
		
		try{
			jObj = new JSONObject(graphSonLine);
			originalVid = jObj.get("id").toString();
		}
		catch ( Exception e ){
    		LOGGER.debug(" -- Could not convert line to JsonObject [ " + graphSonLine + "]" );
    		LOGGER.debug(" -- ErrorMsg = [" +e.getMessage() + "]");
    			
    		return(" DEBUG -a- JSON translation exception when processing this line ---");
    		//xxxxxDEBUGxxxxx I think we put some info on the return String and then return?
    	}
		 	
		// -----------------------------------------------------------------------------------------
		// Note - this assumes that any vertices referred to by an edge will already be in the DB.
		// -----------------------------------------------------------------------------------------
		Vertex dbVtx = null;	
		
		String newVidStr = "";
		Long newVidL = 0L;
		try {
			newVidStr = translateThisVid(originalVid);
			newVidL = Long.parseLong(newVidStr);
		}
		catch ( Exception e ){
    		LOGGER.debug(" -- "  + passInfo + " translate VertexId before adding edges failed for this: vtxId = " 
    				+ originalVid + ".  ErrorMsg = [" +e.getMessage() + "]");
    			
    		return(" DEBUG -b- there VID-translation error when processing this line ---");
    		//xxxxxDEBUGxxxxx I think we put some info on the return String and then return?
    	}
		
		
		try {
			dbVtx = getVertexFromDbForVid(newVidStr);
		}
		catch ( Exception e ){
    		LOGGER.debug(" -- "  + passInfo + " READ Vertex from DB before adding edges failed for this: vtxId = " + originalVid
    				+ ", newVidId = " + newVidL + ".  ErrorMsg = [" +e.getMessage() + "]");
    			
    		return("  --  there was an error processing this line --- Line = [" + graphSonLine + "]");
    		//xxxxxxDEBUGxxxx I think we put some info on the return String and then return?
    	}
			
		
		String edResStr = processEdgesForVtx( jObj, dbVtx, passInfo, originalVid );
		if( edResStr.equals("") ){
			// We will commit the edges by themselves in case the properties stuff below fails
	       	try { 
	       		jg.tx().commit();
			}
			catch ( Exception e ){
				LOGGER.debug(" -- " + passInfo + " COMMIT FAILED adding EDGES for this vertex: vtxId = " 
						+ originalVid + ".  ErrorMsg = [" +e.getMessage() + "]");
				//xxxxxxxxxx I think we put some info on the return String and then return?
	       	    return(" DEBUG -d- there was an error doing the commit while processing edges for this line ---");
			}
		}
		
		// Add the properties that we didn't have when we added the 'bare-bones' vertex
		String pResStr = processPropertiesForVtx( jObj, dbVtx, passInfo, originalVid );
		if( pResStr.equals("") ){
			try { 
	       		jg.tx().commit();
	       		return "";
			}
			catch ( Exception e ){
				LOGGER.debug(" -- " + passInfo + " COMMIT FAILED adding Properties for this vertex: vtxId = " 
						+ originalVid + ".  ErrorMsg = [" +e.getMessage() + "]");
				//xxxxxxxxxx I think we put some info on the return String and then return?
	       	    return(" DEBUG -e- there was an error doing the commit while processing Properties for this line ---");
			}
		}
		else {
			LOGGER.debug("DEBUG " + passInfo + " Error processing Properties for this vertex: vtxId = " + originalVid );
			
			//xxxxxxxxxx I think we put some info on the return String and then return?
       	    return(" DEBUG -f- there was an error while processing Properties for this line ---");
		}
	}
	
	
	private String processPropertiesForVtx( JSONObject jObj, Vertex dbVtx, String passInfo, String originalVid ){
		
		try {
			JSONObject propsOb = (JSONObject) jObj.get("properties");
			Iterator <String> propsItr = propsOb.keys();
			while( propsItr.hasNext() ){
				String pKey = propsItr.next();
				JSONArray propsDetArr = propsOb.getJSONArray(pKey);
				for( int i=0; i< propsDetArr.length(); i++ ){
					JSONObject prop = propsDetArr.getJSONObject(i);
					String val = prop.getString("value");
					dbVtx.property(pKey, val);  //DEBUGjojo -- val is always String here.. which is not right -------------------DEBUG
				}
			}
	
		}
		catch ( Exception e ){
       		LOGGER.debug(" -- " + passInfo + " failure getting/setting properties for: vtxId = " 
       				+ originalVid + ".  ErrorMsg = [" + e.getMessage() + "]");
       		//xxxDEBUGxxxxxxx I think we put some info on the return String and then return?
       	    return(" DEBUG -g- there was an error adding properties while processing this line ---");
       		
       	}
       		
		return "";
	}
	
	
	private Vertex getVertexFromDbForVid( String vtxIdStr ) throws Exception {
		Vertex thisVertex = null;
		Long vtxIdL = 0L;
		
		try {
			vtxIdL = Long.parseLong(vtxIdStr);
			Iterator <Vertex> vItr = jg.vertices(vtxIdL);
			// Note - we only expect to find one vertex found for this ID.
			while( vItr.hasNext() ){
				thisVertex = vItr.next();
			}
		}
		catch ( Exception e ){
			String emsg = "Error finding vertex for vid = " + vtxIdStr + "[" + e.getMessage() + "]";
			throw new Exception ( emsg );
		}
		
		if( thisVertex == null ){
			String emsg = "Could not find vertex for passed vid = " + vtxIdStr;
			throw new Exception ( emsg );
		}
		
		return thisVertex;
	}
	
	
	private String processEdgesForVtx( JSONObject jObj, Vertex dbVtx, String passInfo, String originalVid ){

		// Process the edges for this vertex -- but, just the "OUT" ones so edges don't get added twice (once from
		// each side of the edge).
		JSONObject edOb = null;
		try {
			edOb = (JSONObject) jObj.get("outE");
		}
		catch (Exception e){
			// There were no OUT edges.  This is OK.
			return "";
		}
			
		try {
			if( edOb == null ){
				// There were no OUT edges.  This is OK.  Not all nodes have out edges.
				return "";
			}
			Iterator <String> edItr = edOb.keys();
			while( edItr.hasNext() ){
				String eLabel = edItr.next();
				String inVid = "";   // Note - this should really be a Long?
				JSONArray edArr = edOb.getJSONArray(eLabel);
				for( int i=0; i< edArr.length(); i++ ){
					JSONObject eObj = edArr.getJSONObject(i);
					String inVidStr = eObj.get("inV").toString();
					String translatedInVidStr = translateThisVid(inVidStr);
					Vertex newInVertex = getVertexFromDbForVid(translatedInVidStr);
					
					// Note - addEdge automatically adds the edge in the OUT direction from the 
					//     'anchor' node that the call is being made from.
					Edge tmpE = dbVtx.addEdge(eLabel, newInVertex); 
					JSONObject ePropsOb = null;
					try {
						ePropsOb = (JSONObject) eObj.get("properties");
					}
					catch (Exception e){
						// NOTE - model definition related edges do not have edge properties.  That is OK.
						// Ie. when a model-element node has an "isA" edge to a "model-ver" node, that edge does
						//    not have edge properties on it.
					}
					if( ePropsOb != null ){
						Iterator <String> ePropsItr = ePropsOb.keys();
						while( ePropsItr.hasNext() ){
							String pKey = ePropsItr.next();
							tmpE.property(pKey, ePropsOb.getString(pKey));
						}
					}
				}
			}

		}
		catch ( Exception e ){
			String msg =  " -- " + passInfo + " failure adding edge for: original vtxId = " 
					+ originalVid + ".  ErrorMsg = [" +e.getMessage() + "]";
			LOGGER.debug( " -- " + msg );
			//xxxxxxDEBUGxxxx I think we might need some better info on the return String to return?
			LOGGER.debug(" -- now going to return/bail out of processEdgesForVtx" );
			return(" >> " + msg );
   		
		}
   			
		return "";
	}
	
	
}	     
	

