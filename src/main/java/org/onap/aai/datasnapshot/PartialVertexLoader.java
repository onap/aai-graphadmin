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
package org.onap.aai.datasnapshot;

import java.io.BufferedReader;
import java.io.FileReader;
import java.util.HashMap;
import java.util.concurrent.Callable;
import org.apache.tinkerpop.gremlin.structure.Vertex;
import org.apache.tinkerpop.gremlin.structure.io.graphson.GraphSONReader;
import org.janusgraph.core.JanusGraph;

import com.att.eelf.configuration.EELFLogger;
import com.google.gson.JsonObject;
import com.google.gson.JsonParser;



public class PartialVertexLoader implements Callable<HashMap<String,String>>{
	
	private EELFLogger LOGGER;

	private JanusGraph jg;
	private String fName;
	private Long vertAddDelayMs;
	private Long failurePauseMs;
	private Long retryDelayMs;
	private int maxAllowedErrors;
		
	public PartialVertexLoader (JanusGraph graph, String fn, Long vertDelay, Long failurePause, 
			Long retryDelay, int maxErrors, EELFLogger elfLog ){
		jg = graph;
		fName = fn;
		vertAddDelayMs = vertDelay;
		failurePauseMs = failurePause;
		retryDelayMs = retryDelay;
		maxAllowedErrors = maxErrors;
		LOGGER = elfLog;
	}
		
	public HashMap<String,String> call() throws Exception  {  
	
		// NOTE - we will be loading one node at a time so that bad nodes can be ignored instead of causing the
		//   entire load to fail.   
		//
		int entryCount = 0;
		int retryCount = 0;
		int failureCount = 0;
		int retryFailureCount = 0;
		HashMap <String,String> failedAttemptHash = new HashMap <String,String> ();
		HashMap <String,String> old2NewVtxIdHash = new HashMap <String,String> ();
	
		// Read this file into a JSON object
		JsonParser parser = new JsonParser();
		
		try( BufferedReader br = new BufferedReader(new FileReader(fName))) {
			// loop through the file lines and do PUT for each vertex or the edges depending on what the loadtype is
       		for(String line; (line = br.readLine()) != null; ) {
       			entryCount++;
       			Object ob = parser.parse(line);
       			JsonObject jObj = (JsonObject) ob;
       			// NOTE - we will need to keep track of how the newly generated vid's map
        		//    to the old ones so we can aim the edges correctly later.
        			
        		// ----  Note -- This ONLY loads the vertexId and the label for each vertex -------------
       			Thread.sleep(vertAddDelayMs); 
        				
        		String oldVtxIdStr = jObj.get("id").getAsString();
        		String vtxLabelStr = jObj.get("label").getAsString();
	        	try { 
        			Vertex tmpV = jg.addVertex(vtxLabelStr);
       				String newVtxIdStr = tmpV.id().toString();
       				old2NewVtxIdHash.put(oldVtxIdStr,  newVtxIdStr);
       			}
       			catch ( Exception e ){
       				failureCount++;
       				Thread.sleep(failurePauseMs); // Slow down if things are failing
       				LOGGER.debug(" >> addVertex FAILED for vtxId = " + oldVtxIdStr + ", label = [" 
       						+ vtxLabelStr + "].  ErrorMsg = [" + e.getMessage() + "]" );
        			//e.printStackTrace();
        			failedAttemptHash.put(oldVtxIdStr, vtxLabelStr);
        			if( failureCount > maxAllowedErrors ) {
        				LOGGER.debug(" >>> Abandoning PartialVertexLoader() because " +
               					"Max Allowed Error count was exceeded for this thread. (max = " + 
               					maxAllowedErrors + ". ");
        				throw new Exception(" ERROR - Max Allowed Error count exceeded for this thread. (max = " + maxAllowedErrors + ". ");
        			}
        			else {
        				continue;
        			}
        		}
        		try { 
	       			jg.tx().commit();
	       		}
	       		catch ( Exception e ){
	       			failureCount++;
	       			Thread.sleep(failurePauseMs); // Slow down if things are failing
	       			LOGGER.debug(" -- COMMIT FAILED for Vtx ADD for vtxId = " + oldVtxIdStr + ", label = [" 
	       					+ vtxLabelStr + "].  ErrorMsg = [" +e.getMessage() + "]" );
        			//e.printStackTrace();	        				
        			failedAttemptHash.put(oldVtxIdStr, vtxLabelStr);
        			if( failureCount > maxAllowedErrors ) {
        				LOGGER.debug(">>> Abandoning PartialVertexLoader() because " +
               					"Max Allowed Error count was exceeded for this thread. (max = " + 
               					maxAllowedErrors + ". ");
        				throw new Exception(" ERROR - Max Allowed Error count exceeded for this thread. (max = " + maxAllowedErrors + ". ");
        			}
        			else {
        				continue;
        			}
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
	        	e.printStackTrace();
	            throw e;
		}	
        		
		// ---------------------------------------------------------------------------
       	// Now Re-Try any failed requests that might have Failed on the first pass.
       	// ---------------------------------------------------------------------------
       	try {
        	for (String failedVidStr : failedAttemptHash.keySet()) {
    			// Take a little nap, and retry this failed attempt
        		LOGGER.debug("DEBUG >> We will sleep for " + retryDelayMs + " and then RETRY any failed vertex ADDs. ");
    			Thread.sleep(retryDelayMs);
    			
    			retryCount++;
    			// When a vertex Add fails we store the label as the data in the failedAttemptHash.
    			String failedLabel = failedAttemptHash.get(failedVidStr);
    			LOGGER.debug("DEBUG >> RETRY << " +
    					failedVidStr + ", label = " + failedLabel );
    			try {
    				Vertex tmpV = jg.addVertex(failedLabel);
    				String newVtxIdStr = tmpV.id().toString();
    	      		old2NewVtxIdHash.put(failedVidStr, newVtxIdStr);
    			}
    			catch ( Exception e ){
    				retryFailureCount++;
    				LOGGER.debug(" -- addVertex FAILED for RETRY for vtxId = " +
    						failedVidStr + ", label = [" + failedLabel + 
    						"].  ErrorMsg = [" +e.getMessage() + "]" );
    				e.printStackTrace();
        			if( retryFailureCount > maxAllowedErrors ) {
        				LOGGER.debug(">>> Abandoning PartialVertexLoader() because " +
               					"Max Allowed Error count was exceeded for this thread. (max = " + 
               					maxAllowedErrors + ". ");
        				throw new Exception(" ERROR - Max Allowed Error count exceeded for this thread. (max = " + maxAllowedErrors + ". ");
        			}
        			else {
        				continue;
        			}
    			}
    			try { 
    				jg.tx().commit();
	       			LOGGER.debug(" -- addVertex Successful RETRY for vtxId = " +
    						failedVidStr + ", label = [" + failedLabel + "]");
    	       	}
    			catch ( Exception e ){
    				retryFailureCount++;
    				// Note - this is a "POSSIBLE" error because the reason the commit fails may be that
    				//    the node is a dupe or has some other legit reason that it should not be in the DB.
    				LOGGER.debug(" --POSSIBLE ERROR-- COMMIT FAILED for RETRY for vtxId = " + failedVidStr 
    						+ ", label = [" + failedLabel + "].  ErrorMsg = [" + e.getMessage() 
    						+ "].  This vertex will not be tried again. ");

   					e.printStackTrace();
        			if( retryFailureCount > maxAllowedErrors ) {
        				LOGGER.debug(">>> Abandoning PartialVertexLoader() because " +
               					"Max Allowed Error count was exceeded for this thread. (max = " + 
               					maxAllowedErrors + ". ");
        				throw new Exception(" ERROR - Max Allowed Error count exceeded for this thread. (max = " + maxAllowedErrors + ". ");
        			}
        			else {
        				continue;
        			}
   				}
    		} // End of looping over failed attempt hash and doing retries	
        		
		}		
        catch ( Exception e ){
			LOGGER.debug(" -- error in RETRY block. ErrorMsg = [" +e.getMessage() + "]" );
			e.printStackTrace();
			throw e;	
        }
			
        // This would need to be properly logged...		
       	LOGGER.debug(">>> After Processing in PartialVertexLoader():  " + 
				entryCount + " records processed.  " + failureCount + " records failed. " +
				retryCount + " RETRYs processed.  " + retryFailureCount + " RETRYs failed. ");
        		
        return old2NewVtxIdHash;
	        
	}// end of call()  
	
	
		
}	     
	

