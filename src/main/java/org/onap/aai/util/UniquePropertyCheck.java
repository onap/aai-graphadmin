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
package org.onap.aai.util;
import java.util.HashMap;
import java.util.Iterator;
import java.util.Map;
import java.util.Properties;
import java.util.UUID;

import org.apache.tinkerpop.gremlin.structure.Direction;
import org.apache.tinkerpop.gremlin.structure.Edge;
import org.apache.tinkerpop.gremlin.structure.Graph;
import org.apache.tinkerpop.gremlin.structure.Vertex;
import org.apache.tinkerpop.gremlin.structure.VertexProperty;
import org.janusgraph.core.JanusGraph;
import org.janusgraph.core.JanusGraphFactory;
import org.onap.aai.aailog.logs.AaiScheduledTaskAuditLog;
import org.onap.aai.dbmap.AAIGraphConfig;
import org.onap.aai.exceptions.AAIException;
import org.onap.logging.filter.base.ONAPComponents;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.slf4j.MDC;

public class UniquePropertyCheck {


	private static 	final  String    FROMAPPID = "AAI-UTILS";
	private static 	final  String    TRANSID   = UUID.randomUUID().toString();
	private static 	final  String    COMPONENT = "UniquePropertyCheck";

	/**
	 * The main method.
	 *
	 * @param args the arguments
	 */
	public static void main(String[] args) {

		Logger logger = LoggerFactory.getLogger(UniquePropertyCheck.class);
		MDC.put("logFilenameAppender", UniquePropertyCheck.class.getSimpleName());
		AaiScheduledTaskAuditLog auditLog = new AaiScheduledTaskAuditLog();
		auditLog.logBefore(COMPONENT, ONAPComponents.AAI.toString());

		if( args == null || args.length != 1 ){
				String msg = "usage:  UniquePropertyCheck propertyName \n";
				System.out.println(msg);
				logAndPrint(logger, msg );
				System.exit(1);
		}
	  	String propertyName = args[0];
	  	Graph graph = null;

		try(JanusGraph tGraph = JanusGraphFactory.open(new AAIGraphConfig.Builder(AAIConstants.REALTIME_DB_CONFIG).forService(UniquePropertyCheck.class.getSimpleName()).withGraphType("realtime").buildConfiguration())) {
    		AAIConfig.init();
    		System.out.println("    ---- NOTE --- about to open graph (takes a little while)--------\n");

    		if( tGraph == null ) {
    			logAndPrint(logger, " Error:  Could not get JanusGraph ");
    			System.exit(1);
    		}

    		graph = tGraph.newTransaction();
    		if( graph == null ){
    			logAndPrint(logger, "could not get graph object in UniquePropertyCheck() \n");
    	 		System.exit(0);
    		}
    	}
	    catch (AAIException e1) {
			String msg =  "Threw Exception: [" + e1.toString() + "]";
			logAndPrint(logger, msg);
			System.exit(0);
        }
        catch (Exception e2) {
	 		String msg =  "Threw Exception: [" + e2.toString() + "]";
			logAndPrint(logger, msg);
	 		System.exit(0);
        }

		runTheCheckForUniqueness( TRANSID, FROMAPPID, graph, propertyName, logger );
		auditLog.logAfter();
		System.exit(0);

	}// End main()


	/**
	 * Run the check for uniqueness.
	 *
	 * @param transId the trans id
	 * @param fromAppId the from app id
	 * @param graph the graph
	 * @param propertyName the property name
	 * @param logger the logger
	 * @return the boolean
	 */
	public static Boolean runTheCheckForUniqueness( String transId, String fromAppId, Graph graph,
			String propertyName, Logger logger ){

		// Note - property can be found in more than one nodetype
		//    our uniqueness constraints are always across the entire db - so this
		//   tool looks across all nodeTypes that the property is found in.
		Boolean foundDupesFlag = false;

		HashMap <String,String> valuesAndVidHash = new HashMap <> ();
		HashMap <String,String> dupeHash = new HashMap <> ();

		int propCount = 0;
		int dupeCount = 0;
		Iterator<Vertex> vertItor = graph.traversal().V().has(propertyName);
       	while( vertItor.hasNext() ){
       		propCount++;
    		Vertex v = vertItor.next();
    		String thisVid = v.id().toString();
    		Object val = (v.<Object>property(propertyName)).orElse(null);
    		if( valuesAndVidHash.containsKey(val) ){
    			// We've seen this one before- track it in our  dupe hash
    			dupeCount++;
    			if( dupeHash.containsKey(val) ){
    				// This is not the first one being added to the dupe hash for this value
    				String updatedDupeList = dupeHash.get(val) + "|" + thisVid;
    				dupeHash.put(val.toString(), updatedDupeList);
    			}
    			else {
    				// This is the first time we see this value repeating
    				String firstTwoVids =  valuesAndVidHash.get(val) + "|" + thisVid;
    				dupeHash.put(val.toString(), firstTwoVids);
    			}
    		}
    		else {
    			valuesAndVidHash.put(val.toString(), thisVid);
    		}
       	}


    	String info = "\n Found this property [" + propertyName + "] " + propCount + " times in our db.";
    	logAndPrint(logger, info);
    	info = " Found " + dupeCount + " cases of duplicate values for this property.\n\n";
    	logAndPrint(logger, info);

    	try {
	    	if( ! dupeHash.isEmpty() ){
	    		Iterator <?> dupeItr = dupeHash.entrySet().iterator();
	    		while( dupeItr.hasNext() ){
	    			foundDupesFlag = true;
	    			Map.Entry pair = (Map.Entry) dupeItr.next();
	    			String dupeValue = pair.getKey().toString();
	    			    			String vidsStr = pair.getValue().toString();
	    			String[] vidArr = vidsStr.split("\\|");
	    			logAndPrint(logger, "\n\n -------------- Found " + vidArr.length
	    					+ " nodes with " + propertyName + " of this value: [" + dupeValue + "].  Node details: ");

	    			for( int i = 0; i < vidArr.length; i++ ){
	    				String vidString = vidArr[i];
	    				Long idLong = Long.valueOf(vidString);
	    				Vertex tvx = graph.traversal().V(idLong).next();
	    				showPropertiesAndEdges( TRANSID, FROMAPPID, tvx, logger );
	    			}
	    		}
	    	}
    	}
    	catch( Exception e2 ){
	 		logAndPrint(logger, "Threw Exception: [" + e2.toString() + "]");
    	}


    	return foundDupesFlag;

	}// end of runTheCheckForUniqueness()


	/**
	 * Show properties and edges.
	 *
	 * @param transId the trans id
	 * @param fromAppId the from app id
	 * @param tVert the t vert
	 * @param logger the logger
	 */
	private static void showPropertiesAndEdges( String transId, String fromAppId, Vertex tVert,
			Logger logger ){

		if( tVert == null ){
			logAndPrint(logger, "Null node passed to showPropertiesAndEdges.");
		}
		else {
			String nodeType = "";
			Object ob = tVert.<String>property("aai-node-type").orElse(null);
			if( ob == null ){
				nodeType = "null";
			}
			else{
				nodeType = ob.toString();
			}

			logAndPrint(logger, " AAINodeType/VtxID for this Node = [" + nodeType + "/" + tVert.id() + "]");
			logAndPrint(logger, " Property Detail: ");
			Iterator<VertexProperty<Object>> pI = tVert.properties();
			while( pI.hasNext() ){
				VertexProperty<Object> tp = pI.next();
				Object val = tp.value();
				logAndPrint(logger, "Prop: [" + tp.key() + "], val = [" + val + "] ");
			}

			Iterator <Edge> eI = tVert.edges(Direction.BOTH);
			if( ! eI.hasNext() ){
				logAndPrint(logger, "No edges were found for this vertex. ");
			}
			while( eI.hasNext() ){
				Edge ed = eI.next();
				String lab = ed.label();
				Vertex vtx;
				if (tVert.equals(ed.inVertex())) {
					vtx = ed.outVertex();
				} else {
					vtx = ed.inVertex();
				}
				if( vtx == null ){
					logAndPrint(logger, " >>> COULD NOT FIND VERTEX on the other side of this edge edgeId = " + ed.id() + " <<< ");
				}
				else {
					String nType = vtx.<String>property("aai-node-type").orElse(null);
					String vid = vtx.id().toString();
					logAndPrint(logger, "Found an edge (" + lab + ") from this vertex to a [" + nType + "] node with VtxId = " + vid);
				}
			}
		}
	} // End of showPropertiesAndEdges()


	/**
	 * Log and print.
	 *
	 * @param logger the logger
	 * @param msg the msg
	 */
	protected static void logAndPrint(Logger logger, String msg) {
		System.out.println(msg);
		logger.info(msg);
	}

}
