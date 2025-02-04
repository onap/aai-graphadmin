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
package org.onap.aai.historytruncate;

import java.util.*;
import java.util.concurrent.TimeUnit;
import java.util.stream.Collectors;

import org.apache.tinkerpop.gremlin.process.traversal.P;
import org.apache.tinkerpop.gremlin.process.traversal.dsl.graph.GraphTraversalSource;
import org.apache.tinkerpop.gremlin.structure.Edge;
import org.apache.tinkerpop.gremlin.structure.Graph;
import org.apache.tinkerpop.gremlin.structure.Property;
import org.apache.tinkerpop.gremlin.structure.Vertex;
import org.apache.tinkerpop.gremlin.structure.VertexProperty;
import org.onap.aai.dbmap.AAIGraph;
import org.onap.aai.exceptions.AAIException;
import org.onap.aai.logging.ErrorLogHelper;
import org.onap.aai.logging.LogFormatTools;
import org.onap.aai.util.AAIConfig;
import org.onap.aai.util.AAISystemExitUtil;
import org.springframework.context.annotation.AnnotationConfigApplicationContext;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.janusgraph.core.JanusGraph;

public class HistoryTruncate {

	private static Logger LOGGER = LoggerFactory.getLogger(HistoryTruncate.class);

	/* Using realtime d */
	private static final String REALTIME_DB = "realtime";

	private static final String LOG_ONLY_MODE = "LOG_ONLY";
	private static final String DELETE_AND_LOG_MODE = "DELETE_AND_LOG";
	private static final String SILENT_DELETE_MODE = "SILENT_DELETE";
	static ArrayList <String> VALIDMODES = new <String> ArrayList ();
	static {
		VALIDMODES.add(LOG_ONLY_MODE);
		VALIDMODES.add(DELETE_AND_LOG_MODE);
		VALIDMODES.add(SILENT_DELETE_MODE);
	}

	private static final int batchCommitSize = 500;

	private static boolean historyEnabled;
	private static String defaultTruncateMode;
	private static Integer defaultTruncateWindowDays;


	/**
	 * The main method.
	 *
	 */
	public static void main(String[] args) {

		try (AnnotationConfigApplicationContext ctx = new AnnotationConfigApplicationContext()) {
			try {
				ctx.refresh();
			} catch (Exception e) {
				LOGGER.error("Error - Could not initialize context beans for HistoryTruncate. ");
				AAISystemExitUtil.systemExitCloseAAIGraph(1);
			}

			historyEnabled = Boolean.parseBoolean(ctx.getEnvironment().getProperty("history.enabled","false"));
			if( !historyEnabled ) {
				String emsg = "Error - HistoryTruncate may only be used when history.enabled=true. ";
				System.out.println(emsg);
				LOGGER.error(emsg);
				AAISystemExitUtil.systemExitCloseAAIGraph(1);
			}

			defaultTruncateWindowDays = Integer.parseInt(ctx.getEnvironment().getProperty("history.truncate.window.days","999"));
			defaultTruncateMode = ctx.getEnvironment().getProperty("history.truncate.mode",LOG_ONLY_MODE);
		}
		HistoryTruncate histTrunc = new HistoryTruncate();
		boolean success = histTrunc.executeCommand(args);
		if(success){
			AAISystemExitUtil.systemExitCloseAAIGraph(0);
		} else {
			AAISystemExitUtil.systemExitCloseAAIGraph(1);
		}

	}// End of main()


	public boolean executeCommand(String[] args) {
		boolean successStatus = true;
		// If they passed in args on the command line, then we should
		// use those in place of the default ones we got from environment variables.
		// "-truncateMode","LOG_ONLY","-truncateWindow","999"
		String truncateMode = defaultTruncateMode;
		int truncateWindowDays = defaultTruncateWindowDays;

		if (args != null && args.length > 0) {
				// They passed some arguments in that will affect processing
				for (int i = 0; i < args.length; i++) {
						String thisArg = args[i];
						if (thisArg.equals("-truncateMode")) {
								i++;
								if (i >= args.length) {
										LOGGER.error(" No value passed with -truncateMode option.  ");
										return false;
								}
								if( !VALIDMODES.contains(args[i]) ) {
										LOGGER.error(" Unrecognized -truncateMode value passed: [" +
											args[i] + "].  Valid values = " + VALIDMODES.toString() );
										return false;
								}
								truncateMode = args[i];
						} else if (thisArg.equals("-truncateWindowDays")) {
								i++;
								if (i >= args.length) {
										LOGGER.error("No value passed with -truncateWindowDays option.");
										return false;
								}
								String nextArg = args[i];
								try {
										truncateWindowDays = Integer.parseInt(nextArg);
								} catch (Exception e) {
										LOGGER.error("Bad value passed with -truncateWindowDays option: ["
														+ nextArg + "]");
										return false;
								}
						} else {
								LOGGER.error(" Unrecognized argument passed to HistoryTruncate: ["
												+ thisArg + "]. ");
								LOGGER.error(" Valid values are: -truncateMode -truncateWindowDays ");
								return false;
						}
				}
		}

		LOGGER.debug(" Running HistoryTruncate with: truncateMode = " + truncateMode +
				", truncateWindowDays = " + truncateWindowDays );

		Long truncateEndTs = calculateTruncWindowEndTimeStamp(truncateWindowDays);
		JanusGraph jgraph = null;
		long scriptStartTime = System.currentTimeMillis();
		Boolean doLogging = doLoggingOrNot( truncateMode );
		Boolean doDelete = doDeleteOrNot( truncateMode );

		try {
			AAIConfig.init();
			ErrorLogHelper.loadProperties();

			LOGGER.debug("    ---- NOTE --- about to open graph (takes a little while) ");
			verifyGraph(AAIGraph.getInstance().getGraph());
			jgraph = AAIGraph.getInstance().getGraph();
			LOGGER.debug(" ---- got the new graph instance. ");

			// Note - process edges first so they get logged as they are deleted since
			//   edges connected to vertices being deleted would get auto-deleted by the db.
			long timeA = System.nanoTime();
			processEdges(jgraph, truncateEndTs, doLogging, doDelete);
			long timeB = System.nanoTime();
			long diffTime =  timeB - timeA;
			long minCount = TimeUnit.NANOSECONDS.toMinutes(diffTime);
			long secCount = TimeUnit.NANOSECONDS.toSeconds(diffTime) - (60 * minCount);
			LOGGER.debug(" Took this long to process the Edges: " +
					minCount + " minutes, " + secCount + " seconds " );

			processVerts(jgraph, truncateEndTs, doLogging, doDelete);
			long timeC = System.nanoTime();
			diffTime =  timeC - timeB;
			minCount = TimeUnit.NANOSECONDS.toMinutes(diffTime);
			secCount = TimeUnit.NANOSECONDS.toSeconds(diffTime) - (60 * minCount);
			LOGGER.debug(" Took this long to process the Vertices: " +
					minCount + " minutes, " + secCount + " seconds " );

		} catch (AAIException e) {
			ErrorLogHelper.logError("AAI_6128", e.getMessage());
			LOGGER.error("Encountered an exception during the historyTruncate: ", e);
			successStatus = false;
		} catch (Exception ex) {
			ErrorLogHelper.logError("AAI_6128", ex.getMessage());
			LOGGER.error("Encountered an exception during the historyTruncate: ", ex);
			successStatus = false;
		} finally {
			if (jgraph != null ) {
				// Any changes that worked correctly should have already done
				// their commits.
				if(!"true".equals(System.getProperty("org.onap.aai.graphadmin.started"))) {
					if (jgraph.isOpen()) {
						jgraph.tx().rollback();
						jgraph.close();
					}
				}
			}
		}

		return successStatus;
	}


	public void processVerts(JanusGraph jgraph, Long truncBeforeTs,
			Boolean doLogging, Boolean doDelete ) {

		Graph g = jgraph.newTransaction();
		GraphTraversalSource gts = g.traversal();
		//Iterator <Vertex> vertItr = gts.V().has(AAIProperties.END_TS, P.lt(truncBeforeTs));
		List<Long> vidList = gts.V().has("end-ts", P.lt(truncBeforeTs))
			.toStream()
			.map(v -> v.id().toString())
			.map(Long::valueOf)
			.collect(Collectors.toList());

		int vTotalCount = vidList.size();
		int batchCount = vTotalCount / batchCommitSize;
		if((batchCount * batchCommitSize) < vTotalCount){
			batchCount++;
		}

		LOGGER.info( " Vertex TotalCount = " + vTotalCount +
				", we get batchCount = " + batchCount +
				", using commit size = " + batchCommitSize );

		int vIndex = 0;
		for(int batchNo=1; batchNo<=batchCount; batchNo++){
			ArrayList <Long> batchVids = new ArrayList<Long>();
			int thisBVCount = 0;
			while((thisBVCount < batchCommitSize) && (vIndex < vTotalCount)) {
				batchVids.add(vidList.get(vIndex));
				thisBVCount++;
				vIndex++;
			}
			// now process this batch
			LOGGER.info( "Process vertex batch # " + batchNo +
					", which contains " + batchVids.size() + " ids. ");
			processVertBatch(jgraph, doLogging, doDelete, batchVids);
		}
	}


	private void processVertBatch(JanusGraph jgraph, Boolean doLogging,
			Boolean doDelete, ArrayList <Long> vidList ) {

		Graph g = jgraph.newTransaction();
		GraphTraversalSource gts = g.traversal();
		int delFailCount = 0;
		int vCount = 0;
		int delCount = 0;

		Iterator<Vertex> vertItr = gts.V(vidList);
		while(vertItr.hasNext()) {
			vCount++;
			Vertex tmpV = vertItr.next();
			String tmpVid = tmpV.id().toString();
			String tmpPropsStr = "";
			if(doLogging) {
				Iterator<VertexProperty<Object>> pI = tmpV.properties();
				while( pI.hasNext() ){
					VertexProperty<Object> tp = pI.next();
					Object val = tp.value();
					tmpPropsStr = tmpPropsStr + "[" + tp.key() + "=" + val + "]";
				}
				LOGGER.info(" vid = " + tmpVid + ", props: (" + tmpPropsStr + ") " );
			}

			if(doDelete) {
				LOGGER.info("Removing vid = " + tmpVid );
				try {
					tmpV.remove();
					delCount++;
				} catch ( Exception e ) {
					// figure out what to do
					delFailCount++;
					LOGGER.error("ERROR trying to delete Candidate VID = " + tmpVid + " " + LogFormatTools.getStackTop(e));
				}
			}
		}

		if(doDelete) {
			LOGGER.info("Calling commit on delete of Vertices." );
			try {
				g.tx().commit();
			} catch ( Exception e ) {
				LOGGER.error("ERROR trying to commit Vertex Deletes for this batch. " +
						LogFormatTools.getStackTop(e) );
				LOGGER.info( vCount + " candidate vertices processed.  "
						+ " vertex deletes - COMMIT FAILED. ");
				return;
			}
		}

		if(doDelete) {
			LOGGER.info( vCount + " candidate vertices processed.  " +
					delFailCount + " delete attempts failed, " +
					delCount + " deletes successful. ");
		}
		else {
			LOGGER.info( vCount + " candidate vertices processed in this batch.  " );
		}
	}


	public void processEdges(JanusGraph jgraph, Long truncBeforeTs,
			Boolean doLogging, Boolean doDelete ) {

		Graph g = jgraph.newTransaction();
		GraphTraversalSource gts = g.traversal();
		//Iterator <Edge> edgeItr = gts.E().has(AAIProperties.END_TS, P.lt(truncBeforeTs));
		Iterator <Edge> edgeItr = gts.E().has("end-ts", P.lt(truncBeforeTs));
		ArrayList <String> eidList = new ArrayList <String> ();
		while( edgeItr.hasNext() ) {
			Edge tmpE = edgeItr.next();
			String tmpEid = tmpE.id().toString();
			eidList.add(tmpEid);
		}

		int eTotalCount = eidList.size();
		int batchCount = eTotalCount / batchCommitSize;
		if((batchCount * batchCommitSize) < eTotalCount){
			batchCount++;
		}

		LOGGER.info( " Edge TotalCount = " + eTotalCount +
				", we get batchCount = " + batchCount +
				", using commit size = " + batchCommitSize );

		int eIndex = 0;
		for(int batchNo=1; batchNo<=batchCount; batchNo++){
			ArrayList <String> batchEids = new ArrayList <String> ();
			int thisBECount = 0;
			while( (thisBECount < batchCommitSize) && (eIndex < eTotalCount) ) {
				batchEids.add(eidList.get(eIndex));
				thisBECount++;
				eIndex++;
			}
			// now process this batch
			LOGGER.info( "Process edge batch # " + batchNo +
					", which contains " + batchEids.size() + " ids. ");
			processEdgeBatch(jgraph, doLogging, doDelete, batchEids);
		}
	}


	private void processEdgeBatch(JanusGraph jgraph, Boolean doLogging,
			Boolean doDelete, ArrayList <String> eidList ) {

		Graph g = jgraph.newTransaction();
		GraphTraversalSource gts = g.traversal();
		int delFailCount = 0;
		int eCount = 0;
		int delCount = 0;

		Iterator <Edge> edgeItr = gts.E(eidList);
		while( edgeItr.hasNext() ) {
			eCount++;
			Edge tmpE = edgeItr.next();
			String tmpEid = tmpE.id().toString();
			if( doLogging ) {
				String tmpEProps = "";
				Iterator<Property<Object>> epI = tmpE.properties();
				while( epI.hasNext() ){
					Property<Object> ep = epI.next();
					Object val = ep.value();
					tmpEProps = tmpEProps + "[" + ep.key() + "=" + val + "]";
				}
				Iterator <Vertex> conVtxs = tmpE.bothVertices();
				String tmpConVs = "";
				while( conVtxs.hasNext() ) {
					Vertex conV = conVtxs.next();
					tmpConVs = tmpConVs + "[" + conV.id().toString() + "] ";
				}
				LOGGER.info(" eid = " + tmpEid
						+ ", Connecting vids = " + tmpConVs
						+ ", props: (" + tmpEProps + "). "  );
			}

			if( doDelete ) {
				LOGGER.info("Removing Edge eid = " + tmpEid );
				try {
					tmpE.remove();
					delCount++;
				} catch ( Exception e ) {
					delFailCount++;
					LOGGER.error("ERROR trying to delete Candidate Edge with eid = " + tmpEid + " " + LogFormatTools.getStackTop(e));
				}
			}
		}

		if( doDelete ) {
			LOGGER.info("Calling commit on delete of Edges." );
			try {
				g.tx().commit();
			} catch ( Exception e ) {
				LOGGER.error("ERROR trying to commit Edge Deletes for this batch. " +
						LogFormatTools.getStackTop(e) );
				LOGGER.info( eCount + " candidate edges processed.  "
						+ " edge deletes - COMMIT FAILED. ");
				return;
			}
		}

		if( doDelete ) {
			LOGGER.info( eCount + " candidate edges processed.  " +
					delFailCount + " delete attempts failed, " +
					delCount + " deletes successful. ");
		}
		else {
			LOGGER.info( eCount + " candidate edges processed in this batch.  " );
		}
	}


	public int getCandidateVertexCount(JanusGraph jgraph, int windowDaysVal) {
		Graph g = jgraph.newTransaction();
		GraphTraversalSource gts = g.traversal();
		Long truncTs = calculateTruncWindowEndTimeStamp(windowDaysVal);
		//int candVCount = gts.V().has(AAIProperties.END_TS, P.lt(truncTs)).count().next().intValue();
		int candVCount = gts.V().has("end-ts", P.lt(truncTs)).count().next().intValue();
		LOGGER.info( " for the timeStamp = " + truncTs
				+ ", which corresponds to the passed truncateWindowDays = "
				+ windowDaysVal
				+ ", found " + candVCount
				+ " candidate vertices. ");
		return candVCount;
	}


	public int getCandidateEdgeCount(JanusGraph jgraph, int windowDaysVal) {
		Graph g = jgraph.newTransaction();
		GraphTraversalSource gts = g.traversal();
		Long truncTs = calculateTruncWindowEndTimeStamp(windowDaysVal);
		//int candECount = gts.E().has(AAIProperties.END_TS, P.lt(truncTs)).count().next().intValue();
		int candECount = gts.E().has("end-ts", P.lt(truncTs)).count().next().intValue();
		LOGGER.info( " for the timeStamp = " + truncTs
				+ ", which corresponds to the passed truncateWindowDays = "
				+ windowDaysVal
				+ ", found " + candECount
				+ " candidate Edges. ");
		return candECount;
	}


	public static void verifyGraph(JanusGraph graph) {

		if (graph == null) {
			String emsg = "Not able to get a graph object in HistoryTruncate.java\n";
			LOGGER.debug(emsg);
			AAISystemExitUtil.systemExitCloseAAIGraph(1);
		}

	}

	public long calculateTruncWindowEndTimeStamp( int timeWindowDays ){
		// Given a window size in days, calculate the timestamp that
		//   represents the early-edge of that window.

		long unixTimeNow = System.currentTimeMillis();
		if( timeWindowDays <= 0 ){
			// This just means that they want to truncate all the way up to the current time
			return unixTimeNow;
		}

		long windowInMillis = timeWindowDays * 24 * 60 * 60L * 1000;
		long windowEdgeTimeStampInMs = unixTimeNow - windowInMillis;
		return windowEdgeTimeStampInMs;

	}

	private Boolean doLoggingOrNot( String truncMode ){
		if( truncMode.equals(SILENT_DELETE_MODE) ){
			return false;
		}
		else {
			return true;
		}
	}

	private Boolean doDeleteOrNot( String truncMode ){
		if( truncMode.equals(LOG_ONLY_MODE) ){
			return false;
		}
		else {
			return true;
		}
	}


}
