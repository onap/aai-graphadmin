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

import java.io.ByteArrayOutputStream;
import java.io.File;
import java.io.FileInputStream;
import java.io.IOException;
import java.io.InputStream;
import java.io.SequenceInputStream;
import java.util.*;

import java.util.concurrent.Callable;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.Future;

import java.util.concurrent.TimeUnit;
import org.apache.tinkerpop.gremlin.structure.Vertex;

import org.apache.commons.configuration.PropertiesConfiguration;

import org.apache.tinkerpop.gremlin.structure.io.IoCore;
import org.apache.tinkerpop.gremlin.structure.io.graphson.LegacyGraphSONReader;
import org.onap.aai.dbmap.AAIGraph;
import org.onap.aai.dbmap.AAIGraphConfig;
import org.onap.aai.exceptions.AAIException;
import org.onap.aai.logging.ErrorLogHelper;
import org.onap.aai.util.AAIConfig;
import org.onap.aai.util.AAIConstants;
import org.onap.aai.util.AAISystemExitUtil;
import org.onap.aai.util.FormatDate;

import com.att.eelf.configuration.Configuration;
import com.att.eelf.configuration.EELFLogger;
import com.att.eelf.configuration.EELFManager;
import org.janusgraph.core.JanusGraph;
import org.janusgraph.core.JanusGraphFactory;
import org.janusgraph.core.util.JanusGraphCleanup;

public class DataSnapshot {

	private static EELFLogger LOGGER;
	
	/* Using realtime d */
	private static final String REALTIME_DB = "realtime";

	private static final Set<String> SNAPSHOT_RELOAD_COMMANDS = new HashSet<>();

	static {
	    SNAPSHOT_RELOAD_COMMANDS.add("RELOAD_LEGACY_DATA");
		SNAPSHOT_RELOAD_COMMANDS.add("RELOAD_DATA");
		SNAPSHOT_RELOAD_COMMANDS.add("RELOAD_DATA_MULTI");
	}
	
	
	/**
	 * The main method.
	 *
	 * @param args
	 *            the arguments
	 */
	public static void main(String[] args) {

	    boolean success = true;

		// Set the logging file properties to be used by EELFManager
		System.setProperty("aai.service.name", DataSnapshot.class.getSimpleName());
		Properties props = System.getProperties();
		props.setProperty(Configuration.PROPERTY_LOGGING_FILE_NAME, AAIConstants.AAI_LOGBACK_PROPS);
		props.setProperty(Configuration.PROPERTY_LOGGING_FILE_PATH, AAIConstants.AAI_HOME_BUNDLECONFIG);
		LOGGER = EELFManager.getInstance().getLogger(DataSnapshot.class);
		Boolean dbClearFlag = false;
		JanusGraph graph = null;
		String command = "JUST_TAKE_SNAPSHOT"; // This is the default
		String oldSnapshotFileName = "";
		
		Long vertAddDelayMs = 1L;   // Default value
		Long edgeAddDelayMs = 1L;   // Default value
		
		Long failureDelayMs = 50L;  // Default value
		Long retryDelayMs = 1500L;  // Default value
		int maxErrorsPerThread = 25; // Default value
		Long vertToEdgeProcDelay = 9000L; // Default value 
		Long staggerThreadDelay = 5000L;  // Default value

		int threadCount = 0;
		Boolean debugFlag = false;
		int debugAddDelayTime = 1;  // Default to 1 millisecond

		boolean isExistingTitan = false;
		
		if (args.length >= 1) {
			command = args[0];
		}
			
		if( SNAPSHOT_RELOAD_COMMANDS.contains(command)){
			if (args.length == 2) {
				// If re-loading, they need to also pass the snapshot file name to use.
				// We expected the file to be found in our snapshot directory.
				oldSnapshotFileName = args[1];
			}
		}
		else if( command.equals("THREADED_SNAPSHOT") ){
			if (args.length == 2) {
				// If doing a "threaded" snapshot, they need to specify how many threads to use
				try {
					threadCount = Integer.parseInt(args[1]);
				}
				catch ( NumberFormatException nfe ){
					ErrorLogHelper.logError("AAI_6128", "Bad (non-integer) threadCount passed to DataSnapshot [" + args[1] + "]");
					LOGGER.debug("Bad (non-integer) threadCount passed to DataSnapshot [" + args[1] + "]");
					AAISystemExitUtil.systemExitCloseAAIGraph(1);
				}
				if( threadCount < 1 || threadCount > 100 ){
					ErrorLogHelper.logError("AAI_6128", "Out of range (1-100) threadCount passed to DataSnapshot [" + args[1] + "]");
					LOGGER.debug("Out of range (1-100) threadCount passed to DataSnapshot [" + args[1] + "]");
					AAISystemExitUtil.systemExitCloseAAIGraph(1);
				}
				LOGGER.debug(" Will do Threaded Snapshot with threadCount = " + threadCount );
			}
			else if (args.length == 3) {
				// If doing a "threaded" snapshot, they need to specify how many threads to use
				// They can also use debug mode if they pass the word "DEBUG" to do the nodes one at a time to see where it breaks.
				try {
					threadCount = Integer.parseInt(args[1]);
				}
				catch ( NumberFormatException nfe ){
					ErrorLogHelper.logError("AAI_6128", "Bad (non-integer) threadCount passed to DataSnapshot [" + args[1] + "]");
					LOGGER.debug("Bad (non-integer) threadCount passed to DataSnapshot [" + args[1] + "]");
					AAISystemExitUtil.systemExitCloseAAIGraph(1);
				}
				if( threadCount < 1 || threadCount > 100 ){
					ErrorLogHelper.logError("AAI_6128", "Out of range (1-100) threadCount passed to DataSnapshot [" + args[1] + "]");
					LOGGER.debug("Out of range (1-100) threadCount passed to DataSnapshot [" + args[1] + "]");
					AAISystemExitUtil.systemExitCloseAAIGraph(1);
				}
				if( args[2].equals("DEBUG") ){
					debugFlag = true;
				}
				LOGGER.debug(" Will do Threaded Snapshot with threadCount = " + threadCount + 
						", and DEBUG mode set ON. ");
			}
			else if (args.length == 4) {
				// If doing a "threaded" snapshot, they need to specify how many threads to use (param 1)
				// They can also use debug mode if they pass the word "DEBUG" to do the nodes one (param 2)
				// They can also pass a delayTimer - how many milliseconds to put between each node's ADD (param 3)
				try {
					threadCount = Integer.parseInt(args[1]);
				}
				catch ( NumberFormatException nfe ){
					ErrorLogHelper.logError("AAI_6128", "Bad (non-integer) threadCount passed to DataSnapshot [" + args[1] + "]");
					LOGGER.debug("Bad (non-integer) threadCount passed to DataSnapshot [" + args[1] + "]");
					AAISystemExitUtil.systemExitCloseAAIGraph(1);
				}
				if( threadCount < 1 || threadCount > 100 ){
					ErrorLogHelper.logError("AAI_6128", "Out of range (1-100) threadCount passed to DataSnapshot [" + args[1] + "]");
					LOGGER.debug("Out of range (1-100) threadCount passed to DataSnapshot [" + args[1] + "]");
					AAISystemExitUtil.systemExitCloseAAIGraph(1);
				}
				if( args[2].equals("DEBUG") ){
					debugFlag = true;
				}
				try {
					debugAddDelayTime = Integer.parseInt(args[3]);
				}
				catch ( NumberFormatException nfe ){
					ErrorLogHelper.logError("AAI_6128", "Bad (non-integer) debugAddDelayTime passed to DataSnapshot [" + args[3] + "]");
					LOGGER.debug("Bad (non-integer) debugAddDelayTime passed to DataSnapshot [" + args[3] + "]");
					AAISystemExitUtil.systemExitCloseAAIGraph(1);
				}
				LOGGER.debug(" Will do Threaded Snapshot with threadCount = " + threadCount + 
						", DEBUG mode ON and addDelayTimer = " + debugAddDelayTime + " mSec. ");
			}
			else {
				ErrorLogHelper.logError("AAI_6128", "Wrong param count (should be 2,3 or 4) when using THREADED_SNAPSHOT.");
				LOGGER.debug("Wrong param count (should be 2,3 or 4) when using THREADED_SNAPSHOT.");
				AAISystemExitUtil.systemExitCloseAAIGraph(1);
			}
		}
		else if( command.equals("MULTITHREAD_RELOAD") ){
			// Note - this will use as many threads as the snapshot file is 
			//   broken up into.  (up to a limit)
			if (args.length == 2) {
				// Since they are re-loading, they need to pass the snapshot file name to use.
				// We expected the file to be found in our snapshot directory.  Note - if
				// it is a multi-part snapshot, then this should be the root of the name.
				// We will be using the default delay timers.
				oldSnapshotFileName = args[1];
			}
			else if (args.length == 7) {
				// Since they are re-loading, they need to pass the snapshot file name to use.
				// We expected the file to be found in our snapshot directory.  Note - if
				// it is a multi-part snapshot, then this should be the root of the name.
				oldSnapshotFileName = args[1];
				// They should be passing the timers in in this order:
				//    vertDelay, edgeDelay, failureDelay, retryDelay
				vertAddDelayMs = Long.parseLong(args[2]);
				edgeAddDelayMs = Long.parseLong(args[3]);
				failureDelayMs = Long.parseLong(args[4]);
				retryDelayMs = Long.parseLong(args[5]);
				try {
					maxErrorsPerThread = Integer.parseInt(args[6]);
				}
				catch ( NumberFormatException nfe ){
					ErrorLogHelper.logError("AAI_6128", "Bad (non-integer) maxErrorsPerThread passed to DataSnapshot [" + args[6] + "]");
					LOGGER.debug("Bad (non-integer) maxErrorsPerThread passed to DataSnapshot [" + args[6] + "]");
					AAISystemExitUtil.systemExitCloseAAIGraph(1);
				}
				if( maxErrorsPerThread < 1  ){
					ErrorLogHelper.logError("AAI_6128", "Out of range (>0) maxErrorsPerThread passed to DataSnapshot [" + args[6] + "]");
					LOGGER.debug("Out of range (>0) maxErrorsPerThread passed to DataSnapshot [" + args[6] + "]");
					AAISystemExitUtil.systemExitCloseAAIGraph(1);
				}
			}
			else {
				ErrorLogHelper.logError("AAI_6128", "Wrong param count (should be either 2 or 7) when using MUTLITHREAD_RELOAD.");
				LOGGER.debug("Wrong param count (should be 2 or 7) when using MUTLITHREAD_RELOAD.");
				AAISystemExitUtil.systemExitCloseAAIGraph(1);
			}
		}
		else if (command.equals("CLEAR_ENTIRE_DATABASE")) {
			if (args.length >= 2) {
				oldSnapshotFileName = args[1];
			}
			if (args.length == 3) {
				String titanFlag = args[2];
				if ("titan".equalsIgnoreCase(titanFlag)) {
					isExistingTitan = true;
				}
			}
		}

		ByteArrayOutputStream baos = new ByteArrayOutputStream();
		try {
			
			AAIConfig.init();
			ErrorLogHelper.loadProperties();
			LOGGER.debug("Command = " + command + ", oldSnapshotFileName = [" + oldSnapshotFileName + "].");
			String targetDir = AAIConstants.AAI_HOME + AAIConstants.AAI_FILESEP + "logs" + AAIConstants.AAI_FILESEP + "data" + AAIConstants.AAI_FILESEP + "dataSnapshots";

			// Make sure the dataSnapshots directory is there
			new File(targetDir).mkdirs();

			LOGGER.debug("    ---- NOTE --- about to open graph (takes a little while) ");
			
			if (command.equals("JUST_TAKE_SNAPSHOT")) {
				// ------------------------------------------
				// They just want to take a snapshot.
				// ------------------------------------------
				verifyGraph(AAIGraph.getInstance().getGraph());
				FormatDate fd = new FormatDate("yyyyMMddHHmm", "GMT");
				String dteStr = fd.getDateTime();
				String newSnapshotOutFname = targetDir + AAIConstants.AAI_FILESEP + "dataSnapshot.graphSON." + dteStr;
				graph = AAIGraph.getInstance().getGraph();

				graph.io(IoCore.graphson()).writeGraph(newSnapshotOutFname);

				LOGGER.debug("Snapshot written to " + newSnapshotOutFname);
	
			}	
			else if (command.equals("THREADED_SNAPSHOT")) {
					// ---------------------------------------------------------------------
					// They want the creation of the snapshot to be spread out via threads
					// ---------------------------------------------------------------------
					
					FormatDate fd = new FormatDate("yyyyMMddHHmm", "GMT");
					String dteStr = fd.getDateTime();
					String newSnapshotOutFname = targetDir + AAIConstants.AAI_FILESEP + "dataSnapshot.graphSON." + dteStr;
					verifyGraph(AAIGraph.getInstance().getGraph());
					graph = AAIGraph.getInstance().getGraph();
					LOGGER.debug(" Successfully got the Graph instance. ");
					long timeA = System.nanoTime();

					LOGGER.debug(" Need to divide vertexIds across this many threads: " + threadCount );
					HashMap <String,ArrayList> vertListHash = new HashMap <String,ArrayList> ();
					for( int t = 0; t < threadCount; t++ ){
						ArrayList <Vertex> vList = new ArrayList <Vertex> ();
						String tk = "" + t;
						vertListHash.put( tk, vList);
					}
					LOGGER.debug("Count how many nodes are in the db. ");
					long totalVertCount = graph.traversal().V().count().next();
					LOGGER.debug(" Total Count of Nodes in DB = " + totalVertCount + ".");
					long nodesPerFile = totalVertCount / threadCount;
					LOGGER.debug(" Thread count = " + threadCount + ", each file will get (roughly): " + nodesPerFile + " nodes.");
					long timeA2 = System.nanoTime();
					long diffTime =  timeA2 - timeA;
					long minCount = TimeUnit.NANOSECONDS.toMinutes(diffTime);
					long secCount = TimeUnit.NANOSECONDS.toSeconds(diffTime) - (60 * minCount);
					LOGGER.debug("    -- To count all vertices in DB it took: " +
							minCount + " minutes, " + secCount + " seconds " );
					
					long vtxIndex = 0;
					int currentTNum = 0; 
					String currentTKey = "0";
					long thisThrIndex = 0;
					Iterator <Vertex> vtxItr = graph.vertices();
					while( vtxItr.hasNext() ){
						// Divide up all the vertices so we can process them on different threads
						vtxIndex++;
						thisThrIndex++;
						if( (thisThrIndex > nodesPerFile) && (currentTNum < threadCount -1) ){
							// We will need to start adding to the Hash for the next thread
							currentTNum++;
							currentTKey = "" + currentTNum;
							thisThrIndex = 0;
						}
						(vertListHash.get(currentTKey)).add(vtxItr.next());
					}
					
					long timeB = System.nanoTime();
					diffTime =  timeB - timeA2;
					minCount = TimeUnit.NANOSECONDS.toMinutes(diffTime);
					secCount = TimeUnit.NANOSECONDS.toSeconds(diffTime) - (60 * minCount);
					LOGGER.debug("    -- To Loop over all vertices, and put them into sub-Arrays it took: " +
							minCount + " minutes, " + secCount + " seconds " );
					
					// Need to print out each set of vertices using it's own thread
					ArrayList <Thread> threadArr = new ArrayList <Thread> ();
					for( int thNum = 0; thNum < threadCount; thNum++ ){
						String thNumStr = "" + thNum;
						String subFName = newSnapshotOutFname + ".P" + thNumStr;
						Thread thr = new Thread(new PrintVertexDetails(graph, subFName, vertListHash.get(thNumStr),
								debugFlag, debugAddDelayTime) );
						thr.start();
						threadArr.add(thr);
					}
					
					// Make sure all the threads finish before moving on.
					for( int thNum = 0; thNum < threadCount; thNum++ ){
						if( null != threadArr.get(thNum) ){
							(threadArr.get(thNum)).join();
						}
					}
					
					long timeC = System.nanoTime();
					diffTime =  timeC - timeB;
					minCount = TimeUnit.NANOSECONDS.toMinutes(diffTime);
					secCount = TimeUnit.NANOSECONDS.toSeconds(diffTime) - (60 * minCount);
					LOGGER.debug("   -- To write all the data out to snapshot files, it took: " +
							minCount + " minutes, " + secCount + " seconds " );
			
					
			} else if( command.equals("MULTITHREAD_RELOAD") ){		
				// ---------------------------------------------------------------------
				// They want the RELOAD of the snapshot to be spread out via threads
				// NOTE - it will only use as many threads as the number of files the
				//    snapshot is  written to.  Ie. if you have a single-file snapshot,
				//    then this will be single-threaded.
				//	
				ArrayList <File> snapFilesArr = getFilesToProcess(targetDir, oldSnapshotFileName, false);
				int fCount = snapFilesArr.size();
				Iterator <File> fItr = snapFilesArr.iterator();
				
				JanusGraph graph1 = AAIGraph.getInstance().getGraph();
				long timeStart = System.nanoTime();
				
				HashMap <String,String> old2NewVertIdMap = new <String,String> HashMap ();
				
					// We're going to try loading in the vertices - without edges or properties
					//    using Separate threads
					
					ExecutorService executor = Executors.newFixedThreadPool(fCount);
					List<Future<HashMap<String,String>>> list = new ArrayList<Future<HashMap<String,String>>>();
					
					for( int i=0; i < fCount; i++ ){
						File f = snapFilesArr.get(i);
						String fname = f.getName();
						String fullSnapName = targetDir + AAIConstants.AAI_FILESEP + fname;
						Thread.sleep(staggerThreadDelay);  // Stagger the threads a bit
						LOGGER.debug(" -- Read file: [" + fullSnapName + "]");
						LOGGER.debug(" -- Call the PartialVertexLoader to just load vertices  ----");
						LOGGER.debug(" -- vertAddDelayMs = " + vertAddDelayMs 
								+ ", failureDelayMs = " + failureDelayMs + ", retryDelayMs = " + retryDelayMs 
								+ ", maxErrorsPerThread = " + maxErrorsPerThread );
						Callable <HashMap<String,String>> vLoader = new PartialVertexLoader(graph1, fullSnapName, 
								vertAddDelayMs, failureDelayMs, retryDelayMs, maxErrorsPerThread, LOGGER);
						Future <HashMap<String,String>> future = (Future<HashMap<String, String>>) executor.submit(vLoader);
						
						// add Future to the list, we can get return value using Future
						list.add(future);
						LOGGER.debug(" --  Starting PartialDbLoad VERT_ONLY thread # "+ i );
					}
					
					threadCount = 0;
					int threadFailCount = 0;
					for(Future<HashMap<String,String>> fut : list){
		            	threadCount++;
		            	try {
		            		old2NewVertIdMap.putAll(fut.get());
		            		LOGGER.debug(" -- back from PartialVertexLoader.  returned thread # " + threadCount +
		            				", current size of old2NewVertMap is: " + old2NewVertIdMap.size() );
		            	} 
		            	catch (InterruptedException e) {  
		            		threadFailCount++;
		            		e.printStackTrace();
		            	} 
		            	catch (ExecutionException e) {
		            		threadFailCount++;
		            		e.printStackTrace();
		            	}
		            }                       
					
					executor.shutdown();
					
					if( threadFailCount > 0 ) {
						String emsg = " FAILURE >> " + threadFailCount + " Vertex-loader thread(s) failed to complete successfully.  ";
						LOGGER.debug(emsg);
						throw new Exception( emsg );
					}
					
					long timeX = System.nanoTime();
					long diffTime =  timeX - timeStart;
					long minCount = TimeUnit.NANOSECONDS.toMinutes(diffTime);
					long secCount = TimeUnit.NANOSECONDS.toSeconds(diffTime) - (60 * minCount);
					LOGGER.debug("   -- To reload just the vertex ids from the snapshot files, it took: " +
							minCount + " minutes, " + secCount + " seconds " );
							
					// Give the DB a little time to chew on all those vertices
					Thread.sleep(vertToEdgeProcDelay);
					
					// ----------------------------------------------------------------------------------------
					LOGGER.debug("\n\n\n  -- Now do the edges/props ----------------------");
					// ----------------------------------------------------------------------------------------
					
								
					// We're going to try loading in the edges and missing properties
					// Note - we're passing the whole oldVid2newVid mapping to the PartialPropAndEdgeLoader
					//     so that the String-updates to the GraphSON will happen in the threads instead of
					//     here in the un-threaded calling method.
					executor = Executors.newFixedThreadPool(fCount);	
					ArrayList<Future<ArrayList<String>>> listEdg = new ArrayList<Future<ArrayList<String>>>();
					for( int i=0; i < fCount; i++ ){
						File f = snapFilesArr.get(i);
						String fname = f.getName();
						String fullSnapName = targetDir + AAIConstants.AAI_FILESEP + fname;
						Thread.sleep(staggerThreadDelay);  // Stagger the threads a bit
						LOGGER.debug(" -- Read file: [" + fullSnapName + "]");
						LOGGER.debug(" -- Call the PartialPropAndEdgeLoader for Properties and EDGEs  ----");
						LOGGER.debug(" -- edgeAddDelayMs = " + vertAddDelayMs 
								+ ", failureDelayMs = " + failureDelayMs + ", retryDelayMs = " + retryDelayMs 
								+ ", maxErrorsPerThread = " + maxErrorsPerThread );
						
						Callable  eLoader = new PartialPropAndEdgeLoader(graph1, fullSnapName, 
								edgeAddDelayMs, failureDelayMs, retryDelayMs, 
								old2NewVertIdMap, maxErrorsPerThread, LOGGER);
						Future <ArrayList<String>> future = (Future<ArrayList<String>>) executor.submit(eLoader);
						
						//add Future to the list, we can get return value using Future
						listEdg.add(future);
						LOGGER.debug(" --  Starting PartialPropAndEdge thread # "+ i );
					}
						
					threadCount = 0;
					for(Future<ArrayList<String>> fut : listEdg){
			            threadCount++;
			            try{
			            	fut.get();  // DEBUG -- should be doing something with the return value if it's not empty - ie. errors
			            	LOGGER.debug(" -- back from PartialPropAndEdgeLoader.  thread # " + threadCount  );
			            } 
						catch (InterruptedException e) {  
							threadFailCount++;
							e.printStackTrace();
						} 
						catch (ExecutionException e) {
							threadFailCount++;
							e.printStackTrace();
						}
					}   
					
					executor.shutdown();
									
					if( threadFailCount > 0 ) {
						String emsg = " FAILURE >> " + threadFailCount + " Property/Edge-loader thread(s) failed to complete successfully.  ";
						LOGGER.debug(emsg);
						throw new Exception( emsg );
					}
					
					// This is needed so we can see the data committed by the called threads
					graph1.tx().commit();
					 
					long timeEnd = System.nanoTime();
					diffTime =  timeEnd - timeX;
					minCount = TimeUnit.NANOSECONDS.toMinutes(diffTime);
					secCount = TimeUnit.NANOSECONDS.toSeconds(diffTime) - (60 * minCount);
					LOGGER.debug("   -- To reload the edges and properties from snapshot files, it took: " +
							minCount + " minutes, " + secCount + " seconds " );
					
					long totalDiffTime =  timeEnd - timeStart;
					long totalMinCount = TimeUnit.NANOSECONDS.toMinutes(totalDiffTime);
					long totalSecCount = TimeUnit.NANOSECONDS.toSeconds(totalDiffTime) - (60 * totalMinCount);
					LOGGER.debug("   -- TOTAL multi-threaded reload time: " +
							totalMinCount + " minutes, " + totalSecCount + " seconds " );
					
			} else if (command.equals("CLEAR_ENTIRE_DATABASE")) {
				// ------------------------------------------------------------------
				// They are calling this to clear the db before re-loading it
				// later
				// ------------------------------------------------------------------

				// First - make sure the backup file(s) they will be using can be
				// found and has(have) data.
				// getFilesToProcess makes sure the file(s) exist and have some data.
				getFilesToProcess(targetDir, oldSnapshotFileName, true);
				
				LOGGER.debug("\n>>> WARNING <<<< ");
				LOGGER.debug(">>> All data and schema in this database will be removed at this point. <<<");
				LOGGER.debug(">>> Processing will begin in 5 seconds. <<<");
				LOGGER.debug(">>> WARNING <<<< ");

				try {
					// Give them a chance to back out of this
					Thread.sleep(5000);
				} catch (java.lang.InterruptedException ie) {
					LOGGER.debug(" DB Clearing has been aborted. ");
					AAISystemExitUtil.systemExitCloseAAIGraph(1);
				}

				LOGGER.debug(" Begin clearing out old data. ");
				String rtConfig = AAIConstants.REALTIME_DB_CONFIG;
				String serviceName = System.getProperty("aai.service.name", "NA");
				LOGGER.debug("Getting new configs for clearig");
				PropertiesConfiguration propertiesConfiguration = new AAIGraphConfig.Builder(rtConfig).forService(serviceName).withGraphType(REALTIME_DB).buildConfiguration();
				if(isExistingTitan){
					LOGGER.debug("Existing DB is Titan");
					propertiesConfiguration.setProperty("graph.titan-version","1.0.0");
				}
				LOGGER.debug("Open New Janus Graph");
				JanusGraph janusGraph = JanusGraphFactory.open(propertiesConfiguration);
				verifyGraph(janusGraph);

				if(isExistingTitan){
					JanusGraphFactory.drop(janusGraph);
				} else {
					janusGraph.close();
					JanusGraphCleanup.clear(janusGraph);
				}
				LOGGER.debug(" Done clearing data. ");
				LOGGER.debug(">>> IMPORTANT - NOTE >>> you need to run the SchemaGenerator (use GenTester) before ");
				LOGGER.debug("     reloading data or the data will be put in without indexes. ");
				dbClearFlag = true;
				LOGGER.debug("All done clearing DB");
				
			} else if (command.equals("RELOAD_LEGACY_DATA")) {
				// -------------------------------------------------------------------
				// They want to restore the database from an old snapshot file
				// -------------------------------------------------------------------
				verifyGraph(AAIGraph.getInstance().getGraph());
				graph = AAIGraph.getInstance().getGraph();
				if (oldSnapshotFileName.equals("")) {
					String emsg = "No oldSnapshotFileName passed to DataSnapshot when RELOAD_LEGACY_DATA used.";
					LOGGER.debug(emsg);
					AAISystemExitUtil.systemExitCloseAAIGraph(1);
				}
				String oldSnapshotFullFname = targetDir + AAIConstants.AAI_FILESEP + oldSnapshotFileName;
				File f = new File(oldSnapshotFullFname);
				if (!f.exists()) {
					String emsg = "oldSnapshotFile " + oldSnapshotFullFname + " could not be found.";
					LOGGER.debug(emsg);
					AAISystemExitUtil.systemExitCloseAAIGraph(1);
				} else if (!f.canRead()) {
					String emsg = "oldSnapshotFile " + oldSnapshotFullFname + " could not be read.";
					LOGGER.debug(emsg);
					AAISystemExitUtil.systemExitCloseAAIGraph(1);
				} else if (f.length() == 0) {
					String emsg = "oldSnapshotFile " + oldSnapshotFullFname + " had no data.";
					LOGGER.debug(emsg);
					AAISystemExitUtil.systemExitCloseAAIGraph(1);
				}

				LOGGER.debug("We will load data IN from the file = " + oldSnapshotFullFname);
				LOGGER.debug(" Begin reloading JanusGraph 0.5 data. ");
				
				LegacyGraphSONReader lgr = LegacyGraphSONReader.build().create();
				InputStream is = new FileInputStream(oldSnapshotFullFname);
				lgr.readGraph(is, graph);
				
				LOGGER.debug("Completed the inputGraph command, now try to commit()... ");
				graph.tx().commit();
				LOGGER.debug("Completed reloading JanusGraph 0.5 data.");

				long vCount = graph.traversal().V().count().next();
				LOGGER.debug("A little after repopulating from an old snapshot, we see: " + vCount + " vertices in the db.");
			} else if (command.equals("RELOAD_DATA")) {
				// -------------------------------------------------------------------
				// They want to restore the database from an old snapshot file
				// -------------------------------------------------------------------
				verifyGraph(AAIGraph.getInstance().getGraph());
				graph = AAIGraph.getInstance().getGraph();
				if (oldSnapshotFileName.equals("")) {
					String emsg = "No oldSnapshotFileName passed to DataSnapshot when RELOAD_DATA used.";
					LOGGER.debug(emsg);
					AAISystemExitUtil.systemExitCloseAAIGraph(1);
				}
				String oldSnapshotFullFname = targetDir + AAIConstants.AAI_FILESEP + oldSnapshotFileName;
				File f = new File(oldSnapshotFullFname);
				if (!f.exists()) {
					String emsg = "oldSnapshotFile " + oldSnapshotFullFname + " could not be found.";
					LOGGER.debug(emsg);
					AAISystemExitUtil.systemExitCloseAAIGraph(1);
				} else if (!f.canRead()) {
					String emsg = "oldSnapshotFile " + oldSnapshotFullFname + " could not be read.";
					LOGGER.debug(emsg);
					AAISystemExitUtil.systemExitCloseAAIGraph(1);
				} else if (f.length() == 0) {
					String emsg = "oldSnapshotFile " + oldSnapshotFullFname + " had no data.";
					LOGGER.debug(emsg);
					AAISystemExitUtil.systemExitCloseAAIGraph(1);
				}

				LOGGER.debug("We will load data IN from the file = " + oldSnapshotFullFname);
				LOGGER.debug(" Begin reloading data. ");
				graph.io(IoCore.graphson()).readGraph(oldSnapshotFullFname);
				LOGGER.debug("Completed the inputGraph command, now try to commit()... ");
				graph.tx().commit();
				LOGGER.debug("Completed reloading data.");

				long vCount = graph.traversal().V().count().next();
				
				LOGGER.debug("A little after repopulating from an old snapshot, we see: " + vCount + " vertices in the db.");
				
			} else if (command.equals("RELOAD_DATA_MULTI")) {
				// -------------------------------------------------------------------
				// They want to restore the database from a group of snapshot files
				// Note - this uses multiple snapshot files, but runs single-threaded.
				// -------------------------------------------------------------------
				verifyGraph(AAIGraph.getInstance().getGraph());
				graph = AAIGraph.getInstance().getGraph();
				
				ArrayList <File> snapFilesArr = getFilesToProcess(targetDir, oldSnapshotFileName, false);
				
				long timeA = System.nanoTime();
				
				int fCount = snapFilesArr.size();
				Iterator <File> fItr = snapFilesArr.iterator();
				Vector<InputStream> inputStreamsV = new Vector<>();                  
				for( int i = 0; i < fCount; i++ ){
					File f = snapFilesArr.get(i);
					String fname = f.getName();
					if (!f.canRead()) {
						String emsg = "oldSnapshotFile " + fname + " could not be read.";
						LOGGER.debug(emsg);
						AAISystemExitUtil.systemExitCloseAAIGraph(1);
					} else if (f.length() == 0) {
						String emsg = "oldSnapshotFile " + fname + " had no data.";
						LOGGER.debug(emsg);
						AAISystemExitUtil.systemExitCloseAAIGraph(1);
					}
					String fullFName = targetDir + AAIConstants.AAI_FILESEP + fname;
					InputStream fis = new FileInputStream(fullFName);
					inputStreamsV.add(fis);
				}
				// Now add inputStreams.elements() to the Vector,
			    // inputStreams.elements() will return Enumerations
			    InputStream sis = new SequenceInputStream(inputStreamsV.elements());
			    LOGGER.debug("Begin loading data from " + fCount + " files  -----");
				graph.io(IoCore.graphson()).reader().create().readGraph(sis, graph);  
				LOGGER.debug("Completed the inputGraph command, now try to commit()... ");
				graph.tx().commit();
				LOGGER.debug(" >> Completed reloading data.");
				
				long vCount = graph.traversal().V().count().next();
				LOGGER.debug("A little after repopulating from an old snapshot, we see: " + vCount + " vertices in the db.");
				
				long timeB = System.nanoTime();
				long diffTime =  timeB - timeA;
				long minCount = TimeUnit.NANOSECONDS.toMinutes(diffTime);
				long secCount = TimeUnit.NANOSECONDS.toSeconds(diffTime) - (60 * minCount);
				LOGGER.debug("    -- To Reload this snapshot, it took: " +
						minCount + " minutes, " + secCount + " seconds " );
				
				
			} else {
				String emsg = "Bad command passed to DataSnapshot: [" + command + "]";
				LOGGER.debug(emsg);
				AAISystemExitUtil.systemExitCloseAAIGraph(1);
			}

		} catch (AAIException e) {
			ErrorLogHelper.logError("AAI_6128", e.getMessage());
			LOGGER.error("Encountered an exception during the datasnapshot: ", e);
			e.printStackTrace();
			success = false;
		} catch (Exception ex) {
			ErrorLogHelper.logError("AAI_6128", ex.getMessage());
			LOGGER.error("Encountered an exception during the datasnapshot: ", ex);
			ex.printStackTrace();
			success = false;
		} finally {
			if (!dbClearFlag && graph != null) {
				// Any changes that worked correctly should have already done
				// thier commits.
				if(!"true".equals(System.getProperty("org.onap.aai.graphadmin.started"))) {
					if (graph.isOpen()) {
						graph.tx().rollback();
						graph.close();
					}
				}
			}
			try {
				baos.close();
			} catch (IOException iox) {
			}
		}

		if(success){
			AAISystemExitUtil.systemExitCloseAAIGraph(0);
		} else {
			AAISystemExitUtil.systemExitCloseAAIGraph(1);
		}

	}// End of main()
	
	
	private static ArrayList <File> getFilesToProcess(String targetDir, String oldSnapshotFileName, boolean doingClearDb)
		throws Exception {
	
		if( oldSnapshotFileName == null || oldSnapshotFileName.equals("") ){
			String emsg = "No oldSnapshotFileName passed to DataSnapshot for Reload.  ";
			if( doingClearDb ) {
				emsg = "No oldSnapshotFileName passed to DataSnapshot. Needed when Clearing the db in case we need a backup.  ";
			}
			LOGGER.debug(emsg);
			throw new Exception( emsg );
		}
	
		ArrayList <File> snapFilesArrList = new ArrayList <File> ();
		
		// First, we'll assume that this is a multi-file snapshot and
		//    look for names based on that.
		String thisSnapPrefix = oldSnapshotFileName + ".P";
		File fDir = new File(targetDir); // Snapshot directory
		File[] allFilesArr = fDir.listFiles();
		for (File snapFile : allFilesArr) {
			String snapFName = snapFile.getName();
			if( snapFName.startsWith(thisSnapPrefix)){
				if (!snapFile.canRead()) {
					String emsg = "oldSnapshotFile " + snapFName + " could not be read.";
					LOGGER.debug(emsg);
					throw new Exception (emsg);
				} else if (snapFile.length() == 0) {
					String emsg = "oldSnapshotFile " + snapFName + " had no data.";
					LOGGER.debug(emsg);
					throw new Exception (emsg);
				}
				snapFilesArrList.add(snapFile);
			}
		}
	
		if( snapFilesArrList.isEmpty() ){
			// Multi-file snapshot check did not find files, so this may 
			//   be a single-file snapshot.
			String oldSnapshotFullFname = targetDir + AAIConstants.AAI_FILESEP + oldSnapshotFileName;
			File f = new File(oldSnapshotFullFname);
			if (!f.exists()) {
				String emsg = "oldSnapshotFile " + oldSnapshotFullFname + " could not be found.";
				LOGGER.debug(emsg);
				throw new Exception (emsg);
			} else if (!f.canRead()) {
				String emsg = "oldSnapshotFile " + oldSnapshotFullFname + " could not be read.";
				LOGGER.debug(emsg);
				throw new Exception (emsg);
			} else if (f.length() == 0) {
				String emsg = "oldSnapshotFile " + oldSnapshotFullFname + " had no data.";
				LOGGER.debug(emsg);
				throw new Exception (emsg);
			}
			snapFilesArrList.add(f);
		}
		
		if( snapFilesArrList.isEmpty() ){
			// Still haven't found anything..  that was not a good file name.
			String fullFName = targetDir + AAIConstants.AAI_FILESEP + thisSnapPrefix;
			String emsg = "oldSnapshotFile " + fullFName + "* could not be found.";
			LOGGER.debug(emsg);
			throw new Exception(emsg);
		}
		
		return snapFilesArrList;
	}
	
	
	public static void verifyGraph(JanusGraph graph) {

		if (graph == null) {
			String emsg = "Not able to get a graph object in DataSnapshot.java\n";
			LOGGER.debug(emsg);
			AAISystemExitUtil.systemExitCloseAAIGraph(1);
		}

	}


}