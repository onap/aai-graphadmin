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
import org.onap.aai.dbmap.AAIGraph;
import org.onap.aai.dbmap.AAIGraphConfig;
import org.onap.aai.exceptions.AAIException;
import org.onap.aai.logging.ErrorLogHelper;
import org.onap.aai.util.AAIConfig;
import org.onap.aai.util.AAIConstants;
import org.onap.aai.util.GraphAdminConstants;
import org.onap.aai.util.AAISystemExitUtil;
import org.onap.aai.util.FormatDate;
import org.onap.aai.util.GraphAdminDBUtils;

import com.att.eelf.configuration.Configuration;
import com.att.eelf.configuration.EELFLogger;
import com.att.eelf.configuration.EELFManager;
import com.beust.jcommander.JCommander;
import com.beust.jcommander.Parameter;
import com.beust.jcommander.ParameterException;

import org.janusgraph.core.JanusGraph;
import org.janusgraph.core.JanusGraphFactory;
import org.janusgraph.core.util.JanusGraphCleanup;

public class DataSnapshot {

	private static EELFLogger LOGGER;
	
	/* Using realtime d */
	private static final String REALTIME_DB = "realtime";

	private static final Set<String> SNAPSHOT_RELOAD_COMMANDS = new HashSet<>();

	private static final String MIGRATION_PROCESS_NAME = "migration";

	static {
		SNAPSHOT_RELOAD_COMMANDS.add("RELOAD_DATA");
		SNAPSHOT_RELOAD_COMMANDS.add("RELOAD_DATA_MULTI");
	}
	
	private CommandLineArgs cArgs;
	
	
	/**
	 * The main method.
	 *
	 * @param args
	 *            the arguments
	 */
	public static void main(String[] args) {

	    boolean success = true;

		Boolean dbClearFlag = false;
		JanusGraph graph = null;
		String command = "JUST_TAKE_SNAPSHOT"; // This is the default
		String oldSnapshotFileName = "";

		DataSnapshot dataSnapshot = new DataSnapshot();
		success = dataSnapshot.executeCommand(args, success, dbClearFlag, graph, command,
				oldSnapshotFileName);
		
		if(success){
			AAISystemExitUtil.systemExitCloseAAIGraph(0);
		} else {
			AAISystemExitUtil.systemExitCloseAAIGraph(1);
		}

	}// End of main()


	public boolean executeCommand(String[] args, boolean success,
			Boolean dbClearFlag, JanusGraph graph, String command,
			String oldSnapshotFileName) {
		
		// Set the logging file properties to be used by EELFManager
		System.setProperty("aai.service.name", DataSnapshot.class.getSimpleName());
		Properties props = System.getProperties();
		props.setProperty(Configuration.PROPERTY_LOGGING_FILE_NAME, AAIConstants.AAI_LOGBACK_PROPS);
		props.setProperty(Configuration.PROPERTY_LOGGING_FILE_PATH, AAIConstants.AAI_HOME_BUNDLECONFIG);
		LOGGER = EELFManager.getInstance().getLogger(DataSnapshot.class);
		cArgs = new CommandLineArgs();
		
		String itemName = "aai.datasnapshot.threads.for.create";
		
		try {
			String val = AAIConfig.get(itemName);
			if( val != null &&  !val.equals("") ){
				cArgs.threadCount = Integer.parseInt(val);
			}
		}catch ( Exception e ){
			LOGGER.warn("WARNING - could not get [" + itemName + "] value from aaiconfig.properties file. " + e.getMessage());
		}
		int threadCount4Create = cArgs.threadCount;
		
		cArgs.snapshotType = "graphson";
		
		Long vertAddDelayMs = GraphAdminConstants.AAI_SNAPSHOT_DEFAULT_VERTEX_ADD_DELAY_MS;
		itemName = "aai.datasnapshot.vertex.add.delay.ms";
		try {
			String val = AAIConfig.get(itemName);
			if( val != null &&  !val.equals("") ){
				cArgs.vertAddDelayMs = Long.parseLong(val);
			}
		}catch ( Exception e ){
			LOGGER.warn("WARNING - could not get [" + itemName + "] value from aaiconfig.properties file. " + e.getMessage());
		}
		
		Long edgeAddDelayMs = GraphAdminConstants.AAI_SNAPSHOT_DEFAULT_EDGE_ADD_DELAY_MS;
		itemName = "aai.datasnapshot.edge.add.delay.ms";
		try {
			String val = AAIConfig.get(itemName);
			if( val != null &&  !val.equals("") ){
				cArgs.edgeAddDelayMs = Long.parseLong(val);
			}
		}catch ( Exception e ){
			LOGGER.warn("WARNING - could not get [" + itemName + "] value from aaiconfig.properties file. " + e.getMessage());
		}
		
		Long failureDelayMs = GraphAdminConstants.AAI_SNAPSHOT_DEFAULT_FAILURE_DELAY_MS;
		itemName = "aai.datasnapshot.failure.delay.ms";
		try {
			String val = AAIConfig.get(itemName);
			if( val != null &&  !val.equals("") ){
				cArgs.failureDelayMs = Long.parseLong(val);
			}
		}catch ( Exception e ){
			LOGGER.warn("WARNING - could not get [" + itemName + "] value from aaiconfig.properties file. " + e.getMessage());
		}
		
		Long retryDelayMs = GraphAdminConstants.AAI_SNAPSHOT_DEFAULT_RETRY_DELAY_MS;
		itemName = "aai.datasnapshot.retry.delay.ms";
		try {
			String val = AAIConfig.get(itemName);
			if( val != null &&  !val.equals("") ){
				cArgs.retryDelayMs = Long.parseLong(val);
			}
		}catch ( Exception e ){
			LOGGER.warn("WARNING - could not get [" + itemName + "] value from aaiconfig.properties file. " + e.getMessage());
		}
		
		int maxErrorsPerThread = GraphAdminConstants.AAI_SNAPSHOT_DEFAULT_MAX_ERRORS_PER_THREAD;
		itemName = "aai.datasnapshot.max.errors.per.thread";
		try {
			String val = AAIConfig.get(itemName);
			if( val != null &&  !val.equals("") ){
				cArgs.maxErrorsPerThread = Integer.parseInt(val);
			}
		}catch ( Exception e ){
			LOGGER.warn("WARNING - could not get [" + itemName + "] value from aaiconfig.properties file. " + e.getMessage());
		}
		
		Long vertToEdgeProcDelay = GraphAdminConstants.AAI_SNAPSHOT_DEFAULT_VERTEX_TO_EDGE_PROC_DELAY_MS;
		itemName = "aai.datasnapshot.vertex.to.edge.proc.delay.ms";
		try {
			String val = AAIConfig.get(itemName);
			if( val != null &&  !val.equals("") ){
				cArgs.vertToEdgeProcDelay = Long.parseLong(val);
			}
		}catch ( Exception e ){
			LOGGER.warn("WARNING - could not get [" + itemName + "] value from aaiconfig.properties file. " + e.getMessage());
		}
		
		itemName = "aai.datasnapshot.stagger.thread.delay.ms";
		try {
			String val = AAIConfig.get(itemName);
			if( val != null &&  !val.equals("") ){
				cArgs.staggerThreadDelay = Long.parseLong(val);
			}
		}catch ( Exception e ){
			LOGGER.warn("WARNING - could not get [" + itemName + "] value from aaiconfig.properties file. " + e.getMessage());
		}		
	
		long debugAddDelayTime = 1;  // Default to 1 millisecond
		Boolean debug4Create = false;  // By default we do not use debugging for snapshot creation
		
		JCommander jCommander;
		try {
			jCommander = new JCommander(cArgs, args);
			jCommander.setProgramName(DataSnapshot.class.getSimpleName());
		} catch (ParameterException e1) {
			LOGGER.error("Error - invalid value passed to list of args - "+args);
			AAISystemExitUtil.systemExitCloseAAIGraph(1);
		}
		
				
		if (args.length >= 1) {
			command = cArgs.command;
		}
		
		String source = cArgs.caller;

        String snapshotType = "graphson";
		if( SNAPSHOT_RELOAD_COMMANDS.contains(cArgs.command)){
			if (args.length >= 2) {
				// If re-loading, they need to also pass the snapshot file name to use.
				// We expected the file to be found in our snapshot directory.
				oldSnapshotFileName = cArgs.oldFileName;
				snapshotType = cArgs.snapshotType;
			}
		}
		else if( command.equals("THREADED_SNAPSHOT") ){
			if (args.length >= 2) {
				// If doing a "threaded" snapshot, they need to specify how many threads to use
				try {
					threadCount4Create = cArgs.threadCount;
				}
				catch ( NumberFormatException nfe ){
					ErrorLogHelper.logError("AAI_6128", "Bad (non-integer) threadCount passed to DataSnapshot [" + cArgs.threadCount + "]");
					LOGGER.debug("Bad (non-integer) threadCount passed to DataSnapshot [" + cArgs.threadCount + "]");
					AAISystemExitUtil.systemExitCloseAAIGraph(1);
				}
				if( threadCount4Create < 1 || threadCount4Create > 100 ){
					ErrorLogHelper.logError("AAI_6128", "Out of range (1-100) threadCount passed to DataSnapshot [" + cArgs.threadCount + "]");
					LOGGER.debug("Out of range (1-100) threadCount passed to DataSnapshot [" + cArgs.threadCount + "]");
					AAISystemExitUtil.systemExitCloseAAIGraph(1);
				}
				LOGGER.debug(" Will do Threaded Snapshot with threadCount = " + threadCount4Create );
				
				// If doing a "threaded" snapshot, they need to specify how many threads to use
				// They can also use debug mode if they pass the word "DEBUG" to do the nodes one at a time to see where it breaks.
				if( cArgs.debugFlag.equals("DEBUG") ){
					debug4Create = true;
				}
				LOGGER.debug(" Will do Threaded Snapshot with threadCount = " + threadCount4Create +
						", and DEBUG-flag set to: " + debug4Create );
				
				if (debug4Create) {
					// If doing a "threaded" snapshot, they need to specify how many threads to use (param 1)
					// They can also use debug mode if they pass the word "DEBUG" to do the nodes one (param 2)
					// They can also pass a delayTimer - how many milliseconds to put between each node's ADD (param 3)
					try {
						debugAddDelayTime = cArgs.debugAddDelayTime;
					} catch (NumberFormatException nfe) {
						ErrorLogHelper.logError("AAI_6128",	"Bad (non-integer) debugAddDelayTime passed to DataSnapshot ["
										+ cArgs.debugAddDelayTime + "]");
						LOGGER.debug("Bad (non-integer) debugAddDelayTime passed to DataSnapshot ["+ cArgs.debugAddDelayTime + "]");
						AAISystemExitUtil.systemExitCloseAAIGraph(1);
					}
					LOGGER.debug(" Will do Threaded Snapshot with threadCount = "+ threadCount4Create + ", DEBUG-flag set to: "
							+ debug4Create + ", and addDelayTimer = " + debugAddDelayTime + " mSec. ");
				}
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
			if (args.length >= 2) {
				// Since they are re-loading, they need to pass the snapshot file name to use.
				// We expected the file to be found in our snapshot directory.  Note - if
				// it is a multi-part snapshot, then this should be the root of the name.
				// We will be using the default delay timers.
				oldSnapshotFileName = cArgs.oldFileName;
				
				// They should be passing the timers in in this order:
				//    vertDelay, edgeDelay, failureDelay, retryDelay
				vertAddDelayMs = cArgs.vertAddDelayMs;
				edgeAddDelayMs = cArgs.edgeAddDelayMs;
				failureDelayMs = cArgs.failureDelayMs;
				retryDelayMs = cArgs.retryDelayMs;
				try {
					maxErrorsPerThread = cArgs.maxErrorsPerThread;
				}
				catch ( NumberFormatException nfe ){
					ErrorLogHelper.logError("AAI_6128", "Bad (non-integer) maxErrorsPerThread passed to DataSnapshot [" + cArgs.maxErrorsPerThread + "]");
					LOGGER.debug("Bad (non-integer) maxErrorsPerThread passed to DataSnapshot [" + cArgs.maxErrorsPerThread + "]");
					AAISystemExitUtil.systemExitCloseAAIGraph(1);
				}
				if( maxErrorsPerThread < 1  ){
					ErrorLogHelper.logError("AAI_6128", "Out of range (>0) maxErrorsPerThread passed to DataSnapshot [" + cArgs.maxErrorsPerThread + "]");
					LOGGER.debug("Out of range (>0) maxErrorsPerThread passed to DataSnapshot [" + cArgs.maxErrorsPerThread + "]");
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
				oldSnapshotFileName = cArgs.oldFileName;
			}
		}

		
		//Print Defaults
		LOGGER.info("DataSnapshot command is [" + cArgs.command + "]");
		LOGGER.info("File name to reload snapshot [" + cArgs.oldFileName + "]");
		LOGGER.info("snapshotType is [" + cArgs.snapshotType + "]");
		LOGGER.info("Thread count is [" + cArgs.threadCount + "]");
		LOGGER.info("Debug Flag is [" + cArgs.debugFlag + "]");
		LOGGER.info("DebugAddDelayTimer is [" + cArgs.debugAddDelayTime + "]");
		LOGGER.info("VertAddDelayMs is [" + cArgs.vertAddDelayMs + "]");
		LOGGER.info("FailureDelayMs is [" + cArgs.failureDelayMs + "]");
		LOGGER.info("RetryDelayMs is [" + cArgs.retryDelayMs + "]");
		LOGGER.info("MaxErrorsPerThread is [" + cArgs.maxErrorsPerThread + "]");
		LOGGER.info("VertToEdgeProcDelay is [" + cArgs.vertToEdgeProcDelay + "]");
		LOGGER.info("StaggerThreadDelay is [" + cArgs.staggerThreadDelay + "]");
		LOGGER.info("Caller process is ["+ cArgs.caller + "]");
		
		//Print non-default values
		if (!AAIConfig.isEmpty(cArgs.fileName)){
			LOGGER.info("Snapshot file name (if not default) to use  is [" + cArgs.fileName + "]");
		}
		if (!AAIConfig.isEmpty(cArgs.snapshotDir)){
			LOGGER.info("Snapshot file Directory path (if not default) to use is [" + cArgs.snapshotDir + "]");
		}
		if (!AAIConfig.isEmpty(cArgs.oldFileDir)){
			LOGGER.info("Directory path (if not default) to load the old snapshot file from is [" + cArgs.oldFileDir + "]");
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
			
			if ( (command.equals("THREADED_SNAPSHOT") || command.equals("JUST_TAKE_SNAPSHOT"))
					&& threadCount4Create == 1 ){
				// -------------------------------------------------------------------------------
				// They want to take a snapshot on a single thread and have it go in a single file
				//   NOTE - they can't use the DEBUG option in this case.
				// -------------------------------------------------------------------------------
				LOGGER.debug("\n>>> Command = " + command );
				verifyGraph(AAIGraph.getInstance().getGraph());
				FormatDate fd = new FormatDate("yyyyMMddHHmm", "GMT");
				String dteStr = fd.getDateTime();
				graph = AAIGraph.getInstance().getGraph();
				GraphAdminDBUtils.logConfigs(graph.configuration());
				String newSnapshotOutFname = null;
				long timeA = System.nanoTime();
				newSnapshotOutFname = targetDir + AAIConstants.AAI_FILESEP + "dataSnapshot.graphSON." + dteStr;
				graph.io(IoCore.graphson()).writeGraph(newSnapshotOutFname);
				LOGGER.debug("Snapshot written to " + newSnapshotOutFname);
				long timeB = System.nanoTime();
				long diffTime =  timeB - timeA;
				long minCount = TimeUnit.NANOSECONDS.toMinutes(diffTime);
				long secCount = TimeUnit.NANOSECONDS.toSeconds(diffTime) - (60 * minCount);
				LOGGER.debug("    -- Single-Thread dataSnapshot took: " +
						minCount + " minutes, " + secCount + " seconds " );
	
			}	
			else if ( (command.equals("THREADED_SNAPSHOT") || command.equals("JUST_TAKE_SNAPSHOT")) 
					&& threadCount4Create > 1 ){
					// ------------------------------------------------------------
					// They want the creation of the snapshot to be spread out via 
					//    threads and go to multiple files
					// ------------------------------------------------------------
					LOGGER.debug("\n>>> Command = " + command );
					String newSnapshotOutFname;
					if (!AAIConfig.isEmpty(cArgs.fileName)){
						newSnapshotOutFname = cArgs.fileName;
					} else {
					FormatDate fd = new FormatDate("yyyyMMddHHmm", "GMT");
					String dteStr = fd.getDateTime();
					newSnapshotOutFname = targetDir + AAIConstants.AAI_FILESEP + "dataSnapshot.graphSON." + dteStr;
					}
					verifyGraph(AAIGraph.getInstance().getGraph());
					graph = AAIGraph.getInstance().getGraph();
					LOGGER.debug(" Successfully got the Graph instance. ");
					GraphAdminDBUtils.logConfigs(graph.configuration());
					long timeA = System.nanoTime();

					LOGGER.debug(" Need to divide vertexIds across this many threads: " + threadCount4Create );
					HashMap <String,ArrayList> vertListHash = new HashMap <String,ArrayList> ();
					for( int t = 0; t < threadCount4Create; t++ ){
						ArrayList <Vertex> vList = new ArrayList <Vertex> ();
						String tk = "" + t;
						vertListHash.put( tk, vList);
					}
					LOGGER.debug("Count how many nodes are in the db. ");
					long totalVertCount = graph.traversal().V().count().next();
					LOGGER.debug(" Total Count of Nodes in DB = " + totalVertCount + ".");
					long nodesPerFile = totalVertCount / threadCount4Create;
					LOGGER.debug(" Thread count = " + threadCount4Create + ", each file will get (roughly): " + nodesPerFile + " nodes.");
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
						if( (thisThrIndex > nodesPerFile) && (currentTNum < threadCount4Create -1) ){
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
					for( int thNum = 0; thNum < threadCount4Create; thNum++ ){
						String thNumStr = "" + thNum;
						String subFName = newSnapshotOutFname + ".P" + thNumStr;
						Thread thr = new Thread(new PrintVertexDetails(graph, subFName, vertListHash.get(thNumStr),
								debug4Create, debugAddDelayTime, snapshotType) );
						thr.start();
						threadArr.add(thr);
					}
					
					// Make sure all the threads finish before moving on.
					for( int thNum = 0; thNum < threadCount4Create; thNum++ ){
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
				LOGGER.debug("\n>>> Command = " + command );
				
				if (cArgs.oldFileDir != null && cArgs.oldFileDir != ""){
					targetDir = cArgs.oldFileDir;
				}
				ArrayList <File> snapFilesArr = getFilesToProcess(targetDir, oldSnapshotFileName, false);
				int fCount = snapFilesArr.size();
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
						Thread.sleep(cArgs.staggerThreadDelay);  // Stagger the threads a bit
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

					int threadCount4Reload = 0;
					int threadFailCount = 0;
					for(Future<HashMap<String,String>> fut : list){
		            	threadCount4Reload++;
		            	try {
		            		old2NewVertIdMap.putAll(fut.get());
		            		LOGGER.debug(" -- back from PartialVertexLoader.  returned thread # " + threadCount4Reload +
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
						Thread.sleep(cArgs.staggerThreadDelay);  // Stagger the threads a bit
						LOGGER.debug(" -- Read file: [" + fullSnapName + "]");
						LOGGER.debug(" -- Call the PartialPropAndEdgeLoader for Properties and EDGEs  ----");
						LOGGER.debug(" -- edgeAddDelayMs = " + vertAddDelayMs
								+ ", failureDelayMs = " + failureDelayMs + ", retryDelayMs = " + retryDelayMs
								+ ", maxErrorsPerThread = " + maxErrorsPerThread );

						Callable  eLoader = new PartialPropAndEdgeLoader.PartialPropAndEdgeLoaderBuilder().setGraph(graph1).setFn(fullSnapName).setEdgeDelay(edgeAddDelayMs).setFailureDelay(failureDelayMs).setRetryDelay(retryDelayMs).setVidMap(old2NewVertIdMap).setMaxErrors(maxErrorsPerThread).setElfLog(LOGGER).createPartialPropAndEdgeLoader();
						Future <ArrayList<String>> future = (Future<ArrayList<String>>) executor.submit(eLoader);

						//add Future to the list, we can get return value using Future
						listEdg.add(future);
						LOGGER.debug(" --  Starting PartialPropAndEdge thread # "+ i );
					}

					threadCount4Reload = 0;
					for(Future<ArrayList<String>> fut : listEdg){
			            threadCount4Reload++;
			            try{
			            	fut.get();  // DEBUG -- should be doing something with the return value if it's not empty - ie. errors
			            	LOGGER.debug(" -- back from PartialPropAndEdgeLoader.  thread # " + threadCount4Reload  );
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
				LOGGER.debug("\n>>> Command = " + command );
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
				LOGGER.debug("Open New Janus Graph");
				JanusGraph janusGraph = JanusGraphFactory.open(propertiesConfiguration);
				verifyGraph(janusGraph);
				GraphAdminDBUtils.logConfigs(janusGraph.configuration());
				janusGraph.close();
				JanusGraphCleanup.clear(janusGraph);
				LOGGER.debug(" Done clearing data. ");
				LOGGER.debug(">>> IMPORTANT - NOTE >>> you need to run the SchemaGenerator (use GenTester) before ");
				LOGGER.debug("     reloading data or the data will be put in without indexes. ");
				dbClearFlag = true;
				LOGGER.debug("All done clearing DB");
				
			} else if (command.equals("RELOAD_DATA")) {
				// ---------------------------------------------------------------------------
				// They want to restore the database from either a single file, or a group
				// of snapshot files.  Either way, this command will restore via single
				// threaded processing.
				// ---------------------------------------------------------------------------
				LOGGER.debug("\n>>> Command = " + command );
				verifyGraph(AAIGraph.getInstance().getGraph());
				graph = AAIGraph.getInstance().getGraph();
				GraphAdminDBUtils.logConfigs(graph.configuration());
				if (oldSnapshotFileName.equals("")) {
					String emsg = "No oldSnapshotFileName passed to DataSnapshot when RELOAD_DATA used.";
					LOGGER.debug(emsg);
					AAISystemExitUtil.systemExitCloseAAIGraph(1);
				}
				
				long timeA = System.nanoTime();

				ArrayList <File> snapFilesArr = new ArrayList <File> ();

				// First, see if this is a single file (ie. no ".P#" suffix)
				String onePieceSnapshotFname = targetDir + AAIConstants.AAI_FILESEP + oldSnapshotFileName;
				File sf = new File(onePieceSnapshotFname);
				if( sf.exists() ){
					snapFilesArr.add(sf);
				}
				else {
					// If it's a multi-part snapshot, then collect all the files for it
					String thisSnapPrefix = oldSnapshotFileName + ".P";
					File fDir = new File(targetDir); // Snapshot directory
					File[] allFilesArr = fDir.listFiles();
					for (File snapFile : allFilesArr) {
						String snapFName = snapFile.getName();
						if( snapFName.startsWith(thisSnapPrefix)){
							snapFilesArr.add(snapFile);
						}
					}
				}
				
				if( snapFilesArr.isEmpty() ){
					String emsg = "oldSnapshotFile " + onePieceSnapshotFname + "(with or without .P0) could not be found.";
					LOGGER.debug(emsg);
					AAISystemExitUtil.systemExitCloseAAIGraph(1);
				}
				
				int fCount = snapFilesArr.size();
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
			    if("gryo".equalsIgnoreCase(snapshotType)){
					graph.io(IoCore.gryo()).reader().create().readGraph(sis, graph);
				} else {
					graph.io(IoCore.graphson()).reader().create().readGraph(sis, graph);
				}
				LOGGER.debug("Completed the inputGraph command, now try to commit()... ");
				graph.tx().commit();
				LOGGER.debug("Completed reloading data.");

				long vCount = graph.traversal().V().count().next();
				LOGGER.debug("A little after repopulating from an old snapshot, we see: " + vCount + " vertices in the db.");
				
				long timeB = System.nanoTime();
				long diffTime =  timeB - timeA;
				long minCount = TimeUnit.NANOSECONDS.toMinutes(diffTime);
				long secCount = TimeUnit.NANOSECONDS.toSeconds(diffTime) - (60 * minCount);
				LOGGER.debug("    -- To Reload this snapshot, it took: " +
						minCount + " minutes, " + secCount + " seconds " );
				
				LOGGER.debug("A little after repopulating from an old snapshot, we see: " + vCount + " vertices in the db.");

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
			if (!dbClearFlag && graph != null && !MIGRATION_PROCESS_NAME.equalsIgnoreCase(source)) {
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

		return success;
	}
	

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

	class CommandLineArgs {

		

		@Parameter(names = "--help", help = true)
		public boolean help;

		@Parameter(names = "-c", description = "command for taking data snapshot")
		public String command = "JUST_TAKE_SNAPSHOT";

		@Parameter(names = "-f", description = "previous snapshot file to reload")
		public String oldFileName = "";

		@Parameter(names = "-snapshotType", description = "snapshot type of gryo or graphson")
		public String snapshotType = "graphson";

		@Parameter(names = "-threadCount", description = "thread count for create")
		public int threadCount = GraphAdminConstants.AAI_SNAPSHOT_DEFAULT_THREADS_FOR_CREATE;

		@Parameter(names = "-debugFlag", description = "DEBUG flag")
		public String debugFlag = "";

		@Parameter(names = "-debugAddDelayTime", description = "delay in ms between each Add for debug mode")
		public long debugAddDelayTime = 1L;
		
		@Parameter(names = "-vertAddDelayMs", description = "delay in ms while adding each vertex")
		public long vertAddDelayMs = GraphAdminConstants.AAI_SNAPSHOT_DEFAULT_VERTEX_ADD_DELAY_MS.longValue();
		
		@Parameter(names = "-edgeAddDelayMs", description = "delay in ms while adding each edge")
		public long edgeAddDelayMs = GraphAdminConstants.AAI_SNAPSHOT_DEFAULT_EDGE_ADD_DELAY_MS.longValue();
		
		@Parameter(names = "-failureDelayMs", description = "delay in ms when failure to load vertex or edge in snapshot")
		public long failureDelayMs = GraphAdminConstants.AAI_SNAPSHOT_DEFAULT_FAILURE_DELAY_MS.longValue();

		@Parameter(names = "-retryDelayMs", description = "time in ms after which load snapshot is retried")
		public long retryDelayMs = GraphAdminConstants.AAI_SNAPSHOT_DEFAULT_FAILURE_DELAY_MS.longValue();

		@Parameter(names = "-maxErrorsPerThread", description = "max errors allowed per thread")
		public int maxErrorsPerThread = GraphAdminConstants.AAI_SNAPSHOT_DEFAULT_MAX_ERRORS_PER_THREAD;
		
		@Parameter(names = "-vertToEdgeProcDelay", description = "vertex to edge processing delay in ms")
		public long vertToEdgeProcDelay = GraphAdminConstants.AAI_SNAPSHOT_DEFAULT_VERTEX_TO_EDGE_PROC_DELAY_MS.longValue();
		
		@Parameter(names = "-staggerThreadDelay", description = "thread delay stagger time in ms")
		public long staggerThreadDelay = GraphAdminConstants.AAI_SNAPSHOT_DEFAULT_STAGGER_THREAD_DELAY_MS;
		
		@Parameter(names = "-fileName", description = "file name for generating snapshot ")
		public String fileName = "";
		
		@Parameter(names = "-snapshotDir", description = "file path for generating snapshot ")
		public String snapshotDir = "";
		
		@Parameter(names = "-oldFileDir", description = "directory containing the old snapshot file for reloading")
		public String oldFileDir = "";
		
		@Parameter(names = "-caller", description = "process invoking the dataSnapshot")
		public String caller = "";
		
	}
	
}