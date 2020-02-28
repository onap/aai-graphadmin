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
package org.onap.aai.datagrooming;

import java.io.BufferedReader;
import java.io.BufferedWriter;
import java.io.File;
import java.io.FileReader;
import java.io.FileWriter;
import java.io.IOException;
import java.util.ArrayList;
import java.util.Collection;
import java.util.HashMap;
import java.util.Iterator;
import java.util.LinkedHashSet;
import java.util.List;
import java.util.Map;
import java.util.Map.Entry;
import java.util.Properties;
import java.util.Set;
import java.util.UUID;

import org.apache.tinkerpop.gremlin.process.traversal.dsl.graph.GraphTraversal;
import org.apache.tinkerpop.gremlin.process.traversal.dsl.graph.GraphTraversalSource;
import org.apache.tinkerpop.gremlin.process.traversal.dsl.graph.__;
import org.apache.tinkerpop.gremlin.structure.Direction;
import org.apache.tinkerpop.gremlin.structure.Edge;
import org.apache.tinkerpop.gremlin.structure.Graph;
import org.apache.tinkerpop.gremlin.structure.Property;
import org.apache.tinkerpop.gremlin.structure.Vertex;
import org.apache.tinkerpop.gremlin.structure.VertexProperty;
import org.onap.aai.GraphAdminApp;
import org.onap.aai.config.PropertyPasswordConfiguration;
import org.onap.aai.util.GraphAdminConstants;
import org.onap.logging.ref.slf4j.ONAPLogConstants;
import org.onap.aai.dbmap.AAIGraph;
import org.onap.aai.exceptions.AAIException;
import org.onap.aai.introspection.Introspector;
import org.onap.aai.introspection.Loader;
import org.onap.aai.introspection.LoaderFactory;
import org.onap.aai.introspection.ModelType;
import org.onap.aai.introspection.exceptions.AAIUnknownObjectException;
import org.onap.aai.logging.ErrorLogHelper;
import org.onap.aai.logging.LogFormatTools;
import org.onap.aai.edges.enums.AAIDirection;
import org.onap.aai.edges.enums.EdgeProperty;
import org.onap.aai.setup.SchemaVersions;
import org.onap.aai.setup.SchemaVersion;
import org.onap.aai.util.*;

import com.att.eelf.configuration.Configuration;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import com.beust.jcommander.JCommander;
import com.beust.jcommander.Parameter;

import org.janusgraph.core.JanusGraph;
import org.springframework.context.annotation.AnnotationConfigApplicationContext;


public class DataGrooming {

	private static Logger LOGGER = LoggerFactory.getLogger(DataGrooming.class);
	private static boolean historyEnabled;  
	private static final String FROMAPPID = "AAI-DB";
	private static final String TRANSID = UUID.randomUUID().toString();
	private int dupeGrpsDeleted = 0;

	private LoaderFactory loaderFactory;
	private SchemaVersions schemaVersions;
	
	private CommandLineArgs cArgs;
	
	HashMap<String, Vertex> orphanNodeHash ;
	HashMap<String, Vertex> missingAaiNtNodeHash ;
	HashMap<String, Vertex> badUriNodeHash ;
	HashMap<String, Vertex> badIndexNodeHash ;
	HashMap<String, Edge> oneArmedEdgeHash ;
	HashMap<String, Vertex> ghostNodeHash ;
	ArrayList<String> dupeGroups;
	Set<String> deleteCandidateList;
	private int deleteCount = 0;

	public DataGrooming(LoaderFactory loaderFactory, SchemaVersions schemaVersions){
		this.loaderFactory  = loaderFactory;
		this.schemaVersions = schemaVersions;
	}

	public void execute(String[] args){

		String ver = "version"; // Placeholder
		
		// Note - if execute() is called from DataGroomingTasks, ie. from the cron, 
		//  then 'historyEnabled' will default to false.  In History, dataGrooming 
		//  is never called via the cron, but this check will prevent it from 
		//  being called from the command line.
		if( historyEnabled ) {
	    	LOGGER.debug("ERROR: DataGrooming may not be used when history.enabled=true. ");
	    	return;
		}

		// A value of 0 means that we will not have a time-window -- we will look
		// at all nodes of the passed-in nodeType.
		int timeWindowMinutes = 0;

		int maxRecordsToFix = GraphAdminConstants.AAI_GROOMING_DEFAULT_MAX_FIX;
		int sleepMinutes = GraphAdminConstants.AAI_GROOMING_DEFAULT_SLEEP_MINUTES;
		try {
			String maxFixStr = AAIConfig.get("aai.grooming.default.max.fix");
			if( maxFixStr != null &&  !maxFixStr.equals("") ){
				maxRecordsToFix = Integer.parseInt(maxFixStr);
			}
			String sleepStr = AAIConfig.get("aai.grooming.default.sleep.minutes");
			if( sleepStr != null &&  !sleepStr.equals("") ){
				sleepMinutes = Integer.parseInt(sleepStr);
			}
		}
		catch ( Exception e ){
			// Don't worry, we'll just use the defaults that we got from AAIConstants
			LOGGER.warn("WARNING - could not pick up aai.grooming values from aaiconfig.properties file. " + e.getMessage());
		}

		String prevFileName = "";
		String singleNodeType = "";
		dupeGrpsDeleted = 0;
		FormatDate fd = new FormatDate("yyyyMMddHHmm", "GMT");
		String dteStr = fd.getDateTime();
		cArgs = new CommandLineArgs();
		try {
			String maxFixStr = AAIConfig.get("aai.grooming.default.max.fix");
			if( maxFixStr != null &&  !maxFixStr.equals("") ){
				cArgs.maxRecordsToFix = Integer.parseInt(maxFixStr);
			}
			String sleepStr = AAIConfig.get("aai.grooming.default.sleep.minutes");
			if( sleepStr != null &&  !sleepStr.equals("") ){
				cArgs.sleepMinutes = Integer.parseInt(sleepStr);
			}
		}
		catch ( Exception e ){
			// Don't worry, we'll just use the defaults that we got from AAIConstants
			LOGGER.warn("WARNING - could not pick up aai.grooming values from aaiconfig.properties file. " + e.getMessage());
		}
		
		
		JCommander jCommander = new JCommander(cArgs, args);
		jCommander.setProgramName(DataGrooming.class.getSimpleName());
		
		//Print Defaults
		LOGGER.debug("EdgesOnlyFlag is [" + cArgs.edgesOnlyFlag + "]");
		LOGGER.debug("DoAutoFix is [" + cArgs.doAutoFix + "]");
		LOGGER.debug("skipHostCheck is [" + cArgs.skipHostCheck + "]");
		LOGGER.debug("dontFixOrphansFlag is [" + cArgs.dontFixOrphansFlag + "]");
		LOGGER.debug("dupeCheckOff is [" + cArgs.dupeCheckOff + "]");
		LOGGER.debug("dupeFixOn is [" + cArgs.dupeFixOn + "]");
		LOGGER.debug("ghost2CheckOff is [" + cArgs.ghost2CheckOff + "]");
		LOGGER.debug("ghost2FixOn is [" + cArgs.ghost2FixOn + "]");
		LOGGER.debug("neverUseCache is [" + cArgs.neverUseCache + "]");
		LOGGER.debug("singleNodeType is [" + cArgs.singleNodeType + "]");
		LOGGER.debug("skipEdgeChecks is [" + cArgs.skipEdgeCheckFlag + "]");
		LOGGER.debug("skipIndexUpdateFix is [" + cArgs.skipIndexUpdateFix + "]");
		LOGGER.debug("maxFix is [" + cArgs.maxRecordsToFix + "]");
		

		String windowTag = "FULL";
		//TODO???
		if( cArgs.timeWindowMinutes > 0 ){
			windowTag = "PARTIAL";
		}
		String groomOutFileName = "dataGrooming." + windowTag + "." + dteStr + ".out";

		try {
			loaderFactory.createLoaderForVersion(ModelType.MOXY, schemaVersions.getDefaultVersion());
		}
		catch (Exception ex){
			AAIException ae = new AAIException("AAI_6128", ex , "ERROR - Could not create loader "+args);
			ErrorLogHelper.logException(ae);			
			AAISystemExitUtil.systemExitCloseAAIGraph(1);
		}

		try {
			if (!cArgs.prevFileName.equals("")) {
				// They are trying to fix some data based on a data in a
				// previous file.
				LOGGER.debug(" Call doTheGrooming() with a previous fileName ["
						+ prevFileName + "] for cleanup. ");
				Boolean finalShutdownFlag = true;
				Boolean cacheDbOkFlag = false;
				doTheGrooming(prevFileName, cArgs.edgesOnlyFlag, cArgs.dontFixOrphansFlag,
						cArgs.maxRecordsToFix, groomOutFileName, ver, 
						cArgs.dupeCheckOff, cArgs.dupeFixOn, cArgs.ghost2CheckOff, cArgs.ghost2FixOn,
						finalShutdownFlag, cacheDbOkFlag,
						cArgs.skipEdgeCheckFlag, cArgs.timeWindowMinutes,
						cArgs.singleNodeType, cArgs.skipIndexUpdateFix );
			} else if (cArgs.doAutoFix) {
				// They want us to run the processing twice -- first to look for
				// delete candidates, then after
				// napping for a while, run it again and delete any candidates
				// that were found by the first run.
				// Note: we will produce a separate output file for each of the
				// two runs.
				LOGGER.debug(" Doing an auto-fix call to Grooming. ");
				LOGGER.debug(" First, Call doTheGrooming() to look at what's out there. ");
				Boolean finalShutdownFlag = false;
				Boolean cacheDbOkFlag = true;
				int fixCandCount = doTheGrooming("", cArgs.edgesOnlyFlag,
						cArgs.dontFixOrphansFlag, cArgs.maxRecordsToFix, groomOutFileName,
						ver, cArgs.dupeCheckOff, cArgs.dupeFixOn, cArgs.ghost2CheckOff, cArgs.ghost2FixOn,
						finalShutdownFlag, cacheDbOkFlag,
						cArgs.skipEdgeCheckFlag, cArgs.timeWindowMinutes,
						cArgs.singleNodeType, cArgs.skipIndexUpdateFix );
				if (fixCandCount == 0) {
					LOGGER.debug(" No fix-Candidates were found by the first pass, so no second/fix-pass is needed. ");
				} else {
					// We'll sleep a little and then run a fix-pass based on the
					// first-run's output file.
					try {
						LOGGER.debug("About to sleep for " + cArgs.sleepMinutes
								+ " minutes.");
						int sleepMsec = cArgs.sleepMinutes * 60 * 1000;
						Thread.sleep(sleepMsec);
					} catch (InterruptedException ie) {
						LOGGER.debug(" >>> Sleep Thread has been Interrupted <<< ");
						AAISystemExitUtil.systemExitCloseAAIGraph(0);
					}

					dteStr = fd.getDateTime();
					String secondGroomOutFileName = "dataGrooming." + windowTag + "." + dteStr + ".out";
					LOGGER.debug(" Now, call doTheGrooming() a second time and pass in the name of the file "
							+ "generated by the first pass for fixing: ["
							+ groomOutFileName + "]");
					finalShutdownFlag = true;
					cacheDbOkFlag = false;
					doTheGrooming(groomOutFileName, cArgs.edgesOnlyFlag,
							cArgs.dontFixOrphansFlag, cArgs.maxRecordsToFix,
							secondGroomOutFileName, ver, 
							cArgs.dupeCheckOff, cArgs.dupeFixOn, cArgs.ghost2CheckOff, cArgs.ghost2FixOn,
							finalShutdownFlag, cacheDbOkFlag,
							cArgs.skipEdgeCheckFlag, cArgs.timeWindowMinutes,
							cArgs.singleNodeType, cArgs.skipIndexUpdateFix );
				}
			} else {
				// Do the grooming - plain vanilla (no fix-it-file, no
				// auto-fixing)
				Boolean finalShutdownFlag = true;
				LOGGER.debug(" Call doTheGrooming() ");
				Boolean cacheDbOkFlag = true;
				if( cArgs.neverUseCache ){
					// They have forbidden us from using a cached db connection.
					cacheDbOkFlag = false;
				}
				doTheGrooming("", cArgs.edgesOnlyFlag, cArgs.dontFixOrphansFlag,
						cArgs.maxRecordsToFix, groomOutFileName, ver, 
						cArgs.dupeCheckOff, cArgs.dupeFixOn, cArgs.ghost2CheckOff, cArgs.ghost2FixOn,
						finalShutdownFlag, cacheDbOkFlag,
						cArgs.skipEdgeCheckFlag, cArgs.timeWindowMinutes,
						cArgs.singleNodeType, cArgs.skipIndexUpdateFix );
			}
		} catch (Exception ex) {
			LOGGER.debug("Exception while grooming data " + LogFormatTools.getStackTop(ex));
		}
		LOGGER.debug(" Done! ");
		AAISystemExitUtil.systemExitCloseAAIGraph(0);
	}
	
	/**
	 * The main method.
	 *
	 * @param args the arguments
	 */
	public static void main(String[] args) throws AAIException {

		// Set the logging file properties to be used by EELFManager
		System.setProperty("aai.service.name", DataGrooming.class.getSimpleName());

		Properties props = System.getProperties();
		props.setProperty(Configuration.PROPERTY_LOGGING_FILE_NAME, AAIConstants.AAI_LOGBACK_PROPS);
		props.setProperty(Configuration.PROPERTY_LOGGING_FILE_PATH, AAIConstants.AAI_HOME_BUNDLECONFIG);

		AnnotationConfigApplicationContext ctx = new AnnotationConfigApplicationContext();
		PropertyPasswordConfiguration initializer = new PropertyPasswordConfiguration();
		initializer.initialize(ctx);

		try {
			ctx.scan(
					"org.onap.aai.config",
					"org.onap.aai.setup"
			);
			ctx.refresh();

		} catch (Exception e) {
			AAIException aai = ExceptionTranslator.schemaServiceExceptionTranslator(e);
			ErrorLogHelper.logError(aai.getCode(), e.getMessage() + ", resolve and retry");
			throw aai;
		}
				
		historyEnabled = Boolean.parseBoolean(ctx.getEnvironment().getProperty("history.enabled","false"));
		
		LoaderFactory loaderFactory = ctx.getBean(LoaderFactory.class);
		SchemaVersions schemaVersions = (SchemaVersions) ctx.getBean("schemaVersions");
		DataGrooming dataGrooming = new DataGrooming(loaderFactory, schemaVersions);
		dataGrooming.execute(args);
	}

	/**
	 * Do the grooming.
	 *
	 * @param fileNameForFixing the file name for fixing
	 * @param edgesOnlyFlag the edges only flag
	 * @param dontFixOrphansFlag the dont fix orphans flag
	 * @param maxRecordsToFix the max records to fix
	 * @param groomOutFileName the groom out file name
	 * @param version the version
	 * @param dupeCheckOff the dupe check off
	 * @param dupeFixOn the dupe fix on
	 * @param ghost2CheckOff the ghost 2 check off
	 * @param ghost2FixOn the ghost 2 fix on
	 * @param finalShutdownFlag the final shutdown flag
	 * @param cacheDbOkFlag the cacheDbOk flag
	 * @return the int
	 */
	private int doTheGrooming( String fileNameForFixing,
			Boolean edgesOnlyFlag, Boolean dontFixOrphansFlag,
			int maxRecordsToFix, String groomOutFileName, String version,
			Boolean dupeCheckOff, Boolean dupeFixOn,
			Boolean ghost2CheckOff, Boolean ghost2FixOn, 
			Boolean finalShutdownFlag, Boolean cacheDbOkFlag,
			Boolean skipEdgeCheckFlag, int timeWindowMinutes,
			String singleNodeType, Boolean skipIndexUpdateFix ) {

		LOGGER.debug(" Entering doTheGrooming ");

		int cleanupCandidateCount = 0;
		long windowStartTime = 0; // Translation of the window into a starting timestamp 
		BufferedWriter bw = null;
		JanusGraph graph = null;
		JanusGraph graph2 = null;
		deleteCount = 0;
		int dummyUpdCount = 0;
		int indexUpdCount = 0;
		boolean executeFinalCommit = false;
		deleteCandidateList = new LinkedHashSet<>();
		Set<String> processedVertices = new LinkedHashSet<>();
		Set<String> postCommitRemoveList = new LinkedHashSet<>();

		Graph g = null;
		Graph g2 = null;
		try {
			if( timeWindowMinutes > 0 ){
		  		// Translate the window value (ie. 30 minutes) into a unix timestamp like
		  		//    we use in the db - so we can select data created after that time.
		  		windowStartTime = figureWindowStartTime( timeWindowMinutes );
		  	}
			
			AAIConfig.init();
			String targetDir = AAIConstants.AAI_HOME + AAIConstants.AAI_FILESEP
					+ "logs" + AAIConstants.AAI_FILESEP + "data"
					+ AAIConstants.AAI_FILESEP + "dataGrooming";

			// Make sure the target directory exists
			new File(targetDir).mkdirs();

			if (!fileNameForFixing.equals("")) {
				deleteCandidateList = getDeleteList(targetDir,
						fileNameForFixing, edgesOnlyFlag, dontFixOrphansFlag,
						dupeFixOn);
			}

			if (deleteCandidateList.size() > maxRecordsToFix) {
				LOGGER.warn(" >> WARNING >>  Delete candidate list size ("
						+ deleteCandidateList.size()
						+ ") is too big.  The maxFix we are using is: "
						+ maxRecordsToFix
						+ ".  No candidates will be deleted. ");
				// Clear out the list so it won't be processed below.
				deleteCandidateList = new LinkedHashSet<>();
			}

			String fullOutputFileName = targetDir + AAIConstants.AAI_FILESEP
					+ groomOutFileName;
			File groomOutFile = new File(fullOutputFileName);
			try {
				groomOutFile.createNewFile();
			} catch (IOException e) {
				String emsg = " Problem creating output file ["
						+ fullOutputFileName + "], exception=" + e.getMessage();
				throw new AAIException("AAI_6124", emsg);
			}

			LOGGER.debug(" Will write to " + fullOutputFileName );
			bw = new BufferedWriter(new FileWriter(groomOutFile.getAbsoluteFile()));
			ErrorLogHelper.loadProperties();
			
			LOGGER.debug("    ---- NOTE --- about to open graph (takes a little while)-------- ");

			if( cacheDbOkFlag ){
				// Since we're just reading (not deleting/fixing anything), we can use 
				// a cached connection to the DB
				
				// -- note JanusGraphFactory has been leaving db connections open
				//graph = JanusGraphFactory.open(new AAIGraphConfig.Builder(AAIConstants.CACHED_DB_CONFIG).forService(DataGrooming.class.getSimpleName()).withGraphType("cached").buildConfiguration());
				graph = AAIGraph.getInstance().getGraph();
			}
			else {
				// -- note JanusGraphFactory has been leaving db connections open
				//graph = JanusGraphFactory.open(new AAIGraphConfig.Builder(AAIConstants.REALTIME_DB_CONFIG).forService(DataGrooming.class.getSimpleName()).withGraphType("realtime1").buildConfiguration());
				graph = AAIGraph.getInstance().getGraph();
			}
			if (graph == null) {
				String emsg = "null graph object in DataGrooming\n";
				throw new AAIException("AAI_6101", emsg);
			}
		
			LOGGER.debug(" Got the graph object. ");
			
			g = graph.newTransaction();
			if (g == null) {
				String emsg = "null graphTransaction object in DataGrooming\n";
				throw new AAIException("AAI_6101", emsg);
			}
			GraphTraversalSource source1 = g.traversal();
			
			ArrayList<String> errArr = new ArrayList<>();
			int totalNodeCount = 0;
			HashMap<String, String> misMatchedHash = new HashMap<String, String>();
			orphanNodeHash = new HashMap<String, Vertex>();
			missingAaiNtNodeHash = new HashMap<String, Vertex>();
			badUriNodeHash = new HashMap<String, Vertex>();
			badIndexNodeHash = new HashMap<String, Vertex>();
			oneArmedEdgeHash = new HashMap<String, Edge>();
			HashMap<String, String> emptyVertexHash = new HashMap<String, String>();
			ghostNodeHash = new HashMap<String, Vertex>();
			dupeGroups = new ArrayList<>();

			LOGGER.debug(" Using default schemaVersion = [" + schemaVersions.getDefaultVersion().toString() + "]" );
			Loader loader = loaderFactory.createLoaderForVersion(ModelType.MOXY, schemaVersions.getDefaultVersion());

			// NOTE --- At one point, we tried explicitly searching for
			// nodes that were missing their aai-node-type (which does
			// happen sometimes), but the search takes too long and cannot
			// be restricted to a date-range since these nodes usually do
			// not have timestamps either. Instead, when we run across them
			// as orphans, we will not treat them as orphans, but catagorize
			// them as "missingAaiNodeType" - which we will treat more like
			// ghost nodes - that is, delete them without asking permission.
			//
			// Note Also - It's a little surprising that we can run
			// across these when looking for orphans since that search at
			// least begins based on a given aai-node-type.  But watching
		 	// where they come up, they are getting discovered when a node
			// is looking for its parent node.  So, say, a “tenant” node
			// follows a “contains” edge and finds the bad node.



			Set<Entry<String, Introspector>> entrySet = loader.getAllObjects().entrySet();
			String ntList = "";
			LOGGER.debug("  Starting DataGrooming Processing ");

			if (edgesOnlyFlag) {
				LOGGER.debug(" NOTE >> Skipping Node processing as requested.  Will only process Edges. << ");
			} 
			else {
				for (Entry<String, Introspector> entry : entrySet) {
					String nType = entry.getKey();
					int thisNtCount = 0;
					int thisNtDeleteCount = 0;
					
					if( !singleNodeType.equals("") && !singleNodeType.equals(nType) ){
						// We are only going to process this one node type and this isn't it
						continue;
					}

					LOGGER.debug(" >  Look at : [" + nType + "] ...");
					ntList = ntList + "," + nType;

					// Get a collection of the names of the key properties for this nodeType to use later
					// Determine what the key fields are for this nodeType - use an arrayList so they
					// can be gotten out in a consistent order.
					Set <String> keyPropsSet = entry.getValue().getKeys();
					ArrayList <String> keyProps = new ArrayList <String> ();
					keyProps.addAll(keyPropsSet);
					
					Set <String> indexedPropsSet = entry.getValue().getIndexedProperties();
					ArrayList <String> indexedProps = new ArrayList <String> ();
					indexedProps.addAll(indexedPropsSet);

					Iterator<String> indPropItr = indexedProps.iterator();
					HashMap <String,String> propTypeHash = new HashMap <String, String> ();
					while( indPropItr.hasNext() ){
						String propName = indPropItr.next();
						String propType = entry.getValue().getType(propName);
						propTypeHash.put(propName, propType);
					}

					// Get the types of nodes that this nodetype depends on for uniqueness (if any)
					Collection <String> depNodeTypes = loader.introspectorFromName(nType).getDependentOn();
					
					// Loop through all the nodes of this Node type
					int lastShownForNt = 0;
					ArrayList <Vertex> tmpList = new ArrayList <> ();
					Iterator <Vertex> iterv =  source1.V().has("aai-node-type",nType); 
					while (iterv.hasNext()) {
						// We put the nodes into an ArrayList because the graph.query iterator can time out
						tmpList.add(iterv.next());
					}
					
					Iterator <Vertex> iter = tmpList.iterator();
					while (iter.hasNext()) {
						try {
							thisNtCount++;
							if( thisNtCount == lastShownForNt + 1000 ){
								lastShownForNt = thisNtCount;
								LOGGER.debug("count for " + nType + " so far = " + thisNtCount );
							}
							Vertex thisVtx = iter.next();
							if( windowStartTime > 0 ){
								// They are using the time-window, so we only want nodes that are updated after a
								// passed-in timestamp OR that have no last-modified-timestamp which means they are suspicious.
								Object objModTimeStamp = thisVtx.property("aai-last-mod-ts").orElse(null);
								if( objModTimeStamp != null ){
									long thisNodeModTime = (long)objModTimeStamp;
									if( thisNodeModTime < windowStartTime ){
										// It has a last modified ts and is NOT in our window, so we can pass over it
										continue;
									}
								}
							}
							
							String thisVid = thisVtx.id().toString();
							if (processedVertices.contains(thisVid)) {
								LOGGER.debug("skipping already processed vertex: " + thisVid);
								continue;
							}
							totalNodeCount++;
							// Note - the "secondGetList" is used one node at a time - it is populated
							//   using either the node's defined unique key/keys (if it is not dependent on
							//   a "parent" node, or is populated using the key/keys "under" it's parent node.
							List <Vertex> secondGetList = new ArrayList <> ();  
							
							// -----------------------------------------------------------------------
							// For each vertex of this nodeType, we want to:
							//      a) make sure it can be retrieved using its "aai-uri"
							//		b) make sure that it can be retrieved using it's AAI defined key(s)
							//   	c) make sure that it is not a duplicate
							// -----------------------------------------------------------------------

							Boolean aaiUriOk = checkAaiUriOk(source1, thisVtx);
							
							// For this instance of this nodeType, get the key properties 
							HashMap<String, Object> propHashWithKeys = new HashMap<>();
							Iterator<String> keyPropI = keyProps.iterator();
							while (keyPropI.hasNext()) {
								String propName = keyPropI.next();
								String propVal = "";
								Object obj = thisVtx.<Object>property(propName).orElse(null);
								if (obj != null) {
									propVal = obj.toString();
								}
								propHashWithKeys.put(propName, propVal);
							}
							try {
								// If this node is dependent on another for uniqueness, then do the query from that parent node
								// Note - all of our nodes that are dependent on others for uniqueness are 
								// 		"children" of that node.
								boolean depNodeOk = true;
								if( depNodeTypes.isEmpty() ){
									// This kind of node is not dependent on any other.
									// Make sure we can get it back using it's key properties (that is the
									//   phantom checking) and that we only get one.  Note - we also need
									//   to collect data for a second type of dupe-checking which is done later.
									secondGetList = getNodeJustUsingKeyParams( TRANSID, FROMAPPID, source1, nType, 
											propHashWithKeys, version );
								} 
								else {
									// This kind of node is dependent on another for uniqueness.  
									// Start at it's parent (the parent/containing vertex) and make sure we can get it
									// back using it's key properties and that we only get one.
									Iterator <Vertex> vertI2 = source1.V(thisVtx).union(__.inE().has(EdgeProperty.CONTAINS.toString(), AAIDirection.OUT.toString()).outV(), __.outE().has(EdgeProperty.CONTAINS.toString(), AAIDirection.IN.toString()).inV());
									Vertex parentVtx = null;
									// First we need to try to find the parent/containing vertex.
									int pCount = 0;
									while( vertI2 != null && vertI2.hasNext() ){
										parentVtx = vertI2.next();
										pCount++;
									}
									if( pCount <= 0 ){
										// It's Missing it's dependent/parent/containing node - it's an orphan
										depNodeOk = false;
										if (deleteCandidateList.contains(thisVid)) {
											boolean okFlag = true;
											boolean updateOnlyFlag = false;
											try {
												processedVertices.add(thisVtx.id().toString());
												Object ob = thisVtx.<Object>property("aai-node-type").orElse(null);
												if( ob == null && !skipIndexUpdateFix ){
													updateIndexedPropsForMissingNT(thisVtx, thisVid, nType, propTypeHash, indexedProps);
													updateOnlyFlag = true;
													dummyUpdCount++;
													// Since we are updating this delete candidate, not deleting it, we
													// want it to show up as a delete candidate for this run also.
													missingAaiNtNodeHash.put(thisVid, thisVtx);
												}
												else {
													// There was an aai-node-type parameter, so we'll do the remove
													thisVtx.remove();
													deleteCount++;
													thisNtDeleteCount++;
												}
											} catch (Exception e) {
												okFlag = false;
												LOGGER.debug("ERROR trying to delete delete Candidate VID = " + thisVid + " " + LogFormatTools.getStackTop(e));
											}
											if (okFlag){
												if( updateOnlyFlag ) {
													LOGGER.debug(" Updated Indexes for Delete Candidate VID = " + thisVid);
												}
												else {
													LOGGER.debug(" DELETED Delete Candidate VID = " + thisVid);
												}
											}
										} else {
											// NOTE - Only nodes that are missing their parent/containing node are ever considered "orphaned".
											// That is, you could have a node with no edges... which sounds like an orphan, but not all
											// nodes require edges.  For example, you could have a newly created "image" node which does not have
											// any edges connected to it (using it) yet.
											Object ob = thisVtx.<Object>property("aai-node-type").orElse(null);
											if( ob == null ){
												// Group this with missing-node-type guys - which
												// we will delete more readily than orphans.
												LOGGER.debug(" >> Encountered a missingAaiNodeType while looking for the parent of a [" + nType + "] node.");
												missingAaiNtNodeHash.put(thisVid, thisVtx);
											}
											else {
												Object ob2 = thisVtx.<Object>property("aai-uuid").orElse(null);
												String auid = "";
												if( ob2 != null ){
													auid = ob2.toString();
												}
												String checkDummyUid = thisVid + "dummy";
												if( auid.equals(checkDummyUid) ){
													// Group this with missing-node-type guys.
													LOGGER.debug(" >> Encountered a missingAaiNodeType mid-fix-node while looking for the parent of a [" + nType + "] node.");
													missingAaiNtNodeHash.put(thisVid, thisVtx);
												}
												else {
													// It's a regular old orphan
													orphanNodeHash.put(thisVid, thisVtx);
												}
											}
										}
									}
									else if ( pCount > 1 ){
										// Not sure how this could happen?  Should we do something here?
										depNodeOk = false;
									}
									else {
										// We found the parent - so use it to do the second-look.
										// NOTE --- We're just going to do the same check from the other direction - because
										//  there could be duplicates or the pointer going the other way could be broken
										ArrayList <Vertex> tmpListSec = new ArrayList <> ();
										
										tmpListSec = getConnectedChildrenOfOneType( source1, parentVtx, nType ) ;
										Iterator<Vertex> vIter = tmpListSec.iterator();
										while (vIter.hasNext()) {
											Vertex tmpV = vIter.next();
											if( vertexHasTheseKeys(tmpV, propHashWithKeys) ){
												secondGetList.add(tmpV);
											}
										}
									}
								}// end of -- else this is a dependent node  -- piece
								
								Boolean aaiKeysOk = true;
								if( (secondGetList == null || secondGetList.size() == 0)
										&& depNodeOk){
									aaiKeysOk = false;
								}
							
                              	boolean bothKeysAreBad = false;
                              	if( !aaiKeysOk && !aaiUriOk ) {
                              		bothKeysAreBad = true;
                              	}
                              	else if ( !aaiKeysOk ){
                              		// Just the key-index is bad
                              		// We will not be putting this on the Auto-Delete list, just logging it (AAI-16252)
                              		badIndexNodeHash.put(thisVid, thisVtx);
                              	}
                              	else if ( !aaiUriOk ){
                              		// Just the aai-uri is bad
                              		// We will not be putting this on the Auto-Delete list, just logging it (AAI-16252)
                              		badUriNodeHash.put(thisVid, thisVtx);
                              	}
                               
								if( bothKeysAreBad ){
									// Neither the aai-uri nor key info could retrieve this node - BOTH are bad.
									// So, it's a PHANTOM
									
									if (deleteCandidateList.contains(thisVid)) {
										boolean okFlag = true;
										boolean updateOnlyFlag = false;
										try {
											Object ob = thisVtx.<Object>property("aai-node-type").orElse(null);
											if( ob == null && !skipIndexUpdateFix ){
												updateIndexedPropsForMissingNT(thisVtx, thisVid, nType, propTypeHash, indexedProps);
												dummyUpdCount++;
												updateOnlyFlag = true;
												// Since we are updating this delete candidate, not deleting it, we
												// want it to show up as a delete candidate for this run also.
												missingAaiNtNodeHash.put(thisVid, thisVtx);
											}
											else {
												// There was an aai-node-type parameter, so we'll do the remove
												thisVtx.remove();
												deleteCount++;
												thisNtDeleteCount++;
											}
										} catch (Exception e) {
											okFlag = false;
											LOGGER.debug("ERROR trying to delete phantom VID = " + thisVid + " " + LogFormatTools.getStackTop(e));
										}
										if (okFlag){
											if( updateOnlyFlag ) {
												LOGGER.debug(" Updated Indexes for Delete Candidate VID = " + thisVid);
											}
											else {
												LOGGER.debug(" DELETED VID = " + thisVid);
											}
										}
									} else {
										ghostNodeHash.put(thisVid, thisVtx);
									}
								}
								else if( (secondGetList.size() > 1) && depNodeOk && !dupeCheckOff ){
									// Found some DUPLICATES - need to process them
									LOGGER.debug(" - now check Dupes for this guy - ");
									List<String> tmpDupeGroups = checkAndProcessDupes(
												TRANSID, FROMAPPID, g, source1, version,
												nType, secondGetList, dupeFixOn,
												deleteCandidateList, dupeGroups, loader);
									Iterator<String> dIter = tmpDupeGroups.iterator();
									while (dIter.hasNext()) {
										// Add in any newly found dupes to our running list
										String tmpGrp = dIter.next();
										LOGGER.debug("Found set of dupes: [" + tmpGrp + "]");
										dupeGroups.add(tmpGrp);
									}
								}
							} 
							catch (AAIException e1) {
								LOGGER.warn(" For nodeType = " + nType + " Caught exception", e1);
								errArr.add(e1.getErrorObject().toString());
							}
							catch (Exception e2) {
								LOGGER.warn(" For nodeType = " + nType
										+ " Caught exception", e2);
								errArr.add(e2.getMessage());
							}
						}// try block to enclose looping over each single vertex
						catch (Exception exx) {
							LOGGER.warn("WARNING from inside the while-verts-loop ", exx);
						}
						
					} // while loop for each record of a nodeType
					
					if( depNodeTypes.isEmpty() && !dupeCheckOff ){
						// For this nodeType, we haven't looked at the possibility of a 
						// non-dependent node where two verts have same key info
						ArrayList<ArrayList<Vertex>> nonDependentDupeSets = new ArrayList<ArrayList<Vertex>>();
							nonDependentDupeSets = getDupeSets4NonDepNodes( 
										TRANSID, FROMAPPID, g,
										version, nType, tmpList, 
										keyProps, loader );
						// For each set found (each set is for a unique instance of key-values),
						//  process the dupes found
						Iterator<ArrayList<Vertex>> dsItr = nonDependentDupeSets.iterator();
						while( dsItr.hasNext() ){
							ArrayList<Vertex> dupeList =  dsItr.next();
							LOGGER.debug(" - now check Dupes for some non-dependent guys - ");
							List<String> tmpDupeGroups = checkAndProcessDupes(
										TRANSID, FROMAPPID, g, source1, version,
										nType, dupeList, dupeFixOn,
										deleteCandidateList, dupeGroups, loader);
							Iterator<String> dIter = tmpDupeGroups.iterator();
							while (dIter.hasNext()) {
								// Add in any newly found dupes to our running list
								String tmpGrp = dIter.next();
								LOGGER.debug("Found set of dupes: [" + tmpGrp + "]");
								dupeGroups.add(tmpGrp);
							}
						}
						
					}// end of extra dupe check for non-dependent nodes
					
					thisNtDeleteCount = 0;  // Reset for the next pass
					LOGGER.debug( " Processed " + thisNtCount + " records for [" + nType + "], " + totalNodeCount + " total (in window) overall. " );
					
				}// While-loop for each node type
				
			}// end of check to make sure we weren't only supposed to do edges


		  if( !skipEdgeCheckFlag ){
			// ---------------------------------------------------------------
			// Now, we're going to look for one-armed-edges. Ie. an 
			// edge that should have been deleted (because a vertex on 
			// one side was deleted) but somehow was not deleted.
			// So the one end of it points to a vertexId -- but that 
			// vertex is empty.
			// --------------------------------------------------------------

			// To do some strange checking - we need a second graph object
			LOGGER.debug("    ---- NOTE --- about to open a SECOND graph (takes a little while)-------- ");
			// Note - graph2 just reads - but we want it to use a fresh connection to 
			//      the database, so we are NOT using the CACHED DB CONFIG here.
			
			// -- note JanusGraphFactory has been leaving db connections open
			//graph2 = JanusGraphFactory.open(new AAIGraphConfig.Builder(AAIConstants.REALTIME_DB_CONFIG).forService(DataGrooming.class.getSimpleName()).withGraphType("realtime2").buildConfiguration());
			graph2 = AAIGraph.getInstance().getGraph();
			if (graph2 == null) {
				String emsg = "null graph2 object in DataGrooming\n";
				throw new AAIException("AAI_6101", emsg);
			} else {
				LOGGER.debug("Got the graph2 object... ");
			}
			g2 = graph2.newTransaction();
			if (g2 == null) {
				String emsg = "null graphTransaction2 object in DataGrooming\n";
				throw new AAIException("AAI_6101", emsg);
			}
			
			ArrayList<Vertex> vertList = new ArrayList<>();
			Iterator<Vertex> vItor3 = g.traversal().V();
			// Gotta hold these in a List - or else the DB times out as you cycle
			// through these
			while (vItor3.hasNext()) {
				Vertex v = vItor3.next();
				vertList.add(v);
			}
			int counter = 0;
			int lastShown = 0;
			Iterator<Vertex> vItor2 = vertList.iterator();
			LOGGER.debug(" Checking for bad edges  --- ");

			while (vItor2.hasNext()) {
				Vertex v = null;
				try {
					try {
						v = vItor2.next();
					} catch (Exception vex) {
						LOGGER.warn(">>> WARNING trying to get next vertex on the vItor2 ");
						continue;
					}
					
					counter++;
					String thisVertId = "";
					try {
						thisVertId = v.id().toString();
					} catch (Exception ev) {
						LOGGER.warn("WARNING when doing getId() on a vertex from our vertex list.  ");
						continue;
					}
					if (ghostNodeHash.containsKey(thisVertId)) {
						// We already know that this is a phantom node, so don't bother checking it
						LOGGER.debug(" >> Skipping edge check for edges from vertexId = "
										+ thisVertId
										+ ", since that guy is a Phantom Node");
						continue;
					}
					
					if( windowStartTime > 0 ){
						// They are using the time-window, so we only want nodes that are updated after a
						// passed-in timestamp OR that have no last-modified-timestamp which means they are suspicious.
						Object objModTimeStamp = v.property("aai-last-mod-ts").orElse(null);
						if( objModTimeStamp != null ){
							long thisNodeModTime = (long)objModTimeStamp;
							if( thisNodeModTime < windowStartTime ){
								// It has a last modified ts and is NOT in our window, so we can pass over it
								continue;
							}
						}
					}
					
					if (counter == lastShown + 250) {
						lastShown = counter;
						LOGGER.debug("... Checking edges for vertex # "
								+ counter);
					}
					Iterator<Edge> eItor = v.edges(Direction.BOTH);
					while (eItor.hasNext()) {
						Edge e = null;
						Vertex vIn = null;
						Vertex vOut = null;
						try {
							e = eItor.next();
						} catch (Exception iex) {
							LOGGER.warn(">>> WARNING trying to get next edge on the eItor ", iex);
							continue;
						}

						try {
							vIn = e.inVertex();
						} catch (Exception err) {
							LOGGER.warn(">>> WARNING trying to get edge's In-vertex ", err);
						}
						String vNtI = "";
						String vIdI = "";
						Vertex ghost2 = null;
						
						Boolean keysMissing = true;
						Boolean cantGetUsingVid = false;
						if (vIn != null) {
							try {
								Object ob = vIn.<Object>property("aai-node-type").orElse(null);
								if (ob != null) {
									vNtI = ob.toString();
									keysMissing = anyKeyFieldsMissing(vNtI, vIn, loader);
								}
								ob = vIn.id();
								long vIdLong = 0L;
								if (ob != null) {
									vIdI = ob.toString();
									vIdLong = Long.parseLong(vIdI);
								}
								
								if( ! ghost2CheckOff ){
									Vertex connectedVert = g2.traversal().V(vIdLong).next();
									if( connectedVert == null ) {
										LOGGER.warn( "GHOST2 -- got NULL when doing getVertex for vid = " + vIdLong);
										cantGetUsingVid = true;
										
										// If we can NOT get this ghost with the SECOND graph-object, 
										// it is still a ghost since even though we can get data about it using the FIRST graph 
										// object.  
										
										try {
											 ghost2 = g.traversal().V(vIdLong).next();
										}
										catch( Exception ex){
											LOGGER.warn( "GHOST2 --  Could not get the ghost info for a bad edge for vtxId = " + vIdLong, ex);
										}
										if( ghost2 != null ){
											ghostNodeHash.put(vIdI, ghost2);
										}
									}
								}// end of the ghost2 checking
							} 
							catch (Exception err) {
								LOGGER.warn(">>> WARNING trying to get edge's In-vertex props ", err);
							}
						}
						
						if (keysMissing || vIn == null || vNtI.equals("")
								|| cantGetUsingVid) {
							// this is a bad edge because it points to a vertex
							// that isn't there anymore or is corrupted
							String thisEid = e.id().toString();
							if (deleteCandidateList.contains(thisEid) || deleteCandidateList.contains(vIdI)) {
								boolean okFlag = true;
								if (!vIdI.equals("")) {
									// try to get rid of the corrupted vertex
									try {
										if( (ghost2 != null) && ghost2FixOn ){
											ghost2.remove();
										}
										else {
											vIn.remove();
										}
										executeFinalCommit = true;
										deleteCount++;
									} catch (Exception e1) {
										okFlag = false;
										LOGGER.warn("WARNING when trying to delete bad-edge-connected VERTEX VID = "
												+ vIdI, e1);
									}
									if (okFlag) {
										LOGGER.debug(" DELETED vertex from bad edge = "
														+ vIdI);
									}
								} else {
									// remove the edge if we couldn't get the
									// vertex
									try {
										e.remove();
										executeFinalCommit = true;
										deleteCount++;
									} catch (Exception ex) {
										// NOTE - often, the exception is just
										// that this edge has already been
										// removed
										okFlag = false;
										LOGGER.warn("WARNING when trying to delete edge = "
												+ thisEid);
									}
									if (okFlag) {
										LOGGER.debug(" DELETED edge = " + thisEid);
									}
								}
							} else {
								oneArmedEdgeHash.put(thisEid, e);
								if ((vIn != null) && (vIn.id() != null)) {
									emptyVertexHash.put(thisEid, vIn.id()
											.toString());
								}
							}
						}

						try {
							vOut = e.outVertex();
						} catch (Exception err) {
							LOGGER.warn(">>> WARNING trying to get edge's Out-vertex ");
						}
						String vNtO = "";
						String vIdO = "";
						ghost2 = null;
						keysMissing = true;
						cantGetUsingVid = false;
						if (vOut != null) {
							try {
								Object ob = vOut.<Object>property("aai-node-type").orElse(null);
								if (ob != null) {
									vNtO = ob.toString();
									keysMissing = anyKeyFieldsMissing(vNtO,
											vOut, loader);
								}
								ob = vOut.id();
								long vIdLong = 0L;
								if (ob != null) {
									vIdO = ob.toString();
									vIdLong = Long.parseLong(vIdO);
								}
								
								if( ! ghost2CheckOff ){
									Vertex connectedVert = g2.traversal().V(vIdLong).next();
									if( connectedVert == null ) {
										cantGetUsingVid = true;
										LOGGER.debug( "GHOST2 -- got NULL when doing getVertex for vid = " + vIdLong);
										// If we can get this ghost with the other graph-object, then get it -- it's still a ghost
										try {
											 ghost2 = g.traversal().V(vIdLong).next();
										}
										catch( Exception ex){
											LOGGER.warn( "GHOST2 -- Could not get the ghost info for a bad edge for vtxId = " + vIdLong, ex);
										}
										if( ghost2 != null ){
											ghostNodeHash.put(vIdO, ghost2);
										}
									}
								}
							} catch (Exception err) {
								LOGGER.warn(">>> WARNING trying to get edge's Out-vertex props ", err);
							}
						}
						if (keysMissing || vOut == null || vNtO.equals("")
								|| cantGetUsingVid) {
							// this is a bad edge because it points to a vertex
							// that isn't there anymore
							String thisEid = e.id().toString();
							if (deleteCandidateList.contains(thisEid) || deleteCandidateList.contains(vIdO)) {
								boolean okFlag = true;
								if (!vIdO.equals("")) {
									// try to get rid of the corrupted vertex
									try {
										if( (ghost2 != null) && ghost2FixOn ){
											ghost2.remove();
										}
										else if (vOut != null) {
											vOut.remove();
										}
										executeFinalCommit = true;
										deleteCount++;
									} catch (Exception e1) {
										okFlag = false;
										LOGGER.warn("WARNING when trying to delete bad-edge-connected VID = "
												+ vIdO, e1);
									}
									if (okFlag) {
										LOGGER.debug(" DELETED vertex from bad edge = "
														+ vIdO);
									}
								} else {
									// remove the edge if we couldn't get the
									// vertex
									try {
										e.remove();
										executeFinalCommit = true;
										deleteCount++;
									} catch (Exception ex) {
										// NOTE - often, the exception is just
										// that this edge has already been
										// removed
										okFlag = false;
										LOGGER.warn("WARNING when trying to delete edge = "
												+ thisEid, ex);
									}
									if (okFlag) {
										LOGGER.debug(" DELETED edge = " + thisEid);
									}
								}
							} else {
								oneArmedEdgeHash.put(thisEid, e);
								if ((vOut != null) && (vOut.id() != null)) {
									emptyVertexHash.put(thisEid, vOut.id()
											.toString());
								}
							}
						}
					}// End of while-edges-loop
				} catch (Exception exx) {
					LOGGER.warn("WARNING from in the while-verts-loop ", exx);
				}
			}// End of while-vertices-loop (the edge-checking)
			LOGGER.debug(" Done checking for bad edges  --- ");
		  }	// end of -- if we're not skipping the edge-checking
			

			deleteCount = deleteCount + dupeGrpsDeleted;
			if (deleteCount > 0 || dummyUpdCount > 0 || indexUpdCount > 0){
				executeFinalCommit = true;
			}

			int ghostNodeCount = ghostNodeHash.size();
			int orphanNodeCount = orphanNodeHash.size();
			int oneArmedEdgeCount = oneArmedEdgeHash.size();
			int missingAaiNtNodeCount = missingAaiNtNodeHash.size();
			int badUriNodeCount = badUriNodeHash.size();
			int badIndexNodeCount = badIndexNodeHash.size();
			int dupeCount = dupeGroups.size();

			deleteCount = deleteCount + dupeGrpsDeleted;

			bw.write("\n\n ============ Summary ==============\n");
			if( timeWindowMinutes == 0 ){
				bw.write("Ran FULL data grooming (no time-window). \n");
			}
			else {
				bw.write("Ran PARTIAL data grooming just looking at data added/updated in the last " + timeWindowMinutes + " minutes. \n");
			}
			
			bw.write("\nRan these nodeTypes: " + ntList + "\n\n");
			bw.write("There were this many delete candidates from previous run =  "
					+ deleteCandidateList.size() + "\n");
			if (dontFixOrphansFlag) {
				bw.write(" Note - we are not counting orphan nodes since the -dontFixOrphans parameter was used. \n");
			}
			bw.write("Deleted this many delete candidates =  " + deleteCount
					+ "\n");
			bw.write("Dummy-index-update to delete candidates =  " + dummyUpdCount
					+ "\n");
			bw.write("index-update-Fix Attempts to phantom nodes =  " + indexUpdCount
					+ "\n");
			bw.write("Total number of nodes looked at =  " + totalNodeCount
					+ "\n");
			bw.write("Ghost Nodes identified = " + ghostNodeCount + "\n");
			bw.write("Orphan Nodes identified =  " + orphanNodeCount + "\n");
			bw.write("Missing aai-node-type Nodes identified =  " + missingAaiNtNodeCount + "\n");
			bw.write("Bad Edges identified =  " + oneArmedEdgeCount + "\n");
			bw.write("Bad aai-uri property Nodes identified =  " + badUriNodeCount + "\n");
			bw.write("Bad index property Nodes identified =  " + badIndexNodeCount + "\n");
			bw.write("Duplicate Groups count =  " + dupeCount + "\n");
			bw.write("MisMatching Label/aai-node-type count =  "
					+ misMatchedHash.size() + "\n");

			bw.write("\n ------------- Delete Candidates ---------\n");
			for (Map.Entry<String, Vertex> entry : ghostNodeHash
					.entrySet()) {
				String vid = entry.getKey();
				bw.write("DeleteCandidate: Phantom Vid = [" + vid + "]\n");
				cleanupCandidateCount++;
			}
			for (Map.Entry<String, Vertex> entry : missingAaiNtNodeHash
					.entrySet()) {
				String vid = entry.getKey();
				bw.write("DeleteCandidate: Missing aai-node-type Vid = [" + vid + "]\n");
				cleanupCandidateCount++;
			}
			for (Map.Entry<String, Vertex> entry : orphanNodeHash
					.entrySet()) {
				String vid = entry.getKey();
				bw.write("DeleteCandidate: OrphanDepNode Vid = [" + vid + "]\n");
				if (!dontFixOrphansFlag) {
					cleanupCandidateCount++;
				}
			}
			for (Map.Entry<String, Edge> entry : oneArmedEdgeHash.entrySet()) {
				String eid = entry.getKey();
				bw.write("DeleteCandidate: Bad EDGE Edge-id = [" + eid + "]\n");
				cleanupCandidateCount++;
			}
			
			

			bw.write("\n-- NOTE - To see DeleteCandidates for Duplicates, you need to look in the Duplicates Detail section below.\n");

			bw.write("\n ------------- GHOST NODES - detail ");
			for (Map.Entry<String, Vertex> entry : ghostNodeHash
					.entrySet()) {
				try {
					String vid = entry.getKey();
					bw.write("\n ==> Phantom Vid = " + vid + "\n");
					ArrayList<String> retArr = showPropertiesForNode(
							TRANSID, FROMAPPID, entry.getValue());
					for (String info : retArr) {
						bw.write(info + "\n");
					}
					retArr = showAllEdgesForNode(TRANSID, FROMAPPID,
							entry.getValue());
					for (String info : retArr) {
						bw.write(info + "\n");
					}
				} catch (Exception dex) {
					LOGGER.debug("error trying to print detail info for a ghost-node:  " + LogFormatTools.getStackTop(dex));
				}
			}

			bw.write("\n ------------- Missing aai-node-type NODES - detail ");
			for (Map.Entry<String, Vertex> entry : missingAaiNtNodeHash
					.entrySet()) {
				try {
					String vid = entry.getKey();
					bw.write("\n> Missing aai-node-type Node Vid = " + vid + "\n");
					ArrayList<String> retArr = showPropertiesForNode(
							TRANSID, FROMAPPID, entry.getValue());
					for (String info : retArr) {
						bw.write(info + "\n");
					}

					retArr = showAllEdgesForNode(TRANSID, FROMAPPID,
							entry.getValue());
					for (String info : retArr) {
						bw.write(info + "\n");
					}
				} catch (Exception dex) {
					LOGGER.debug("error trying to print detail info for a node missing its aai-node-type  " + LogFormatTools.getStackTop(dex));
				}
			}
			
			bw.write("\n ------------- Nodes where aai-uri property is bad - detail ");
			for (Map.Entry<String, Vertex> entry : badUriNodeHash
					.entrySet()) {
				try {
					String vid = entry.getKey();
					bw.write("\n> Has Bad aai-uri - Vid = " + vid + "\n");
					ArrayList<String> retArr = showPropertiesForNode(
							TRANSID, FROMAPPID, entry.getValue());
					for (String info : retArr) {
						bw.write(info + "\n");
					}

					retArr = showAllEdgesForNode(TRANSID, FROMAPPID,
							entry.getValue());
					for (String info : retArr) {
						bw.write(info + "\n");
					}
				} catch (Exception dex) {
					LOGGER.debug("error trying to print detail info for a node with a bad aai-uri  " + LogFormatTools.getStackTop(dex));
				}
			}
					
			bw.write("\n ------------- Nodes where an indexed property is bad - detail: ");
			for (Map.Entry<String, Vertex> entry : badIndexNodeHash
					.entrySet()) {
				try {
					String vid = entry.getKey();
					bw.write("\n> Node with bad index - Vid = " + vid + "\n");
					ArrayList<String> retArr = showPropertiesForNode(
							TRANSID, FROMAPPID, entry.getValue());
					for (String info : retArr) {
						bw.write(info + "\n");
					}

					retArr = showAllEdgesForNode(TRANSID, FROMAPPID,
							entry.getValue());
					for (String info : retArr) {
						bw.write(info + "\n");
					}
				} catch (Exception dex) {
					LOGGER.debug("error trying to print detail info for a node with bad index  " + LogFormatTools.getStackTop(dex));
				}
			}

			bw.write("\n ------------- Missing Dependent Edge ORPHAN NODES - detail: ");
			for (Map.Entry<String, Vertex> entry : orphanNodeHash
					.entrySet()) {
				try {
					String vid = entry.getKey();
					bw.write("\n> Orphan Node Vid = " + vid + "\n");
					ArrayList<String> retArr = showPropertiesForNode(
							TRANSID, FROMAPPID, entry.getValue());
					for (String info : retArr) {
						bw.write(info + "\n");
					}
	
					retArr = showAllEdgesForNode(TRANSID, FROMAPPID,
							entry.getValue());
					for (String info : retArr) {
						bw.write(info + "\n");
					}
				} catch (Exception dex) {
					LOGGER.debug("error trying to print detail info for a Orphan Node /missing dependent edge " + LogFormatTools.getStackTop(dex));
				}
			}

			bw.write("\n ------------- EDGES pointing to empty/bad vertices: ");
			for (Map.Entry<String, Edge> entry : oneArmedEdgeHash.entrySet()) {
				try {
					String eid = entry.getKey();
					Edge thisE = entry.getValue();
					String badVid = emptyVertexHash.get(eid);
					bw.write("\n>  Edge pointing to bad vertex (Vid = "
							+ badVid + ") EdgeId = " + eid + "\n");
					bw.write("Label: [" + thisE.label() + "]\n");
					Iterator<Property<Object>> pI = thisE.properties();
					while (pI.hasNext()) {
						Property<Object> propKey = pI.next();
						bw.write("Prop: [" + propKey + "], val = ["
								+ propKey.value() + "]\n");
					}
				} catch (Exception pex) {
					LOGGER.debug("error trying to print empty/bad vertex data: " + LogFormatTools.getStackTop(pex));
				}
			}

			bw.write("\n ------------- Duplicates: ");
			Iterator<String> dupeIter = dupeGroups.iterator();
			int dupeSetCounter = 0;
			while (dupeIter.hasNext()) {
				dupeSetCounter++;
				String dset = (String) dupeIter.next();

				bw.write("\n --- Duplicate Group # " + dupeSetCounter
						+ " Detail -----------\n");
				try {
					// We expect each line to have at least two vid's, followed
					// by the preferred one to KEEP
					String[] dupeArr = dset.split("\\|");
					ArrayList<String> idArr = new ArrayList<>();
					int lastIndex = dupeArr.length - 1;
					for (int i = 0; i <= lastIndex; i++) {
						if (i < lastIndex) {
							// This is not the last entry, it is one of the
							// dupes, so we want to show all its info
							bw.write("    >> Duplicate Group # "
									+ dupeSetCounter + "  Node # " + i
									+ " ----\n");
							String vidString = dupeArr[i];
							idArr.add(vidString);
							long longVertId = Long.parseLong(vidString);
							Iterator<Vertex> vtxIterator = g.vertices(longVertId);
							Vertex vtx = null;
							if (vtxIterator.hasNext()) {
								vtx = vtxIterator.next();
							}
							ArrayList<String> retArr = showPropertiesForNode(TRANSID, FROMAPPID, vtx);
							for (String info : retArr) {
								bw.write(info + "\n");
							}

							retArr = showAllEdgesForNode(TRANSID,
									FROMAPPID, vtx);
							for (String info : retArr) {
								bw.write(info + "\n");
							}
						} else {
							// This is the last entry which should tell us if we
							// have a preferred keeper
							String prefString = dupeArr[i];
							if (prefString.equals("KeepVid=UNDETERMINED")) {
								bw.write("\n For this group of duplicates, could not tell which one to keep.\n");
								bw.write(" >>> This group needs to be taken care of with a manual/forced-delete.\n");
							} else {
								// If we know which to keep, then the prefString
								// should look like, "KeepVid=12345"
								String[] prefArr = prefString.split("=");
								if (prefArr.length != 2
										|| (!prefArr[0].equals("KeepVid"))) {
									throw new Exception("Bad format. Expecting KeepVid=999999");
								} else {
									String keepVidStr = prefArr[1];
									if (idArr.contains(keepVidStr)) {
										bw.write("\n The vertex we want to KEEP has vertexId = "
												+ keepVidStr);
										bw.write("\n The others become delete candidates: \n");
										idArr.remove(keepVidStr);
										for (int x = 0; x < idArr.size(); x++) {
											cleanupCandidateCount++;
											bw.write("DeleteCandidate: Duplicate Vid = ["
													+ idArr.get(x) + "]\n");
										}
									} else {
										throw new Exception("ERROR - Vertex Id to keep not found in list of dupes.  dset = ["
												+ dset + "]");
									}
								}
							}// else we know which one to keep
						}// else last entry
					}// for each vertex in a group
				} catch (Exception dex) {
					LOGGER.debug("error trying to print duplicate vertex data " + LogFormatTools.getStackTop(dex));
				}

			}// while - work on each group of dupes

			bw.write("\n ------------- Mis-matched Label/aai-node-type Nodes: \n ");
			for (Map.Entry<String, String> entry : misMatchedHash.entrySet()) {
				String msg = entry.getValue();
				bw.write("MixedMsg = " + msg + "\n");
			}

			bw.write("\n ------------- Got these errors while processing: \n");
			Iterator<String> errIter = errArr.iterator();
			while (errIter.hasNext()) {
				String line = (String) errIter.next();
				bw.write(line + "\n");
			}

			bw.close();

			LOGGER.debug(" ------------- Done doing all the checks ------------ ");
			LOGGER.debug("Output will be written to " + fullOutputFileName);

			if (cleanupCandidateCount > 0 || badUriNodeCount > 0 || badIndexNodeCount > 0) {
				// Technically, this is not an error -- but we're throwing this
				// error so that hopefully a
				// monitoring system will pick it up and do something with it.
				throw new AAIException("AAI_6123", "See file: [" + fullOutputFileName
						+ "] and investigate delete candidates. ");
			}
		} catch (AAIException e) {
			LOGGER.debug("Caught AAIException while grooming data");
			ErrorLogHelper.logException(e);
		} catch (Exception ex) {
			LOGGER.debug("Caught exception while grooming data");
			ErrorLogHelper.logError("AAI_6128", ex.getMessage() + ", resolve and rerun dataGrooming");
		} finally {

			if (bw != null) {
				try {
					bw.close();
				} catch (IOException iox) {
					LOGGER.warn("Got an IOException trying to close bufferedWriter() ", iox);
				}
			}

			if (executeFinalCommit) {
				// If we were holding off on commits till the end - then now is the time.
				if( g == null ){
					LOGGER.debug(" >>>> ERROR <<<<   Could not commit changes. graph was null when we wanted to commit.");
				}
				else if( !g.tx().isOpen() ){
					LOGGER.debug(" >>>> ERROR <<<<   Could not commit changes. Transaction was not open when we wanted to commit.");
				}
				else {
					try {
						LOGGER.debug("About to do the commit for "
							+ deleteCount + " removes. ");
						g.tx().commit();
						LOGGER.debug("Commit was successful ");
					} catch (Exception excom) {
						LOGGER.debug(" >>>> ERROR <<<<   Could not commit changes. " + LogFormatTools.getStackTop(excom));
						deleteCount = 0;
					}
				}
			}
			else if (g != null && g.tx().isOpen()) {
				try {
					// We did not do any deletes that need to be committed.
					// The rollback is to clear out the transaction used while doing those reads
					g.tx().rollback();
				} catch (Exception ex) {
					// Don't throw anything because JanusGraph sometimes is just saying that the graph is already closed
					LOGGER.warn("WARNING from final graphTransaction.rollback()", ex);
				}
			}
			
			if (g2 != null && g2.tx().isOpen()) {
				try {
					// We only read on g2.  The rollback is to clear out the transaction used while doing those reads
					g2.tx().rollback();
				} catch (Exception ex) {
					// Don't throw anything because JanusGraph sometimes is just saying that the graph is already closed
					LOGGER.warn("WARNING from final graphTransaction2.rollback()", ex);
				}
			}
				
			if( finalShutdownFlag ){
				try {
					if( graph != null && graph.isOpen() ){
						graph.tx().close();
						if( "true".equals(System.getProperty("org.onap.aai.graphadmin.started"))) {
							// Since dataGrooming was called from a scheduled task - do not call graph.close() 
						}
						else {
							// DataGrooming must have been called manually - so we need to call close().
							graph.close();
						}
					}
				} catch (Exception ex) {
					// Don't throw anything because JanusGraph sometimes is just saying that the graph is already closed{
					LOGGER.warn("WARNING from final graph.shutdown()", ex);
				}
				
				try {
					if( graph2 != null && graph2.isOpen() ){
						graph2.tx().close();
						if( "true".equals(System.getProperty("org.onap.aai.graphadmin.started"))) {
							// Since dataGrooming was called from a scheduled task - do not call graph2.close() 
						}
						else {
							// DataGrooming must have been called manually - so we need to call close().
							graph2.close();
						}
					}
				} catch (Exception ex) {
					// Don't throw anything because JanusGraph sometimes is just saying that the graph is already closed{
					LOGGER.warn("WARNING from final graph2.shutdown()", ex);
				}
			}
				
		}

		return cleanupCandidateCount;

	}// end of doTheGrooming()
	
	
	public void tryToReSetIndexedProps(Vertex thisVtx, String thisVidStr, ArrayList <String> indexedProps) {
		// Note - This is for when a node looks to be a phantom (ie. an index/pointer problem)
	    // We will only deal with properties that are indexed and have a value - and for those,
	    // we will re-set them to the same value they already have, so that hopefully if their 
	    // index was broken, it may get re-set.
		
		// NOTE -- as of 1902-P2, this is deprecated --------------
	       
		LOGGER.debug(" We will try to re-set the indexed properties for this node without changing any property values.  VID = " + thisVidStr );
		// These reserved-prop-names are all indexed for all nodes
		
		ArrayList <String> propList = new ArrayList <String> ();
		propList.addAll(indexedProps);
		// Add in the global props that we'd also like to reset
		propList.add("aai-node-type");
		propList.add("aai-uri");
		propList.add("aai-uuid");
		Iterator<String> propNameItr = propList.iterator();
		while( propNameItr.hasNext() ){
			String propName = propNameItr.next();
			try {
				Object valObj = thisVtx.<Object>property(propName).orElse(null);
				if( valObj != null ){
					LOGGER.debug(" We will try resetting prop [" + propName 
							+ "], to val = [" + valObj.toString() + "] for VID = " + thisVidStr);
					thisVtx.property(propName, valObj);
				}
			} catch (Exception ex ){
				// log that we did not re-set this property
				LOGGER.debug("DEBUG - Exception while trying to re-set the indexed properties for this node: VID = " 
				+ thisVidStr + ".  exception msg = [" + ex.getMessage() + "]" );
			}
		}
	}
	          
	          
	 public void updateIndexedPropsForMissingNT(Vertex thisVtx, String thisVidStr, String nType,
			HashMap <String,String>propTypeHash, ArrayList <String> indexedProps) {
		// This is for the very specific "missing-aai-node-type" scenario.
		// That is: a node that does not have the "aai-node-type" property, but still has
		//     an aai-node-type Index pointing to it and is an orphan node.   Nodes like this
		//     are (probably) the result of a delete request that got hosed.
		// Other indexes may also be messed up, so we will update all of them on
		//    this pass.  A future pass will just treat this node like a regular orphan
		//    and delete it (if appropriate).
		 
		LOGGER.debug("  We will be updating the indexed properties for this node to dummy values.  VID = " + thisVidStr );
		String dummyPropValStr = thisVidStr + "dummy";
		// These reserved-prop-names are all indexed for all nodes
		thisVtx.property("aai-node-type",nType);
		thisVtx.property("aai-uri", dummyPropValStr);
		thisVtx.property("aai-unique-key", dummyPropValStr);
		thisVtx.property("aai-uuid", dummyPropValStr);
		thisVtx.property("source-of-truth", dummyPropValStr);
		Iterator<String> indexedPropI = indexedProps.iterator();
		while (indexedPropI.hasNext()) {
			String propName = indexedPropI.next();
			// Using the VID in case this property is unique in the db and
			// we're doing this kind of thing on more than one of these nodes..
			String dataType = propTypeHash.get(propName);
			if( dataType == null || dataType.toLowerCase().endsWith(".string") ){
				thisVtx.property(propName, dummyPropValStr);
			}
			else if( dataType.toLowerCase().endsWith(".long") ){
				Long thisVidLong = (Long) thisVtx.id();
				thisVtx.property(propName, thisVidLong);
			}
			else if( dataType.toLowerCase().endsWith(".boolean") ){
				thisVtx.property(propName, false);
			}
			else if( dataType.toLowerCase().endsWith(".integer") ){
				thisVtx.property(propName, 9999);
			}
			else {
				// Not sure what it is - try a string
				thisVtx.property(propName, dummyPropValStr);
			}
		}
	}

	/**
	 * Vertex has these keys.
	 *
	 * @param tmpV the tmp V
	 * @param propHashWithKeys the prop hash with keys
	 * @return the boolean
	 */
	private Boolean vertexHasTheseKeys( Vertex tmpV, HashMap <String, Object> propHashWithKeys) {
		Iterator <?> it = propHashWithKeys.entrySet().iterator();
		while( it.hasNext() ){
			String propName = "";
			String propVal = "";
			Map.Entry <?,?>propEntry = (Map.Entry<?,?>)it.next();
			Object propNameObj = propEntry.getKey();
			if( propNameObj != null ){
				propName = propNameObj.toString();
			}
			Object propValObj = propEntry.getValue();
			if( propValObj != null ){
				propVal = propValObj.toString();
			}
			Object checkValObj = tmpV.<Object>property(propName).orElse(null);
			if( checkValObj == null ) {
				return false;
			}
			else if( !propVal.equals(checkValObj.toString()) ){
				return false;
			}
		}
		return true;
	}	
	
	
	/**
	 * Any key fields missing.
	 *
	 * @param nType the n type
	 * @param v the v
	 * @return the boolean
	 */
	private Boolean anyKeyFieldsMissing(String nType, Vertex v, Loader loader) {
		
		try {
			Introspector obj = null;
			try {
				obj = loader.introspectorFromName(nType);
			} catch (AAIUnknownObjectException e) {
				// They gave us a non-empty nodeType but our NodeKeyProps does
				//   not have data for it.  Since we do not know what the
				//   key params are for this type of node, we will just
				//   return "false".
				String emsg = " -- WARNING -- Unrecognized nodeType: [" + nType 
						+ "].  We cannot determine required keys for this nType. ";
				// NOTE - this will be caught below and a "false" returned
				throw new AAIException("AAI_6121", emsg);
			}	
			
			// Determine what the key fields are for this nodeType
			Collection <String> keyPropNamesColl = obj.getKeys();
			Iterator<String> keyPropI = keyPropNamesColl.iterator();
			while (keyPropI.hasNext()) {
				String propName = keyPropI.next();
				Object ob = v.<Object>property(propName).orElse(null);
				if (ob == null || ob.toString().equals("")) {
					// It is missing a key property
					String thisVertId = v.id().toString();
					LOGGER.debug(" -- Vid = " + thisVertId 
							+ ",nType = [" + nType + "], is missing keyPropName = [" + propName + "]");
					return true;
				}
			}
			Object ob = v.<Object>property("aai-uri").orElse(null);
			if (ob == null || ob.toString().equals("")) {
				// It is missing a key property
				String thisVertId = v.id().toString();
				LOGGER.debug(" -- Vid = " + thisVertId 
						+ ",nType = [" + nType + "], is missing its [aai-uri] property");
				return true;
			}
		} catch (AAIException e) {
			// Something was wrong -- but since we weren't able to check
			// the keys, we will not declare that it is missing keys.
			return false;
		}
		return false;
	}
	

	/**
	 * Gets the delete list.
	 *
	 * @param targetDir the target dir
	 * @param fileName the file name
	 * @param edgesOnlyFlag the edges only flag
	 * @param dontFixOrphans the dont fix orphans
	 * @param dupeFixOn the dupe fix on
	 * @return the delete list
	 * @throws AAIException the AAI exception
	 */
	private Set<String> getDeleteList(String targetDir,
			String fileName, Boolean edgesOnlyFlag, Boolean dontFixOrphans,
			Boolean dupeFixOn) throws AAIException {

		// Look in the file for lines formated like we expect - pull out any
		// Vertex Id's to delete on this run
		Set<String> delList = new LinkedHashSet<>();
		String fullFileName = targetDir + AAIConstants.AAI_FILESEP + fileName;

		try(BufferedReader br = new BufferedReader(new FileReader(fullFileName))) {
			String line = br.readLine();
			while (line != null) {
				if (!"".equals(line) && line.startsWith("DeleteCandidate")) {
					if (edgesOnlyFlag && (!line.contains("Bad Edge"))) {
						// We're only processing edges and this line is not for an edge
					} else if (dontFixOrphans && line.contains("Orphan")) {
						// We're not going to process orphans
					} else if (!dupeFixOn && line.contains("Duplicate")) {
						// We're not going to process Duplicates
					} else {
						int begIndex = line.indexOf("id = ");
						int endIndex = line.indexOf("]");
						String vidVal = line.substring(begIndex + 6, endIndex);
						delList.add(vidVal);
					}
				}
				line = br.readLine();
			}
			br.close();
		} catch (IOException e) {
			throw new AAIException("AAI_6124", e, "Could not open input-file [" + fullFileName
					+ "], exception= " + e.getMessage());
		}

		return delList;

	}// end of getDeleteList

	/**
	 * Gets the preferred dupe.
	 *
	 * @param transId the trans id
	 * @param fromAppId the from app id
	 * @param g the g
	 * @param dupeVertexList the dupe vertex list
	 * @param ver the ver
	 * @return Vertex
	 * @throws AAIException the AAI exception
	 */
	public Vertex getPreferredDupe(String transId,
			String fromAppId, GraphTraversalSource g,
			ArrayList<Vertex> dupeVertexList, String ver, Loader loader)
			throws AAIException {

		// This method assumes that it is being passed a List of 
		// vertex objects which violate our uniqueness constraints.
		// Note - returning a null vertex means we could not 
		//   safely pick one to keep (Ie. safely know which to delete.)
		Vertex nullVtx = null;  

		if (dupeVertexList == null) {
			return nullVtx;
		}
		int listSize = dupeVertexList.size();
		if (listSize == 0) {
			return nullVtx;
		}
		if (listSize == 1) {
			return (dupeVertexList.get(0));
		}

		// If they don't all have the same aai-uri, then we will not 
		// choose between them - we'll need someone to manually 
		// check to pick which one makes sense to keep.
		Object uriOb = dupeVertexList.get(0).<Object>property("aai-uri").orElse(null);
		if( uriOb == null || uriOb.toString().equals("") ){
			// this is a bad node - hopefully will be picked up by phantom checker
			return nullVtx;
		}
		String thisUri = uriOb.toString();
		for (int i = 1; i < listSize; i++) {
			uriOb = dupeVertexList.get(i).<Object>property("aai-uri").orElse(null);
			if( uriOb == null || uriOb.toString().equals("") ){
				// this is a bad node - hopefully will be picked up by phantom checker
				return nullVtx;
			}
			String nextUri = uriOb.toString();
			if( !thisUri.equals(nextUri)){
				// there are different URI's on these - so we can't pick 
				// a dupe to keep.  Someone will need to look at it.
				return nullVtx;
			}
		}
		
		// Compare them two at a time to see if we can tell which out of 
		// the batch to keep.
		Vertex vtxPreferred = null;
		Vertex currentFaveVtx = dupeVertexList.get(0);
		for (int i = 1; i < listSize; i++) {
			Vertex vtxB = dupeVertexList.get(i);
			vtxPreferred = pickOneOfTwoDupes(transId, fromAppId, g,
					currentFaveVtx, vtxB, ver, loader);
			if (vtxPreferred == null) {
				// We couldn't choose one
				return nullVtx;
			} else {
				currentFaveVtx = vtxPreferred;
			}
		}

		if( currentFaveVtx != null && checkAaiUriOk(g, currentFaveVtx) ){
			return (currentFaveVtx);
		}
		else {
			// We had a preferred vertex, but its aai-uri was bad, so
			// we will not recommend one to keep.
			return nullVtx;
		}

	} // end of getPreferredDupe()

	/**
	 * Pick one of two dupes.
	 *
	 * @param transId the trans id
	 * @param fromAppId the from app id
	 * @param g the g
	 * @param vtxA the vtx A
	 * @param vtxB the vtx B
	 * @param ver the ver
	 * @return Vertex
	 * @throws AAIException the AAI exception
	 */
	public Vertex pickOneOfTwoDupes(String transId,
			String fromAppId, GraphTraversalSource g, Vertex vtxA,
			Vertex vtxB, String ver, Loader loader) throws AAIException {

		Vertex nullVtx = null;
		Vertex preferredVtx = null;

		Long vidA = new Long(vtxA.id().toString());
		Long vidB = new Long(vtxB.id().toString());

		String vtxANodeType = "";
		String vtxBNodeType = "";
		Object objType = vtxA.<Object>property("aai-node-type").orElse(null);
		if (objType != null) {
			vtxANodeType = objType.toString();
		}
		objType = vtxB.<Object>property("aai-node-type").orElse(null);
		if (objType != null) {
			vtxBNodeType = objType.toString();
		}

		if (vtxANodeType.equals("") || (!vtxANodeType.equals(vtxBNodeType))) {
			// Either they're not really dupes or there's some bad data - so
			// don't pick one
			return nullVtx;
		}

		// Check that node A and B both have the same key values (or else they
		// are not dupes)
		// (We'll check dep-node later)
		// Determine what the key fields are for this nodeType
		Collection <String> keyProps = new ArrayList <>();
		HashMap <String,Object> keyPropValsHash = new HashMap <String,Object>();
		try {
			keyProps = loader.introspectorFromName(vtxANodeType).getKeys();
		} catch (AAIUnknownObjectException e) {
			LOGGER.warn("Required property not found", e);
			throw new AAIException("AAI_6105", "Required Property name(s) not found for nodeType = " + vtxANodeType + ")");
		}
		
		Iterator<String> keyPropI = keyProps.iterator();
		while (keyPropI.hasNext()) {
			String propName = keyPropI.next();
			String vtxAKeyPropVal = "";
			objType = vtxA.<Object>property(propName).orElse(null);
			if (objType != null) {
				vtxAKeyPropVal = objType.toString();
			}
			String vtxBKeyPropVal = "";
			objType = vtxB.<Object>property(propName).orElse(null);
			if (objType != null) {
				vtxBKeyPropVal = objType.toString();
			}

			if (vtxAKeyPropVal.equals("")
					|| (!vtxAKeyPropVal.equals(vtxBKeyPropVal))) {
				// Either they're not really dupes or they are missing some key
				// data - so don't pick one
				return nullVtx;
			}
			else {
				// Keep these around for (potential) use later
				keyPropValsHash.put(propName, vtxAKeyPropVal);
			}
			     
		}

		// Collect the vid's and aai-node-types of the vertices that each vertex
		// (A and B) is connected to.
		ArrayList<String> vtxIdsConn2A = new ArrayList<>();
		ArrayList<String> vtxIdsConn2B = new ArrayList<>();
		HashMap<String, String> nodeTypesConn2A = new HashMap<>();
		HashMap<String, String> nodeTypesConn2B = new HashMap<>();

		ArrayList<Vertex> vertListA = getConnectedNodes( g, vtxA );
		if (vertListA != null) {
			Iterator<Vertex> iter = vertListA.iterator();
			while (iter.hasNext()) {
				Vertex tvCon = iter.next();
				String conVid = tvCon.id().toString();
				String nt = "";
				objType = tvCon.<Object>property("aai-node-type").orElse(null);
				if (objType != null) {
					nt = objType.toString();
				}
				nodeTypesConn2A.put(nt, conVid);
				vtxIdsConn2A.add(conVid);
			}
		}

		ArrayList<Vertex> vertListB = getConnectedNodes( g, vtxB );
		if (vertListB != null) {
			Iterator<Vertex> iter = vertListB.iterator();
			while (iter.hasNext()) {
				Vertex tvCon = iter.next();
				String conVid = tvCon.id().toString();
				String nt = "";
				objType = tvCon.<Object>property("aai-node-type").orElse(null);
				if (objType != null) {
					nt = objType.toString();
				}
				nodeTypesConn2B.put(nt, conVid);
				vtxIdsConn2B.add(conVid);
			}
		}

		// 1 - If this kind of node needs a dependent node for uniqueness, then
		//    verify that they both nodes point to the same dependent 
		//    node (otherwise they're not really duplicates)
		// Note - there are sometimes more than one dependent node type since
		//    one nodeType can be used in different ways. But for a 
		//    particular node, it will only have one dependent node that 
		//    it's connected to.
		String onlyNodeThatIndexPointsToVidStr = "";
		Collection<String> depNodeTypes = loader.introspectorFromName(vtxANodeType).getDependentOn();
		if (depNodeTypes.isEmpty()) {
			// This kind of node is not dependent on any other. That is ok.
			// We need to find out if the unique index info is good or not and
			// use that later when deciding if we can delete one.
			onlyNodeThatIndexPointsToVidStr = findJustOneUsingIndex( transId,
					fromAppId, g, keyPropValsHash, vtxANodeType, vidA, vidB, ver );
		} else {
			String depNodeVtxId4A = "";
			String depNodeVtxId4B = "";
			Iterator<String> iter = depNodeTypes.iterator();
			while (iter.hasNext()) {
				String depNodeType = iter.next();
				if (nodeTypesConn2A.containsKey(depNodeType)) {
					// This is the dependent node type that vertex A is using
					depNodeVtxId4A = nodeTypesConn2A.get(depNodeType);
				}
				if (nodeTypesConn2B.containsKey(depNodeType)) {
					// This is the dependent node type that vertex B is using
					depNodeVtxId4B = nodeTypesConn2B.get(depNodeType);
				}
			}
			if (depNodeVtxId4A.equals("")
					|| (!depNodeVtxId4A.equals(depNodeVtxId4B))) {
				// Either they're not really dupes or there's some bad data - so
				// don't pick either one
				return nullVtx;
			}
		}

		if (vtxIdsConn2A.size() == vtxIdsConn2B.size()) {
			// 2 - If they both have edges to all the same vertices, 
			//  then return the one that can be reached uniquely via the 
			//  key if that is the case or
			//  else the one with the lower vertexId
			
			boolean allTheSame = true;
			Iterator<String> iter = vtxIdsConn2A.iterator();
			while (iter.hasNext()) {
				String vtxIdConn2A = iter.next();
				if (!vtxIdsConn2B.contains(vtxIdConn2A)) {
					allTheSame = false;
					break;
				}
			}

			if (allTheSame) {
				// If everything is the same, but one of the two has a good 
				// pointer to it, then save that one.  Otherwise, take the
				// older one.
				if( !onlyNodeThatIndexPointsToVidStr.equals("") ){
					// only one is reachable via the index - choose that one if its aai-uri is also good
					if( onlyNodeThatIndexPointsToVidStr.equals(vidA.toString()) ){
						preferredVtx = vtxA;
					}
					else if( onlyNodeThatIndexPointsToVidStr.equals(vidB.toString()) ){
						preferredVtx = vtxB;
					}
				}
				else if ( checkAaiUriOk(g, vtxA) ) {
					preferredVtx = vtxA;
				} 
				else if ( checkAaiUriOk(g, vtxB) ) {
					preferredVtx = vtxB;
				}
				// else we're picking neither because neither one had a working aai-uri index property
			}
		} else if (vtxIdsConn2A.size() > vtxIdsConn2B.size()) {
			// 3 - VertexA is connected to more things than vtxB.
			// We'll pick VtxA if its edges are a superset of vtxB's edges 
			//   and it doesn't contradict the check for the index/key pointer.
			boolean missingOne = false;
			Iterator<String> iter = vtxIdsConn2B.iterator();
			while (iter.hasNext()) {
				String vtxIdConn2B = iter.next();
				if (!vtxIdsConn2A.contains(vtxIdConn2B)) {
					missingOne = true;
					break;
				}
			}
			if (!missingOne) {
				if( onlyNodeThatIndexPointsToVidStr.equals("") 
						|| onlyNodeThatIndexPointsToVidStr.equals(vidA.toString()) ){
					preferredVtx = vtxA;
				}
			}
		} else if (vtxIdsConn2B.size() > vtxIdsConn2A.size()) {
			// 4 - VertexB is connected to more things than vtxA.
			// We'll pick VtxB if its edges are a superset of vtxA's edges
			//   and it doesn't contradict the check for the index/key pointer.
			boolean missingOne = false;
			Iterator<String> iter = vtxIdsConn2A.iterator();
			while (iter.hasNext()) {
				String vtxIdConn2A = iter.next();
				if (!vtxIdsConn2B.contains(vtxIdConn2A)) {
					missingOne = true;
					break;
				}
			}
			if (!missingOne) {
				if( onlyNodeThatIndexPointsToVidStr.equals("") 
						|| onlyNodeThatIndexPointsToVidStr.equals(vidB.toString()) ){
					preferredVtx = vtxB;
				}
			}
		} else {
			preferredVtx = nullVtx;
		}

		return (preferredVtx);

	} // end of pickOneOfTwoDupes()

	/**
	 * Check and process dupes.
	 *
	 * @param transId the trans id
	 * @param fromAppId the from app id
	 * @param g the g
	 * @param version the version
	 * @param nType the n type
	 * @param passedVertList the passed vert list
	 * @param dupeFixOn the dupe fix on
	 * @param deleteCandidateList the delete candidate list
	 * @param alreadyFoundDupeGroups the already found dupe groups
	 * @return the array list
	 */
	private List<String> checkAndProcessDupes(String transId,
			String fromAppId, Graph g, GraphTraversalSource source, String version, String nType,
			List<Vertex> passedVertList, Boolean dupeFixOn,
			Set<String> deleteCandidateList, 
			ArrayList<String> alreadyFoundDupeGroups, Loader loader ) {
		
		ArrayList<String> returnList = new ArrayList<>();
		ArrayList<Vertex> checkVertList = new ArrayList<>();
		ArrayList<String> alreadyFoundDupeVidArr = new ArrayList<>();
		Boolean noFilterList = true;
		Iterator<String> afItr = alreadyFoundDupeGroups.iterator();
		while (afItr.hasNext()) {
			String dupeGrpStr = afItr.next();
			String[] dupeArr = dupeGrpStr.split("\\|");
			int lastIndex = dupeArr.length - 1;
			for (int i = 0; i < lastIndex; i++) {
				// Note: we don't want the last one...
				String vidString = dupeArr[i];
				alreadyFoundDupeVidArr.add(vidString);
				noFilterList = false;
			}
		}

		// For a given set of Nodes that were found with a set of KEY
		// Parameters, (nodeType + key data) we will
		// see if we find any duplicate nodes that need to be cleaned up. Note -
		// it's legit to have more than one
		// node with the same key data if the nodes depend on a parent for
		// uniqueness -- as long as the two nodes
		// don't hang off the same Parent.
		// If we find duplicates, and we can figure out which of each set of
		// duplicates is the one that we
		// think should be preserved, we will record that. Whether we can tell
		// which one should be
		// preserved or not, we will return info about any sets of duplicates
		// found.
		//
		// Each element in the returned arrayList might look like this:
		// "1234|5678|keepVid=UNDETERMINED" (if there were 2 dupes, and we
		// couldn't figure out which one to keep)
		// or, "100017|200027|30037|keepVid=30037" (if there were 3 dupes and we
		// thought the third one was the one that should survive)

		// Because of the way the calling code loops over stuff, we can get the
		// same data multiple times - so we should
		// not process any vertices that we've already seen.

		try {
			Iterator<Vertex> pItr = passedVertList.iterator();
			while (pItr.hasNext()) {
				Vertex tvx = pItr.next();
				String passedId = tvx.id().toString();
				if (noFilterList || !alreadyFoundDupeVidArr.contains(passedId)) {
					// We haven't seen this one before - so we should check it.
					checkVertList.add(tvx);
				}
			}

			if (checkVertList.size() < 2) {
				// Nothing new to check.
				return returnList;
			}

			if (loader.introspectorFromName(nType).isTopLevel()) {
				// If this was a node that does NOT depend on other nodes for
				// uniqueness, and we
				// found more than one node using its key -- record the found
				// vertices as duplicates.
				String dupesStr = "";
				for (int i = 0; i < checkVertList.size(); i++) {
					dupesStr = dupesStr
							+ ((checkVertList.get(i))).id()
									.toString() + "|";
				}
				if (dupesStr != "") {
					Vertex prefV = getPreferredDupe(transId, fromAppId,
							source, checkVertList, version, loader);
					if (prefV == null) {
						// We could not determine which duplicate to keep
						dupesStr = dupesStr + "KeepVid=UNDETERMINED";
						returnList.add(dupesStr);
					} else {
						dupesStr = dupesStr + "KeepVid=" + prefV.id();
						Boolean didRemove = false;
						if (dupeFixOn) {
							didRemove = deleteNonKeepersIfAppropriate(g,
									dupesStr, prefV.id().toString(),
									deleteCandidateList);
						}
						if (didRemove) {
							dupeGrpsDeleted++;
						} else {
							// keep them on our list
							returnList.add(dupesStr);
						}
					}
				}
			} else {
				// More than one node have the same key fields since they may
				// depend on a parent node for uniqueness. Since we're finding 
				// more than one, we want to check to see if any of the
				// vertices that have this set of keys (and are the same nodeType)
				// are also pointing at the same 'parent' node.
				// Note: for a given set of key data, it is possible that there
				// could be more than one set of duplicates.
				HashMap<String, ArrayList<Vertex>> vertsGroupedByParentHash = groupVertsByDepNodes(
						transId, fromAppId, source, version, nType,
						checkVertList, loader);
				for (Map.Entry<String, ArrayList<Vertex>> entry : vertsGroupedByParentHash
						.entrySet()) {
					ArrayList<Vertex> thisParentsVertList = entry
							.getValue();
					if (thisParentsVertList.size() > 1) {
						// More than one vertex found with the same key info
						// hanging off the same parent/dependent node
						String dupesStr = "";
						for (int i = 0; i < thisParentsVertList.size(); i++) {
							dupesStr = dupesStr
									+ ((thisParentsVertList
											.get(i))).id() + "|";
						}
						if (dupesStr != "") {
							Vertex prefV = getPreferredDupe(transId,
									fromAppId, source, thisParentsVertList,
									version, loader);

							if (prefV == null) {
								// We could not determine which duplicate to
								// keep
								dupesStr = dupesStr + "KeepVid=UNDETERMINED";
								returnList.add(dupesStr);
							} else {
								Boolean didRemove = false;
								dupesStr = dupesStr + "KeepVid="
										+ prefV.id().toString();
								if (dupeFixOn) {
									didRemove = deleteNonKeepersIfAppropriate(
											g, dupesStr, prefV.id()
													.toString(),
											deleteCandidateList );
								}
								if (didRemove) {
									dupeGrpsDeleted++;
								} else {
									// keep them on our list
									returnList.add(dupesStr);
								}
							}
						}
					}
				}
			}
		} catch (Exception e) {
			LOGGER.warn(" >>> Threw an error in checkAndProcessDupes - just absorb this error and move on. ", e);
		}

		return returnList;

	}// End of checkAndProcessDupes()

	/**
	 * Group verts by dep nodes.
	 *
	 * @param transId the trans id
	 * @param fromAppId the from app id
	 * @param g the g
	 * @param version the version
	 * @param nType the n type
	 * @param passedVertList the passed vert list
	 * @return the hash map
	 * @throws AAIException the AAI exception
	 */
	private HashMap<String, ArrayList<Vertex>> groupVertsByDepNodes(
			String transId, String fromAppId, GraphTraversalSource g, String version,
			String nType, ArrayList<Vertex> passedVertList, Loader loader)
			throws AAIException {
		// Given a list of JanusGraph Vertices of one nodeType (see AAI-8956), group 
		// them together by the parent node they depend on.
		// Ie. if given a list of ip address nodes (assumed to all have the
		// same key info) they might sit under several different parent vertices.
		// Under Normal conditions, there would only be one per parent -- but
		// we're trying to find duplicates - so we
		// allow for the case where more than one is under the same parent node.

		HashMap<String, ArrayList<Vertex>> retHash = new HashMap<String, ArrayList<Vertex>>();
		if (loader.introspectorFromName(nType).isTopLevel()) {
			// This method really should not have been called if this is not the
			// kind of node
			// that depends on a parent for uniqueness, so just return the empty
			// hash.
			return retHash;
		}

		// Find out what types of nodes the passed in nodes can depend on
		ArrayList<String> depNodeTypeL = new ArrayList<>();
		Collection<String> depNTColl = loader.introspectorFromName(nType).getDependentOn();
		Iterator<String> ntItr = depNTColl.iterator();
		while (ntItr.hasNext()) {
			depNodeTypeL.add(ntItr.next());
		}
		// For each vertex, we want find its depended-on/parent vertex so we
		// can track what other vertexes that are dependent on that same guy.
		if (passedVertList != null) {
			Iterator<Vertex> iter = passedVertList.iterator();
			while (iter.hasNext()) {
				Vertex thisVert = iter.next();
				Vertex tmpParentVtx = getConnectedParent( g, thisVert );
				if( tmpParentVtx != null ) {
					String parentNt = null;
					Object obj = tmpParentVtx.<Object>property("aai-node-type").orElse(null);
					if (obj != null) {
						parentNt = obj.toString();
					}
					if (depNTColl.contains(parentNt)) {
						// This must be the parent/dependent node
						String parentVid = tmpParentVtx.id().toString();
						if (retHash.containsKey(parentVid)) {
							// add this vert to the list for this parent key
							retHash.get(parentVid).add(thisVert);
						} else {
							// This is the first one we found on this parent
							ArrayList<Vertex> vList = new ArrayList<>();
							vList.add(thisVert);
							retHash.put(parentVid, vList);
						}
					}
				}
			}
		}

		return retHash;

	}// end of groupVertsByDepNodes()

	/**
	 * Delete non keepers if appropriate.
	 *
	 * @param g the g
	 * @param dupeInfoString the dupe info string
	 * @param vidToKeep the vid to keep
	 * @param deleteCandidateList the delete candidate list
	 * @return the boolean
	 */
	private Boolean deleteNonKeepersIfAppropriate(Graph g,
			String dupeInfoString, String vidToKeep,
			Set<String> deleteCandidateList ) {

		Boolean deletedSomething = false;
		// This assumes that the dupeInfoString is in the format of
		// pipe-delimited vid's followed by
		// ie. "3456|9880|keepVid=3456"
		if (deleteCandidateList == null || deleteCandidateList.size() == 0) {
			// No vid's on the candidate list -- so no deleting will happen on
			// this run
			return false;
		}

		String[] dupeArr = dupeInfoString.split("\\|");
		ArrayList<String> idArr = new ArrayList<>();
		int lastIndex = dupeArr.length - 1;
		for (int i = 0; i <= lastIndex; i++) {
			if (i < lastIndex) {
				// This is not the last entry, it is one of the dupes,
				String vidString = dupeArr[i];
				idArr.add(vidString);
			} else {
				// This is the last entry which should tell us if we have a
				// preferred keeper
				String prefString = dupeArr[i];
				if (prefString.equals("KeepVid=UNDETERMINED")) {
					// They sent us a bad string -- nothing should be deleted if
					// no dupe could be tagged as preferred
					return false;
				} else {
					// If we know which to keep, then the prefString should look
					// like, "KeepVid=12345"
					String[] prefArr = prefString.split("=");
					if (prefArr.length != 2 || (!prefArr[0].equals("KeepVid"))) {
						LOGGER.debug("Bad format. Expecting KeepVid=999999");
						return false;
					} else {
						String keepVidStr = prefArr[1];
						if (idArr.contains(keepVidStr)) {
							idArr.remove(keepVidStr);

							// So now, the idArr should just contain the vid's
							// that we want to remove.
							for (int x = 0; x < idArr.size(); x++) {
								boolean okFlag = true;
								String thisVid = idArr.get(x);
								if (deleteCandidateList.contains(thisVid)) {
									// This vid is a valid delete candidate from
									// a prev. run, so we can remove it.
									try {
										long longVertId = Long
												.parseLong(thisVid);
										Vertex vtx = g
												.traversal().V(longVertId).next();
										vtx.remove();

									} catch (Exception e) {
										okFlag = false;
										LOGGER.debug("ERROR trying to delete VID = " + thisVid + " " + LogFormatTools.getStackTop(e));
									}
									if (okFlag) {
										LOGGER.debug(" DELETED VID = " + thisVid);
										deletedSomething = true;
									}
								}
							}
						} else {
							LOGGER.debug("ERROR - Vertex Id to keep not found in list of dupes.  dupeInfoString = ["
									+ dupeInfoString + "]");
							return false;
						}
					}
				}// else we know which one to keep
			}// else last entry
		}// for each vertex in a group

		return deletedSomething;

	}// end of deleteNonKeepersIfAppropriate()

	
	
	/**
	 * makes sure aai-uri exists and can be used to get this node back
	 *
	 * @param transId the trans id
	 * @param fromAppId the from app id
	 * @param graph the graph
	 * @param vtx
	 * @return true if aai-uri is populated and the aai-uri-index points to this vtx
	 * @throws AAIException the AAI exception
	 */
	public Boolean checkAaiUriOk( GraphTraversalSource graph, Vertex origVtx )
			throws AAIException{
		String aaiUriStr = "";
		try { 
			Object ob = origVtx.<Object>property("aai-uri").orElse(null);
			String origVid = origVtx.id().toString();
			LOGGER.debug("DEBUG --- do checkAaiUriOk() for origVid = " + origVid);
			if (ob == null || ob.toString().equals("")) {
				// It is missing its aai-uri
				LOGGER.debug("DEBUG No [aai-uri] property found for vid = [" 
						+ origVid + "] " );
				return false;
			}
			else {
				aaiUriStr = ob.toString();
				Iterator <Vertex> verts = graph.V().has("aai-uri",aaiUriStr);
				int count = 0;
				while( verts.hasNext() ){
					count++;
					Vertex foundV = verts.next();
					String foundVid = foundV.id().toString();
					if( !origVid.equals(foundVid) ){
						LOGGER.debug("DEBUG aai-uri key property ["  
								+ aaiUriStr + "] for vid = [" 
								+ origVid + "] brought back different vertex with vid = [" 
								+ foundVid + "]." );
						return false;
					}
				}
				if( count == 0 ){
					LOGGER.debug("DEBUG aai-uri key property ["  
							+ aaiUriStr + "] for vid = [" 
							+ origVid + "] could not be used to query for that vertex. ");
					return false;	
				}
				else if( count > 1 ){
					LOGGER.debug("DEBUG aai-uri key property ["  
							+ aaiUriStr + "] for vid = [" 
							+ origVid + "] brought back multiple (" 
							+ count + ") vertices instead of just one. ");
					return false;	
				}
			}
		}
		catch( Exception ex ){
			LOGGER.debug(" ERROR trying to get node with aai-uri: [" + aaiUriStr + "]" + LogFormatTools.getStackTop(ex));
		}
		return true;
		
	}// End of checkAaiUriOk() 
	
	/**
	 * Gets the node just using key params.
	 *
	 * @param transId the trans id
	 * @param fromAppId the from app id
	 * @param graph the graph
	 * @param nodeType the node type
	 * @param keyPropsHash the key props hash
	 * @param apiVersion the api version
	 * @return the node just using key params
	 * @throws AAIException the AAI exception
	 */
	public List <Vertex> getNodeJustUsingKeyParams( String transId, String fromAppId, GraphTraversalSource graph, String nodeType,
			HashMap<String,Object> keyPropsHash, String apiVersion ) 	 throws AAIException{
		
		List <Vertex> retVertList = new ArrayList <> ();
		
		// We assume that all NodeTypes have at least one key-property defined.  
		// Note - instead of key-properties (the primary key properties), a user could pass
		//        alternate-key values if they are defined for the nodeType.
		List<String> kName = new ArrayList<>();
		List<Object> kVal = new ArrayList<>();
		if( keyPropsHash == null || keyPropsHash.isEmpty() ) {
			throw new AAIException("AAI_6120", " NO key properties passed for this getNodeJustUsingKeyParams() request.  NodeType = [" + nodeType + "]. "); 
		}
		
		int i = -1;
		for( Map.Entry<String, Object> entry : keyPropsHash.entrySet() ){
			i++;
			kName.add(i, entry.getKey());
			kVal.add(i, entry.getValue());
		}
		int topPropIndex = i;
		Vertex tiV = null;
		String propsAndValuesForMsg = "";
		Iterator <Vertex> verts = null;

		try { 
			if( topPropIndex == 0 ){
				propsAndValuesForMsg = " (" + kName.get(0) + " = " + kVal.get(0) + ") ";
				verts= graph.V().has(kName.get(0),kVal.get(0)).has("aai-node-type",nodeType);	
			}	
			else if( topPropIndex == 1 ){
				propsAndValuesForMsg = " (" + kName.get(0) + " = " + kVal.get(0) + ", " 
						+ kName.get(1) + " = " + kVal.get(1) + ") ";
				verts =  graph.V().has(kName.get(0),kVal.get(0)).has(kName.get(1),kVal.get(1)).has("aai-node-type",nodeType);	
			}	 		
			else if( topPropIndex == 2 ){
				propsAndValuesForMsg = " (" + kName.get(0) + " = " + kVal.get(0) + ", " 
						+ kName.get(1) + " = " + kVal.get(1) + ", " 
						+ kName.get(2) + " = " + kVal.get(2) +  ") ";
				verts= graph.V().has(kName.get(0),kVal.get(0)).has(kName.get(1),kVal.get(1)).has(kName.get(2),kVal.get(2)).has("aai-node-type",nodeType);
			}	
			else if( topPropIndex == 3 ){
				propsAndValuesForMsg = " (" + kName.get(0) + " = " + kVal.get(0) + ", " 
						+ kName.get(1) + " = " + kVal.get(1) + ", " 
						+ kName.get(2) + " = " + kVal.get(2) + ", " 
						+ kName.get(3) + " = " + kVal.get(3) +  ") ";
				verts= graph.V().has(kName.get(0),kVal.get(0)).has(kName.get(1),kVal.get(1)).has(kName.get(2),kVal.get(2)).has(kName.get(3),kVal.get(3)).has("aai-node-type",nodeType);
			}	 		
			else {
				throw new AAIException("AAI_6114", " We only support 4 keys per nodeType for now \n"); 
			}
		}
		catch( Exception ex ){
			LOGGER.debug( " ERROR trying to get node for: [" + propsAndValuesForMsg + "]" + LogFormatTools.getStackTop(ex));
		}

		if( verts != null ){
			while( verts.hasNext() ){
				tiV = verts.next();
				retVertList.add(tiV);
			}
		}
		
		if( retVertList.size() == 0 ){
			LOGGER.debug("DEBUG No node found for nodeType = [" + nodeType +
					"], propsAndVal = " + propsAndValuesForMsg );
		}
		
		return retVertList;
		
	}// End of getNodeJustUsingKeyParams() 
	
	/**
	 * Show all edges for node.
	 *
	 * @param transId the trans id
	 * @param fromAppId the from app id
	 * @param tVert the t vert
	 * @return the array list
	 */
	private ArrayList <String> showAllEdgesForNode( String transId, String fromAppId, Vertex tVert ){

		ArrayList <String> retArr = new ArrayList <> ();
		Iterator <Edge> eI = tVert.edges(Direction.IN);
		if( ! eI.hasNext() ){
			retArr.add("No IN edges were found for this vertex. ");
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
				retArr.add(" >>> COULD NOT FIND VERTEX on the other side of this edge edgeId = " + ed.id() + " <<< ");
			}
			else {
				String nType = vtx.<String>property("aai-node-type").orElse(null);
				String vid = vtx.id().toString();
				retArr.add("Found an IN edge (" + lab + ") to this vertex from a [" + nType + "] node with VtxId = " + vid );
				
			}
		}
		
		eI = tVert.edges(Direction.OUT);
		if( ! eI.hasNext() ){
			retArr.add("No OUT edges were found for this vertex. ");
		}
		while( eI.hasNext() ){
			Edge ed =  eI.next();
			String lab = ed.label();
			Vertex vtx;
			if (tVert.equals(ed.inVertex())) {
				vtx = ed.outVertex();
			} else {
				vtx = ed.inVertex();
			}
			if( vtx == null ){
				retArr.add(" >>> COULD NOT FIND VERTEX on the other side of this edge edgeId = " + ed.id() + " <<< ");
			}
			else {
				String nType = vtx.<String>property("aai-node-type").orElse(null);
				String vid = vtx.id().toString();
				retArr.add("Found an OUT edge (" + lab + ") from this vertex to a [" + nType + "] node with VtxId = " + vid );
			}
		}
		return retArr;
	}

	
	/**
	 * Show properties for node.
	 *
	 * @param transId the trans id
	 * @param fromAppId the from app id
	 * @param tVert the t vert
	 * @return the array list
	 */
	private ArrayList <String> showPropertiesForNode( String transId, String fromAppId, Vertex tVert ){

		ArrayList <String> retArr = new ArrayList <> ();
		if( tVert == null ){
			retArr.add("null Node object passed to showPropertiesForNode()\n");
		}
		else {
			String nodeType = "";
			Object ob = tVert.<Object>property("aai-node-type").orElse(null);
			if( ob == null ){
				nodeType = "null";
			}
			else{
				nodeType = ob.toString();
			}
			
			retArr.add(" AAINodeType/VtxID for this Node = [" + nodeType + "/" + tVert.id() + "]");
			retArr.add(" Property Detail: ");
			Iterator<VertexProperty<Object>> pI = tVert.properties();
			while( pI.hasNext() ){
				VertexProperty<Object> tp = pI.next();
				Object val = tp.value();
				retArr.add("Prop: [" + tp.key() + "], val = [" + val + "] ");
			}
		}
		return retArr;
	}

	
	private ArrayList <Vertex> getConnectedNodes(GraphTraversalSource g, Vertex startVtx )
			throws AAIException {
	
		ArrayList <Vertex> retArr = new ArrayList <> ();
		if( startVtx == null ){
			return retArr;
		}
		else {
			 GraphTraversal<Vertex, Vertex> modPipe = null;
			 modPipe = g.V(startVtx).both();
			 if( modPipe != null && modPipe.hasNext() ){
				while( modPipe.hasNext() ){
					Vertex conVert = modPipe.next();
					retArr.add(conVert);
				}
			}
		}
		return retArr;
		
	}// End of getConnectedNodes()
	

	private ArrayList <Vertex> getConnectedChildrenOfOneType( GraphTraversalSource g,
			Vertex startVtx, String childNType ) throws AAIException{
		
		ArrayList <Vertex> childList = new ArrayList <> ();
		Iterator <Vertex> vertI =  g.V(startVtx).union(__.outE().has(EdgeProperty.CONTAINS.toString(), AAIDirection.OUT.toString()).inV(), __.inE().has(EdgeProperty.CONTAINS.toString(), AAIDirection.IN.toString()).outV());
		
		Vertex tmpVtx = null;
		while( vertI != null && vertI.hasNext() ){
			tmpVtx = vertI.next();
			Object ob = tmpVtx.<Object>property("aai-node-type").orElse(null);
			if (ob != null) {
				String tmpNt = ob.toString();
				if( tmpNt.equals(childNType)){
					childList.add(tmpVtx);
				}
			}
		}
		
		return childList;		

	}// End of getConnectedChildrenOfOneType()


	private Vertex getConnectedParent( GraphTraversalSource g,
			Vertex startVtx ) throws AAIException{
		
		Vertex parentVtx = null;
		Iterator <Vertex> vertI = g.V(startVtx).union(__.inE().has(EdgeProperty.CONTAINS.toString(), AAIDirection.OUT.toString()).outV(), __.outE().has(EdgeProperty.CONTAINS.toString(), AAIDirection.IN.toString()).inV());

		while( vertI != null && vertI.hasNext() ){
			// Note - there better only be one!
			parentVtx = vertI.next();
		}
		
		return parentVtx;		

	}// End of getConnectedParent()
	
	
	private long figureWindowStartTime( int timeWindowMinutes ){
		// Given a window size, calculate what the start-timestamp would be.
		
		if( timeWindowMinutes <= 0 ){
			// This just means that there is no window...
			return 0;
		}
		long unixTimeNow = System.currentTimeMillis();
		long windowInMillis = timeWindowMinutes * 60L * 1000;
		
		long startTimeStamp = unixTimeNow - windowInMillis;
		
		return startTimeStamp;
	} // End of figureWindowStartTime()
	
	
	/**
	 * Collect Duplicate Sets for nodes that are NOT dependent on parent nodes.
	 *
	 * @param transId the trans id
	 * @param fromAppId the from app id
	 * @param g the g
	 * @param version the version
	 * @param nType the n type
	 * @param passedVertList the passed vert list
	 * @return the array list
	 */
	private ArrayList<ArrayList<Vertex>> getDupeSets4NonDepNodes( String transId,
			String fromAppId, Graph g, String version, String nType,
			ArrayList<Vertex> passedVertList,
			ArrayList <String> keyPropNamesArr, 
			 Loader loader ) {
		
		ArrayList<ArrayList<Vertex>> returnList = new ArrayList<ArrayList<Vertex>>();
		
		// We've been passed a set of nodes that we want to check. 
		// They are all NON-DEPENDENT nodes of the same nodeType meaning that they should be 
		// unique in the DB based on their KEY DATA alone.  So, if
		// we group them by their key data - if any key has more than one
		// vertex mapped to it, those vertices are dupes.
		//
		// When we find duplicates, we group them in an ArrayList (there can be
		//     more than one duplicate for one set of key data)
		// Then these dupeSets are grouped up and returned.
		// 
		
		HashMap <String, ArrayList<String>> keyVals2VidHash = new HashMap <String, ArrayList<String>>();
		HashMap <String,Vertex> vtxHash = new HashMap <String,Vertex>();
		Iterator<Vertex> pItr = passedVertList.iterator();
		while (pItr.hasNext()) {
			try {
				Vertex tvx =  pItr.next();
				String thisVid = tvx.id().toString();
				vtxHash.put(thisVid, tvx);
				
				// if there are more than one vertexId mapping to the same keyProps -- they are dupes
				// we dont check till later since a set can contain more than 2.
				String hKey = getNodeKeyValString( tvx, keyPropNamesArr );
				if( hKey.equals("") ){
					// When we have corrupted data, hKey comes back as an empty string
					// We will just skip this entry since it is not a Dupe - it is
					// corrupted data which should be picked up in other checks.
					continue;
				}
				if( keyVals2VidHash.containsKey(hKey) ){
					// We've already seen this key 
					ArrayList <String> tmpVL = (ArrayList <String>)keyVals2VidHash.get(hKey);
					tmpVL.add(thisVid);
					keyVals2VidHash.put(hKey, tmpVL);
				}
				else {
					// First time for this key
					ArrayList <String> tmpVL = new ArrayList <String>();
					tmpVL.add(thisVid);
					keyVals2VidHash.put(hKey, tmpVL);
				}
			}
			catch (Exception e) {
				LOGGER.warn(" >>> Threw an error in getDupeSets4NonDepNodes - just absorb this error and move on. ", e);
			}
		}
					
		for( Map.Entry<String, ArrayList<String>> entry : keyVals2VidHash.entrySet() ){
			ArrayList <String> vidList = entry.getValue();
			try {
				if( !vidList.isEmpty() && vidList.size() > 1 ){
					// There are more than one vertex id's using the same key info
					ArrayList <Vertex> vertList = new ArrayList <Vertex> ();
					for (int i = 0; i < vidList.size(); i++) {
						String tmpVid = vidList.get(i);
						vertList.add(vtxHash.get(tmpVid));
					}
					returnList.add(vertList);
				}
			} 
			catch (Exception e) {
				LOGGER.warn(" >>> Threw an error in getDupeSets4NonDepNodes - just absorb this error and move on. ", e);
			}
			
		}
		return returnList;

	}// End of getDupeSets4NonDepNodes()
	
	
	/**
	 * Get values of the key properties for a node as a single string
	 *
	 * @param tvx the vertex to pull the properties from
	 * @param keyPropNamesArr collection of key prop names
	 * @return a String of concatenated values
	 */
	private String getNodeKeyValString( Vertex tvx,
			ArrayList <String> keyPropNamesArr ) {
		
		String retString = "";
		Iterator <String> propItr = keyPropNamesArr.iterator();
		while( propItr.hasNext() ){
			String propName = propItr.next();
			if( tvx != null ){
				Object propValObj = tvx.property(propName).orElse(null);
				if( propValObj == null ){
					LOGGER.warn(" >>> WARNING >>> could not find this key-property for this vertex.  propName = ["
							+ propName + "], VID = " + tvx.id().toString() );
				}
				else {
					retString = " " + retString + propValObj.toString();
				}
			} 
		}
		return retString;
	
	}// End of getNodeKeyValString()	
	
	
	private String findJustOneUsingIndex( String transId, String fromAppId,
			GraphTraversalSource gts, HashMap <String,Object> keyPropValsHash, 
			String nType, Long vidAL, Long vidBL, String apiVer){
		
		// See if querying by JUST the key params (which should be indexed) brings back
		// ONLY one of the two vertices. Ie. the db still has a pointer to one of them
		// and the other one is sort of stranded.
		String returnVid = "";
		
		try {
			List <Vertex> tmpVertList = getNodeJustUsingKeyParams( transId, fromAppId, gts,
					nType, keyPropValsHash, apiVer );
			if( tmpVertList != null && tmpVertList.size() == 1 ){
				// We got just one - if it matches one of the ones we're looking
				// for, then return that VID
				Vertex tmpV = tmpVertList.get(0);
				String thisVid = tmpV.id().toString();
				if( thisVid.equals(vidAL.toString()) || thisVid.equals(vidBL.toString()) ){
					String msg = " vid = " + thisVid + " is one of two that the DB can retrieve directly ------";
					//System.out.println(msg);
					LOGGER.info(msg);
					returnVid = thisVid;
				}
			}
		}
		catch ( AAIException ae ){
			String emsg = "Error trying to get node just by key " + ae.getMessage();
			//System.out.println(emsg);
			LOGGER.debug(emsg);
	  	}
		
		return returnVid;
		
	}// End of findJustOneUsingIndex()
	
class CommandLineArgs {

		
		@Parameter(names = "--help", help = true)
		public boolean help;

		@Parameter(names = "-edgesOnly", description = "Check grooming on edges only")
		public Boolean edgesOnlyFlag = false;

		@Parameter(names = "-autoFix", description = "doautofix")
		public Boolean doAutoFix = false;

		@Parameter(names = "-skipHostCheck", description = "skipHostCheck")
		public Boolean skipHostCheck = false;

		@Parameter(names = "-dontFixOrphans", description = "dontFixOrphans")
		public Boolean dontFixOrphansFlag = false;

		@Parameter(names = "-dupeCheckOff", description = "dupeCheckOff")
		public Boolean dupeCheckOff = false;

		@Parameter(names = "-dupeFixOn", description = "dupeFixOn")
		public Boolean dupeFixOn = false;

		@Parameter(names = "-ghost2CheckOff", description = "ghost2CheckOff")
		public Boolean ghost2CheckOff = false;

		@Parameter(names = "-ghost2FixOn", description = "ghost2FixOn")
		public Boolean ghost2FixOn = false;
		
		@Parameter(names = "-neverUseCache", description = "neverUseCache")
		public Boolean neverUseCache = false;
		
		@Parameter(names = "-skipEdgeChecks", description = "skipEdgeChecks")
		public Boolean skipEdgeCheckFlag = false;
		
		@Parameter(names = "-skipIndexUpdateFix", description = "skipIndexUpdateFix")
		public Boolean skipIndexUpdateFix = false;
		
		@Parameter(names = "-maxFix", description = "maxFix")
		public int maxRecordsToFix = GraphAdminConstants.AAI_GROOMING_DEFAULT_MAX_FIX;
		
		@Parameter(names = "-sleepMinutes", description = "sleepMinutes")
		public int sleepMinutes = GraphAdminConstants.AAI_GROOMING_DEFAULT_SLEEP_MINUTES;
		
		// A value of 0 means that we will not have a time-window -- we will look
				// at all nodes of the passed-in nodeType.
		@Parameter(names = "-timeWindowMinutes", description = "timeWindowMinutes")
		public int timeWindowMinutes = 0;
		
		@Parameter(names = "-f", description = "file")
		public String prevFileName = "";
		
		@Parameter(names = "-singleNodeType", description = "singleNodeType")
		public String singleNodeType = "";
		
	}

	public HashMap<String, Vertex> getGhostNodeHash() {
		return ghostNodeHash;
	}

	public void setGhostNodeHash(HashMap<String, Vertex> ghostNodeHash) {
		this.ghostNodeHash = ghostNodeHash;
	}
	
	public int getGhostNodeCount(){
		return getGhostNodeHash().size();
	}
	
	public HashMap<String, Vertex> getOrphanNodeHash() {
		return orphanNodeHash;
	}

	public void setOrphanNodeHash(HashMap<String, Vertex> orphanNodeHash) {
		this.orphanNodeHash = orphanNodeHash;
	}
	
	public int getOrphanNodeCount(){
		return getOrphanNodeHash().size();
	}
	  
	public HashMap<String, Vertex> getMissingAaiNtNodeHash() {
		return missingAaiNtNodeHash;
	}

	public void setMissingAaiNtNodeHash(HashMap<String, Vertex> missingAaiNtNodeHash) {
		this.missingAaiNtNodeHash = missingAaiNtNodeHash;
	}
	
	public int getMissingAaiNtNodeCount(){
		return getMissingAaiNtNodeHash().size();
	}
		
	public HashMap<String, Vertex> getBadUriNodeHash() {
		return badUriNodeHash;
	}

	public void setBadUriNodeHash(HashMap<String, Vertex> badUriNodeHash) {
		this.badUriNodeHash = badUriNodeHash;
	}
	
	public int getBadUriNodeCount(){
		return getBadUriNodeHash().size();
	}

	public HashMap<String, Vertex> getBadIndexNodeHash() {
		return badIndexNodeHash;
	}

	public void setBadIndexNodeHash(HashMap<String, Vertex> badIndexNodeHash) {
		this.badIndexNodeHash = badIndexNodeHash;
	}
	
	public int getBadIndexNodeCount(){
		return getBadIndexNodeHash().size();
	}
	
	public HashMap<String, Edge> getOneArmedEdgeHash() {
		return oneArmedEdgeHash;
	}

	public void setOneArmedEdgeHash(HashMap<String, Edge> oneArmedEdgeHash) {
		this.oneArmedEdgeHash = oneArmedEdgeHash;
	}
	
	public int getOneArmedEdgeHashCount(){
		return getOneArmedEdgeHash().size();
	}
	
	public Set<String> getDeleteCandidateList() {
		return deleteCandidateList;
	}

	public void setDeleteCandidateList(Set<String> deleteCandidateList) {
		this.deleteCandidateList = deleteCandidateList;
	}

	public int getDeleteCount() {
		return deleteCount;
	}

	public void setDeleteCount(int deleteCount) {
		this.deleteCount = deleteCount;
	}
	
	public ArrayList<String> getDupeGroups() {
		return dupeGroups;
	}

	public void setDupeGroups(ArrayList<String> dupeGroups) {
		this.dupeGroups = dupeGroups;
	}

	

}