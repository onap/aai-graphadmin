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

package org.onap.aai.audit;

import com.google.gson.JsonObject;
import com.google.gson.JsonParser;
import java.io.BufferedReader;
import java.io.BufferedWriter;
import java.io.File;
import java.io.FileReader;
import java.io.FileWriter;
import java.io.IOException;
import java.nio.file.Files;
import java.nio.file.Path;
import java.text.ParseException;
import java.text.SimpleDateFormat;
import java.util.Collection;
import java.util.Date;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.stream.Collectors;
import org.javatuples.Triplet;
import org.onap.aai.edges.EdgeIngestor;
import org.onap.aai.edges.EdgeRule;
import org.onap.aai.edges.exceptions.EdgeRuleNotFoundException;
import org.onap.aai.introspection.Loader;
import org.onap.aai.introspection.LoaderFactory;
import org.onap.aai.introspection.ModelType;
import org.onap.aai.rest.client.ApertureService;
import org.onap.aai.setup.SchemaVersions;
import org.onap.aai.util.FormatDate;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.stereotype.Component;

@Component
public class AuditGraphson2Sql {
	
	private EdgeIngestor ei;
	private Loader loader;
	private static final Logger LOGGER = LoggerFactory.getLogger(AuditGraphson2Sql.class.getSimpleName());
	private SchemaVersions schemaVersions;
	private ApertureService apertureService;

    public static final String DEFAULT_SRC_DIR = "logs/data/dataSnapshots/"; 
    public static final int DEFAULT_THRESHOLD_PERCENT = 10;
    public static final String DEFAULT_OUTPUT_DIR = "logs/data/audit";

    //DEBUG -- should be getting default-src-dir, default-output-dir and rdbms-db-name from param file
	public AuditGraphson2Sql( EdgeIngestor ei, SchemaVersions schemaVersions, LoaderFactory loaderFactory, ApertureService apertureService) {
		this.schemaVersions = schemaVersions;
		this.loader = loaderFactory.createLoaderForVersion(ModelType.MOXY, schemaVersions.getDefaultVersion());
		this.ei = ei;
	    this.apertureService = apertureService;
	}

	public String runAudit(String dbName, String sourceFName, String sourceDir )
				throws Exception {

		if( sourceDir == null || sourceDir.isEmpty() ){
			sourceDir = DEFAULT_SRC_DIR;
		}

        HashMap <String,Integer> gsonTableCounts = new HashMap <> ();
        try {
        	gsonTableCounts = getCountsFromGraphsonSnapshot(dbName, sourceFName, sourceDir);
        } catch( Exception e ) {
        	LOGGER.error(" ERROR when calling getCountsFromGraphsonSnapshot(" 
        			+ dbName + "," 
        			+ sourceFName  + ").  Exception = " 
        			+ e.getMessage() );
        	throw e;
        }
        
        long timestamp = 0L;
        try {
        	timestamp = getTimestampFromFileName(sourceFName);
        } catch( Exception e ) {
        	LOGGER.error(" ERROR getting timestamp from filename: " 
        			+ e.getMessage() );
			throw e;
        }

        JsonObject rdbmsJsonResults = new JsonObject ();
		try {
			rdbmsJsonResults = getRdbmsTableCounts(timestamp, dbName);
        } catch( Exception e ) {
        	LOGGER.error(" ERROR getting table count info from mySql. timestamp = ["
        			+ timestamp + "], dbName = [" + dbName +
					"].  Exception = " + e.getMessage() );
			throw e;
        }
		HashMap <String,Integer> sqlNodeCounts = getNodeCountsFromSQLRes(rdbmsJsonResults);
        HashMap <String,Integer> sqlEdgeCounts = getEdgeCountsFromSQLRes(rdbmsJsonResults);

        String resultString = "Audit ran and logged to file: ";
        try {
        	String titleInfo = "Comparing data from GraphSon file: " +
        		sourceFName + ", and Aperture data using timeStamp = [" + 
        		timestamp + "] ";
        	String outFileName = compareResults(gsonTableCounts, sqlNodeCounts,
					sqlEdgeCounts, DEFAULT_OUTPUT_DIR, titleInfo);
        	resultString = resultString + outFileName;
        }
        catch( IOException ie) {
        	LOGGER.error(" ERROR writing to output file. [" + ie.getMessage() + "]" );
			throw new Exception( ie.getMessage() );
        }
        
        return resultString;
        
	}
	
	
    public String compareResults(HashMap <String,Integer> gsonTableCounts,
    		HashMap <String,Integer> sqlNodeCounts, 
    		HashMap <String,Integer> sqlEdgeCounts,
    		String outputDir, String titleInfo) 
    	throws IOException {
    	
    	FormatDate fd = new FormatDate("yyyyMMddHHmm", "GMT");
		String dteStr = fd.getDateTime();
		String fName = outputDir + "/"  + "gson-sql-audit" + dteStr;
    	File auditOutFile = new File(fName);
        auditOutFile.getParentFile().mkdirs();
        BufferedWriter outWriter = new BufferedWriter(new FileWriter(auditOutFile));
        outWriter.write(titleInfo);
        outWriter.newLine();
    	outWriter.write("Totals from Graphson: " + getGsonTotals(gsonTableCounts));
    	outWriter.newLine();
    	outWriter.write("Totals from mySql: " + getMSqlTotals(sqlNodeCounts,sqlEdgeCounts));
    	outWriter.newLine();
    	outWriter.newLine();
    	
    	for (Map.Entry<String, Integer> entry : gsonTableCounts.entrySet()) {
    	    String tblKey = entry.getKey();
    	    int gVal = entry.getValue();
    	    if(!sqlNodeCounts.containsKey(tblKey) && !sqlEdgeCounts.containsKey(tblKey)) {
    	    	String msg = "> Entry for Table: [" + tblKey 
    	    			+ "] not found in the SQL db but had "
    	    			+ gVal + " entries in the GraphSon db";
    	    	LOGGER.error(msg);
    	    	System.out.println(msg);
    	    	outWriter.write(msg);
    	    	outWriter.newLine();
    	    	continue;
    	    }
    	    int sVal = 0;
    	    if( sqlNodeCounts.containsKey(tblKey) ) {
    	    	sVal = sqlNodeCounts.get(tblKey);
    	    }else {
    	    	sVal = sqlEdgeCounts.get(tblKey);
    	    }
    	    if ((gVal > 0) && (sVal == 0)){
    	    	String msg = "For Table [" + tblKey + "], GSon count = " 
    	    			+ gVal + ", zero found in SQL db.";
    	    	LOGGER.error(msg);
    	    	System.out.println(msg);
    	    	outWriter.write(msg);
    	    	outWriter.newLine();
    	    }
    	    String msg = tblKey + ": gson/sql = " + gVal + "/" + sVal;
    	    LOGGER.debug(msg);
	    	System.out.println(msg); 
	    	outWriter.write(msg);
	    	outWriter.newLine();
    	}
    	for (Map.Entry<String, Integer> entry : sqlNodeCounts.entrySet()) {
    		// check for Node types that are no longer on the Graphson side, but are 
    		// still in the SQL DB
    	    String tblKey = entry.getKey();
    	    int sVal = entry.getValue();
    	    if(!gsonTableCounts.containsKey(tblKey)) {
    	    	String msg = " Entry for Table [" + tblKey 
    	    			+ "] not found in the Graphson db, but had " 
    	    			+ sVal + " entries in the SQL db.";
    	    	LOGGER.error(msg);
    	    	System.out.println(msg);
    	    	outWriter.write(msg);
    	    	outWriter.newLine();
    	    	continue;
    	    }
    	}
    	for (Map.Entry<String, Integer> entry : sqlEdgeCounts.entrySet()) {
    		// check for Edge+Label combos that are no longer on the Graphson side, but are 
    		// still in the SQL DB
    	    String tblKey = entry.getKey();
    	    int sVal = entry.getValue();
    	    if(!gsonTableCounts.containsKey(tblKey)) {
    	    	String msg = " Entry for edge+Label combo [" + tblKey 
    	    			+ "] not found in the Graphson db, but had " 
    	    			+ sVal + " entries in the SQL db.";
    	    	LOGGER.error(msg);
    	    	System.out.println(msg);
    	    	outWriter.write(msg);
    	    	outWriter.newLine();
    	    	continue;
    	    }
    	}
    	outWriter.close();
    	String msg = "Audit Results written to: " + fName;
    	LOGGER.debug(msg);
    	System.out.println(msg);
    	return fName;

    }

	
    public HashMap <String,Integer> getNodeCountsFromSQLRes(JsonObject sqlJsonData) {
    	
    	HashMap<String,Integer> nodeCtHash = new HashMap<>();
    	if( sqlJsonData != null ) {
			for (Object e : sqlJsonData.entrySet()) {
				Map.Entry entry = (Map.Entry) e;
				String tableName = String.valueOf(entry.getKey());
				if (!tableName.startsWith("edge__")) {
					nodeCtHash.put(String.valueOf(entry.getKey()), Integer.parseInt(entry.getValue().toString()));
				}
			}
		}
    	return nodeCtHash;
    }


	public HashMap <String,Integer> getEdgeCountsFromSQLRes(JsonObject sqlJsonData){

		HashMap<String,Integer> edgeCtHash = new HashMap<>();
		if( sqlJsonData != null ) {
			for (Object e : sqlJsonData.entrySet()) {
				Map.Entry entry = (Map.Entry) e;
				String tableName = String.valueOf(entry.getKey());
				if (tableName.startsWith("edge__")) {
					edgeCtHash.put(String.valueOf(entry.getKey()), Integer.parseInt(entry.getValue().toString()));

				}
			}
		}
		return edgeCtHash;
	}


	public JsonObject getRdbmsTableCounts(long timestamp, String dbName)
			throws Exception {

		return apertureService.runAudit(timestamp, dbName);
	}
  
    
    public String getGsonTotals(HashMap <String,Integer> tableCounts){
    	int nodeCount = 0;
    	int edgeCount = 0;
	    for (Map.Entry<String, Integer> entry : tableCounts.entrySet()) {
	        String tblKey = entry.getKey();
	        if( tblKey != null && tblKey.startsWith("edge__")){
	        	edgeCount = edgeCount + entry.getValue();
	        } else {
	        	nodeCount = nodeCount + entry.getValue();
	        }
	    }
	    String countStr = " nodes = " + nodeCount + ", edges = " + edgeCount;
	    return countStr;
	}
        
    public String getMSqlTotals(HashMap <String,Integer> nodeCounts,
    		HashMap <String,Integer> edgeCounts){   	
    	int nodeCount = 0;
    	int edgeCount = 0;
	    for (Map.Entry<String, Integer> entry : nodeCounts.entrySet()) {
	        nodeCount = nodeCount + entry.getValue();
	    }
	    for (Map.Entry<String, Integer> entry : edgeCounts.entrySet()) {
	        edgeCount = edgeCount + entry.getValue();
	    }
	    String countStr = " nodes = " + nodeCount + ", edges = " + edgeCount;
	    return countStr;
	}
     
    public Long getTimestampFromFileName(String fName) throws Exception {   	
    	// Note -- we are expecting filenames that look like our 
    	//  current snapshot filenames (without the ".PXX" suffix)
    	//  Ie. dataSnapshot.graphSON.201908151845
    	
    	String datePiece = getDateTimePieceOfFileName(fName);
    	final SimpleDateFormat dateFormat = new SimpleDateFormat("YYYYMMddHHmm");
    	Date date = null;
    	try {
    		date = dateFormat.parse(datePiece);
    	}
    	catch (ParseException pe) {
    		throw new Exception ("Error trying to parse this to a Date-Timestamp ["
    				+ datePiece + "]: " + pe.getMessage() );
    	}
    	final long timestamp = date.getTime();
    	return timestamp;
     	
    }

       
    public String getDateTimePieceOfFileName(String fName) throws Exception {   	
    	// Note -- we are expecting filenames that look like our 
    	//  current snapshot filenames (without the ".PXX" suffix)
    	//  Ie. dataSnapshot.graphSON.201908151845
    	
    	if( fName == null || fName.isEmpty() || fName.length() < 12) {
    		throw new Exception ("File name must end with .yyyymmddhhmm ");
    	}
    	int index = fName.lastIndexOf('.');
    	
    	if ( index == -1 ) {
    		throw new Exception ("File name must end with .yyyymmddhhmm ");
    	}
    	index++;
    	if(fName.length() <= index) {
    		throw new Exception ("File name must end with .yyyymmddhhmm ");
    	}
    	String datePiece = fName.substring(index);
    	if( datePiece.length() != 12 ) {
    		throw new Exception ("File name must end date in the format .yyyymmddhhmm ");
    	}
    	
    	return datePiece;
    }
    
    
    public Map<String, String> getEdgeMapKeys() throws EdgeRuleNotFoundException {
    	
    	int edKeyCount = 0;
        Map<String, String> edgeKeys = new HashMap<>();
        // EdgeKey will look like:  nodeTypeA__nodeTypeB
        // The value will be the key with "edge__" pre-pended
        
        for (Map.Entry<String, Collection<EdgeRule>> rulePairings : ei.getAllCurrentRules().asMap().entrySet()) {
            for (EdgeRule er : rulePairings.getValue()) {
            	String a = er.getTo();
            	String b = er.getFrom();
            	if (a.compareTo(b) >= 0) {
            		er.flipDirection();
                }
                if (!loader.getAllObjects().containsKey(er.getTo()) || !loader.getAllObjects().containsKey(er.getFrom())) {
                	// we don't have one of these nodeTypes, so skip this entry
                	continue;
                }
                
                final String to = er.getTo().replace("-", "_");
                final String from = er.getFrom().replace("-", "_");
                final String edKey = from + "__" + to;
                
                if (edgeKeys.containsKey(edKey)) {
                	continue;
                }
                edKeyCount++;
                edgeKeys.put(edKey, "edge__"+ edKey);
               
            }
        }
        System.out.println("DEBUG --> There are " + edKeyCount + " edge table keys defined. " );
        LOGGER.debug(" -- There are " + edKeyCount + " edge table keys defined. " );
        return edgeKeys;
    }
    
	
    public HashMap <String,Integer> getCountsFromGraphsonSnapshot(String databaseName, 
    		String fileNamePart, String srcDir) throws Exception {
        
        final JsonParser jsonParser = new JsonParser();
        final Set<String> uris = new HashSet<>();
        final Set<String> uuids = new HashSet<>();
        final Map<Long, String> idToUri = new HashMap<>();
        final Map<Long, String> idToType = new HashMap<>();
        final Map<Long, String> idToUuid = new HashMap<>();
        final Set<Triplet<Long, Long, String>> idIdLabelOfEdges = new HashSet<>();
        
        final HashMap<String,Integer> tableCountHash = new HashMap<>();
      
              
        String sourceDir = DEFAULT_SRC_DIR;  // Default for now
        if( srcDir != null && srcDir.length() > 1 ) {
        	// If they passed one in, then we'll use it.
        	sourceDir = srcDir;
        }
        String graphsonDir = sourceDir + "/";
     
        if( fileNamePart == null || fileNamePart.trim().isEmpty() ){
        	String msg = "ERROR -- fileName is required to be passed in. ";
		    LOGGER.error(msg);
        	System.out.println(msg);
        	throw new Exception(msg);
        }
        
        final List<File> graphsons = Files.walk(Path.of(graphsonDir))
                .filter(Files::isRegularFile)
                .map(Path::toFile)
                .sorted()
                .collect(Collectors.toList());

        int skippedNodeCount = 0;  
        int nodeCounter = 0;
        int edgeCounter = 0;
        for (File graphson : graphsons) {
            if( !graphson.getName().contains(fileNamePart) ) {
            	continue;
            }
            
            try (BufferedReader reader = new BufferedReader(new FileReader(graphson))) {
                String msg = "Processing snapshot file " + graphson.getName();
                LOGGER.debug(msg);
                System.out.println(msg);
                String line;
                    
                while ((line = reader.readLine()) != null) {
                    JsonObject vertex = jsonParser.parse(line).getAsJsonObject();
                    long id = vertex.get("id").getAsLong();

                    if ((vertex.get("properties") == null) ||
                    		!vertex.get("properties").getAsJsonObject().has("aai-uri") ||
                            !vertex.get("properties").getAsJsonObject().has("aai-node-type") ||
                            !vertex.get("properties").getAsJsonObject().has("aai-uuid")) {
                        	
                        msg = "DEBUG  --  Could not find keys for this line: [" +
                        			line + "] ------";
                        LOGGER.debug(msg);
                        System.out.println(msg);
                        skippedNodeCount++;
                        continue;
                    }

                    String uri = vertex.get("properties").getAsJsonObject().get("aai-uri").getAsJsonArray().get(0).getAsJsonObject().get("value").getAsString();
                    String nodeType = vertex.get("properties").getAsJsonObject().get("aai-node-type").getAsJsonArray().get(0).getAsJsonObject().get("value").getAsString();
                    String nodeTypeKey = nodeType.replaceAll("-", "_");
                    String uuid = vertex.get("properties").getAsJsonObject().get("aai-uuid").getAsJsonArray().get(0).getAsJsonObject().get("value").getAsString();

                    try {
                        loader.introspectorFromName(nodeType);
                    } catch (Exception e) {
                        msg = "DEBUG -- loader introspector for nodeType error: [" +
                        		e.getMessage() + "], [" + e.getLocalizedMessage() + "]------";
                        LOGGER.debug(msg);
                        System.out.println(msg);
                        skippedNodeCount++;
                        continue;
                    }
                                               
                    if (uris.contains(uri)) {
                        msg = "DEBUG -- SKIP Uri because it has been seen before: [" +
                        		uri + "] ------";
                        LOGGER.debug(msg);
                    	System.out.println(msg);
                        skippedNodeCount++;
                        continue;
                    }
                    else if (uuids.contains(uuid)) {
                        msg = "DEBUG -- SKIP UUID because it has been seen before: [" +
                        		uuid + "] ------";
                        LOGGER.debug(msg);
                    	System.out.println(msg);
                        skippedNodeCount++;
                        continue;
                    }
                        
                    uris.add(uri);
                    uuids.add(uuid);
                    idToUri.put(id, uri);
                    idToType.put(id, nodeType);
                    idToUuid.put(id, uuid);
                      
                    // Collect Edge Info for this node
                    if (vertex.has("inE")) {
                        vertex.get("inE").getAsJsonObject().entrySet().forEach(es -> {
                            String label = es.getKey();
                            es.getValue().getAsJsonArray().forEach(e -> {
                                long otherId = e.getAsJsonObject().get("outV").getAsLong();
                                idIdLabelOfEdges.add(new Triplet<>(id, otherId, label));
                            });
                        });
                    }

                    if( !tableCountHash.containsKey(nodeTypeKey)) {
                    	int ct = 1;
                    	tableCountHash.put(nodeTypeKey, ct);
                    }
                    else {
                    	int tmpCt = tableCountHash.get(nodeTypeKey);
                    	tmpCt++;
                    	tableCountHash.remove(nodeTypeKey);
                    	tableCountHash.put(nodeTypeKey, tmpCt);
                    }
                    nodeCounter++;
                }// end of looping over this file
            } catch (IOException e) {
        	    String msg = "DEBUG --  Error while processing nodes ------";
                LOGGER.debug(msg);
    		    System.out.println(msg);
            }
        }// End of looping over all files
        
        String msg = "DEBUG -- Found this many Kinds of nodes: " + tableCountHash.size();
        LOGGER.debug(msg);
 		System.out.println(msg);
        
        msg = "DEBUG -- Found this many total nodes: " + nodeCounter;
        LOGGER.debug(msg);
		System.out.println(msg);
		
        msg = "  >> Skipped a total of " + skippedNodeCount + " Node Records ------";
        LOGGER.debug(msg);
		System.out.println(msg);
		
        
        msg = "DEBUG -- Begin Processing Edges ------";
        LOGGER.debug(msg);
		System.out.println(msg);
        
		int edgeTableCounter = 0;
        int edgeSkipCounter = 0;
        
        Map<String, String> edgeKeys = this.getEdgeMapKeys();
        for (Triplet<Long, Long, String> edge : idIdLabelOfEdges) {
            if (!idToType.containsKey(edge.getValue0())) {
            	LOGGER.info(" Edge Skipped because ID not found: [" + edge.getValue0() + "]");
                System.out.println(" Edge Skipped because ID not found: [" + edge.getValue0() + "]");
                edgeSkipCounter++;
                continue;
            }
            else if (!idToType.containsKey(edge.getValue1())) {
                System.out.println(" Edge Skipped because ID not found: [" + edge.getValue1() + "]");
                LOGGER.info(" Edge Skipped because ID not found: [" + edge.getValue1() + "]");
                edgeSkipCounter++;
                continue;
            }
            else {
                String colA = idToType.get(edge.getValue1()).replace("-","_");
                String colB = idToType.get(edge.getValue0()).replace("-","_");
                
                String edLabel = edge.getValue2(); // if we start using edLabel to sort
                String edKey = colA + "__" + colB;
                // Note the edgeKeys table has nodeTypeX__nodeTypeY as the key
                //  The value stored for that key just has "edge__" pre-pended which 
                //  is the key used by the tableCount thing
                String tcEdKey = "";
                if (!edgeKeys.containsKey(edKey)) {
                    tcEdKey = edgeKeys.get(colB + "__" + colA );
                } else {
                    tcEdKey = edgeKeys.get(edKey);
                }
              
                if( !tableCountHash.containsKey(tcEdKey)) {
                	int ct = 1;
                	tableCountHash.put(tcEdKey, ct);
                	edgeTableCounter++;
                }
                else {
                	int tmpCt = tableCountHash.get(tcEdKey);
                	tmpCt++;
                	tableCountHash.remove(tcEdKey);
                	tableCountHash.put(tcEdKey, tmpCt);
                }
                edgeCounter++;
            }
        }
            
        msg = " Processed a total of " + edgeCounter + " Edge Records ------";
        LOGGER.debug(msg);
        System.out.println(msg);
        
        msg = " Found data for this many edgeTables: " + edgeTableCounter;
        LOGGER.debug(msg);
        System.out.println(msg);
    		
        msg = "  >> Skipped a total of " + edgeSkipCounter + " Edge Records ------";
        LOGGER.debug(msg);
    	System.out.println(msg);
           
        return tableCountHash;
        
    }



}
