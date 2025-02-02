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
package org.onap.aai.migration.v12;
/*-
 * ============LICENSE_START=======================================================
 * org.onap.aai
 * ================================================================================
 * Copyright (C) 2017 AT&T Intellectual Property. All rights reserved.
 * ================================================================================
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 * 
 *      http://www.apache.org/licenses/LICENSE-2.0
 * 
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 * ============LICENSE_END=========================================================
 */


import java.io.FileNotFoundException;
import java.io.IOException;
import java.nio.file.Files;
import java.nio.file.Path;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.Iterator;
import java.util.List;
import java.util.Map;
import java.util.Optional;

import org.apache.tinkerpop.gremlin.process.traversal.dsl.graph.GraphTraversalSource;
import org.apache.tinkerpop.gremlin.structure.Vertex;
import org.onap.aai.db.props.AAIProperties;
import org.onap.aai.edges.EdgeIngestor;
import org.onap.aai.introspection.LoaderFactory;
import org.onap.aai.migration.MigrationDangerRating;
import org.onap.aai.migration.MigrationPriority;
import org.onap.aai.migration.Migrator;
import org.onap.aai.migration.Status;
import org.onap.aai.serialization.db.EdgeSerializer;
import org.onap.aai.serialization.engines.TransactionalGraphEngine;
import org.onap.aai.setup.SchemaVersions;
import org.onap.aai.util.AAIConfig;


@MigrationPriority(31)
@MigrationDangerRating(100)
//@Enabled
public class MigrateHUBEvcInventory extends Migrator {

	private static final String FORWARDER_EVC_NODE_TYPE = "forwarder-evc";
	
	private static boolean success = true;
    private static boolean checkLog = false;
    private static GraphTraversalSource g = null;
    private int headerLength;
  
    private static int processedEvcsCount = 0;
    private static int falloutRowsCount = 0;
    private static List<String> processedEvcsList = new ArrayList<String>();
    private static Map<String, String> falloutLinesMap = new HashMap<String, String>();
    
    private static final String homeDir = System.getProperty("AJSC_HOME");
   	private static List<String> dmaapMsgList = new ArrayList<String>();
    
	
    public MigrateHUBEvcInventory(TransactionalGraphEngine engine, LoaderFactory loaderFactory, EdgeIngestor edgeIngestor, EdgeSerializer edgeSerializer, SchemaVersions schemaVersions) {
        super(engine, loaderFactory, edgeIngestor, edgeSerializer, schemaVersions);
        this.g = this.engine.asAdmin().getTraversalSource();
    }

    @Override
    public void run() {
        logger.info("---------- Start migration of HUB EVC Inventory  ----------");
        String homeDir = System.getProperty("AJSC_HOME");
        String configDir = System.getProperty("BUNDLECONFIG_DIR");
        if (homeDir == null) {
            logger.info("ERROR: Could not find sys prop AJSC_HOME");
            success = false;
            return;
        }
        if (configDir == null) {
            success = false;
            return;
        }
        
        String feedDir = homeDir + "/" + configDir + "/" + "migration-input-files/sarea-inventory/";
        int fileLineCounter = 0;
        String fileName = feedDir+ "hub.csv";
        logger.info(fileName);
        logger.info("---------- Processing HUB Entries from file  ----------");
        try {
            String line;
            List<String> lines = Files.readAllLines(Path.of(fileName));
            Iterator<String> lineItr = lines.iterator();
            while (lineItr.hasNext()){
            	line = lineItr.next();
                logger.info("\n");
                if (!line.isEmpty()) {
                    if (fileLineCounter != 0) {
                        String[] colList = line.split(",", -1);
//                        if (colList.length != headerLength) {
//                            logger.info("ERROR: HUB line entry should contain " + headerLength + " columns, contains " + colList.length + " instead.");
//                            success = false;
//                            continue;
//                        }
                        Map<String, String> hubColValues = new HashMap<String, String>();
                        hubColValues.put("ivlan", colList[1].trim());
                        hubColValues.put("nniSvlan", colList[3].trim());
                        hubColValues.put("evcName", colList[4].trim());
                    	
                    	String evcName = hubColValues.get("evcName");
                    	String ivlan = hubColValues.get("ivlan");
                    	String nniSvlan = hubColValues.get("nniSvlan");
                        if (!AAIConfig.isEmpty(evcName)) {
                        	logger.info("---------- Processing Line " + line + "----------");
                            logger.info("\t Evc Name = " + evcName );
                            
                            List<Vertex> forwarderEvcList = g.V().has ("forwarding-path-id", evcName).has(AAIProperties.NODE_TYPE, "forwarding-path")
                                    .in("org.onap.relationships.inventory.BelongsTo").has("aai-node-type", "forwarder")
                            		.out("org.onap.relationships.inventory.Uses").has("aai-node-type", "configuration")
                            		.in("org.onap.relationships.inventory.BelongsTo").has("aai-node-type", "forwarder-evc").toList();
                                    		
                            
                            if (forwarderEvcList == null || forwarderEvcList.isEmpty()){
                                logger.info("\t ERROR: Forwarder-evc does not exist for evc-id = " + evcName + " - skipping");
                                falloutLinesMap.put(String.valueOf(fileLineCounter+1), "["+evcName+"] - Forwarder-evc does not exist" );
                                falloutRowsCount++;
                            }
                            else if (forwarderEvcList!= null && !forwarderEvcList.isEmpty()) {
                            	Iterator<Vertex> listItr = forwarderEvcList.iterator();
                            	while (listItr.hasNext()){
	                            	Vertex forwarderEvcVtx = listItr.next();
									if (forwarderEvcVtx != null && forwarderEvcVtx.property("forwarder-evc-id").isPresent() && !AAIConfig.isEmpty(ivlan )) {
										boolean isUpdated = updateIvlanOnForwarder(forwarderEvcVtx, ivlan, nniSvlan );
										if (!isUpdated){
											falloutLinesMap.put(String.valueOf(fileLineCounter+1), "["+evcName+"] - Forwarder-evc does not have svlan populated" );
			                                falloutRowsCount++;
										}
	                            	}
	                            }
                            	if (!processedEvcsList.contains(evcName)) {
                            		processedEvcsList.add(evcName);
                            		processedEvcsCount++;
                            	}
                            	
                            }
                        }
                    } else {
                        this.headerLength = line.split(",", -1).length;
                        logger.info("headerLength: " + headerLength);
                        if (this.headerLength < 5){
                            logger.info("ERROR: Input file should have 5 columns");
                            MigrateHUBEvcInventory.success = false;
                            return;
                        }
                    }
                }
               
                fileLineCounter++;
            }
            
            logger.info ("\n \n ******* Final Summary for HUB FILE Migration ********* \n");
            logger.info("Evcs processed: "+processedEvcsCount);
            logger.info("Total Rows Count: "+(fileLineCounter + 1));
            logger.info("Fallout Rows Count : "+falloutRowsCount +"\n");
            if (!falloutLinesMap.isEmpty()) {
            	logger.info("------ Fallout Details: ------");
            	falloutLinesMap.forEach((lineNumber, errorMsg) -> {
            		logger.info(errorMsg + ": on row "+lineNumber.toString());
            	});
            }
            
      } catch (FileNotFoundException e) {
            logger.info("ERROR: Could not file file " + fileName, e.getMessage());
            success = false;
            checkLog = true;
        } catch (IOException e) {
            logger.info("ERROR: Issue reading file " + fileName, e);
            success = false;
        } catch (Exception e) {
            logger.info("encountered exception", e.getMessage());
            success = false;
        }
    }
    
    private boolean updateIvlanOnForwarder(Vertex forwarderEvcVtx, String ivlan, String nniSvlan) throws Exception {
    	
    	boolean isUpdated = true;
    	String forwarderEvcId = forwarderEvcVtx.value("forwarder-evc-id");
    	
    	String forwarderSvlan = null;
    	if( forwarderEvcVtx.property("svlan").isPresent()) {
    		forwarderSvlan = forwarderEvcVtx.value("svlan");
    	}
    	if (forwarderSvlan != null && !forwarderSvlan.isEmpty()) {
			int forwarderSvlanValue = Integer.parseInt(forwarderSvlan);
			int nniSvlanValue = Integer.parseInt(nniSvlan);
			if (forwarderSvlan != null && nniSvlan != null && (forwarderSvlanValue == nniSvlanValue)) {
				if (ivlan != null && !ivlan.isEmpty()) {
					if (forwarderEvcVtx.property("ivlan").isPresent()) {
						String forwarderIvlan = forwarderEvcVtx.value("ivlan");
						if (forwarderIvlan != null && !forwarderIvlan.isEmpty()) {
							if (Integer.parseInt(forwarderIvlan) == Integer.parseInt(ivlan)) {
								logger.info("\t Skipped update ivlan for  forwarder-evc[" + forwarderEvcId
										+ "], ivlan already set to expected value");
							} else {
								logger.info("\t Start ivlan update for forwarder-evc[" + forwarderEvcId + "]");
								updateIvlan(forwarderEvcVtx, ivlan, forwarderEvcId);
							}
						}
					} else {
						updateIvlan(forwarderEvcVtx, ivlan, forwarderEvcId);
					}
				}
			}
		} else {
			logger.info("Skipping ivlan update, svlan is not present on the forwarder-evc ["+forwarderEvcId +"]" );
			isUpdated = false;
		}
		return isUpdated;
	}

	private void updateIvlan(Vertex forwarderEvcVtx, String ivlan, String forwarderEvcId) {
		forwarderEvcVtx.property("ivlan", ivlan);
		this.touchVertexProperties(forwarderEvcVtx, false);
		logger.info("\t Updated ivlan to "+ ivlan	+ " on forwarder-evc["
				+ forwarderEvcId + "]");
		String dmaapMsg = System.nanoTime() + "_" + forwarderEvcVtx.id().toString() + "_"	+ forwarderEvcVtx.value("resource-version").toString();
		dmaapMsgList.add(dmaapMsg);
//		try {
//			final Introspector evcIntrospector = serializer.getLatestVersionView(forwarderEvcVtx);
//			this.notificationHelper.addEvent(forwarderEvcVtx, evcIntrospector, EventAction.UPDATE,
//					this.serializer.getURIForVertex(forwarderEvcVtx, false));
//		} catch (UnsupportedEncodingException e) {
//			logger.info("\t ERROR: Could not update ivlan on forwader-evc "	+ forwarderEvcVtx, e.getMessage());
//		} catch (AAIException e) {
//			logger.info("\t ERROR: Could not update ivlan on forwarder-evc "+ forwarderEvcVtx, e.getMessage());
//		}
	}


    @Override
    public Status getStatus() {
        if (checkLog) {
            return Status.CHECK_LOGS;
        }
        else if (success) {
            return Status.SUCCESS;
        }
        else {
            return Status.FAILURE;
        }
    }

    @Override
	public void commit() {
		engine.commit();
        createDmaapFiles(dmaapMsgList);
	}

    @Override
    public Optional<String[]> getAffectedNodeTypes() {
        return Optional.of(new String[]{MigrateHUBEvcInventory.FORWARDER_EVC_NODE_TYPE});
    }

    @Override
    public String getMigrationName() {
        return "MigrateHUBEvcInventory";
    }
}
