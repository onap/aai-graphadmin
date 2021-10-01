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


import java.io.BufferedReader;
import java.io.FileNotFoundException;
import java.io.FileReader;
import java.io.IOException;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.Map;
import java.util.Optional;
import java.util.List;

import org.apache.tinkerpop.gremlin.process.traversal.dsl.graph.GraphTraversalSource;
import org.apache.tinkerpop.gremlin.structure.Vertex;
import org.onap.aai.edges.EdgeIngestor;
import org.onap.aai.introspection.LoaderFactory;
import org.onap.aai.migration.*;
import org.onap.aai.db.props.AAIProperties;
import org.onap.aai.serialization.db.EdgeSerializer;
import org.onap.aai.serialization.engines.TransactionalGraphEngine;
import org.onap.aai.setup.SchemaVersions;
import org.onap.aai.util.AAIConfig;


@MigrationPriority(28)
@MigrationDangerRating(100)
public class MigrateINVEvcInventory extends Migrator {

	private static final String PROPERTY_EVC_ID = "evc-id";
	private static final String EVC_NODE_TYPE = "evc";
	
	private static boolean success = true;
    private static boolean checkLog = false;
    private static GraphTraversalSource g = null;
    private int headerLength;
  
    private static int processedEvcsCount = 0;
    private static int falloutEvcsCount = 0;
    private static Map<String, String> falloutEvcsMap = new HashMap<String, String>();
    
    private static final String homeDir = System.getProperty("AJSC_HOME");
	private static List<String> dmaapMsgList = new ArrayList<String>();
    
	
    public MigrateINVEvcInventory(TransactionalGraphEngine engine, LoaderFactory loaderFactory, EdgeIngestor edgeIngestor, EdgeSerializer edgeSerializer, SchemaVersions schemaVersions) {
        super(engine, loaderFactory, edgeIngestor, edgeSerializer, schemaVersions);
        this.g = this.engine.asAdmin().getTraversalSource();
    }

    @Override
    public void run() {
        logger.info("---------- Start migration of INV EVC Inventory  ----------");
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
        String fileName = feedDir+ "inv.csv";
        logger.info(fileName);
        logger.info("---------- Processing INV Entries from file  ----------");
        try (BufferedReader br = new BufferedReader(new FileReader(fileName))) {
            String line;
            while ((line = br.readLine()) != null) {
                line = line.replace("\n", "").replace("\r", "");
                logger.info("\n");
                if (!line.isEmpty()) {
                    if (fileLineCounter != 0) {
                        String[] colList = line.split(",", -1);
                        if (colList.length != headerLength) {
                            logger.info("ERROR: INV line should contain " + headerLength + " columns, contains " + colList.length + " instead.");
                            continue;
                        }
                        Map<String, String> invColValues = new HashMap<String, String>();
                        invColValues.put("evcName", colList[22].trim());
                        invColValues.put("collectorInterconnectType", colList[17].trim());
                    	
                    	String evcName = invColValues.get("evcName");
                    	String interconnectType = invColValues.get("collectorInterconnectType");
                        if (!AAIConfig.isEmpty(evcName) && !AAIConfig.isEmpty(interconnectType) ) {
                        	logger.info("---------- Processing Line " + line + "----------");
                            logger.info("\t Evc Name = " + evcName );
                            
                            // For each provided evc-name, check if the evc already exists
                            List<Vertex> existingEvcList = g.V().has(PROPERTY_EVC_ID, evcName).has(AAIProperties.NODE_TYPE, EVC_NODE_TYPE).toList();
                            if (existingEvcList == null || existingEvcList.size() == 0){
                                logger.info("\t ERROR: Evc does not exist with evc-id = " + evcName + " - skipping");
                                falloutEvcsCount++;
                                falloutEvcsMap.put((fileLineCounter+1)+"", "["+evcName+"] - Evc does not exist" );
                            }
                            else if (existingEvcList!= null && existingEvcList.size() == 1) {
                            	Vertex evcVtx = existingEvcList.get(0);
								if (evcVtx != null && !AAIConfig.isEmpty(interconnectType )) {
									updateEvcInterconnectType(evcVtx, interconnectType );
                            	}
                            	processedEvcsCount++;
                            }
                            else if (existingEvcList!= null && existingEvcList.size() > 1) {
                            	 logger.info("\t ERROR: More than one EVC exist with evc-id = " + evcName + " - skipping");
                                 falloutEvcsCount++;
                                 falloutEvcsMap.put((fileLineCounter+1)+"", "["+evcName+"] - More than one EVC exist with evc-id" );
                            }
                        } else {
                        	logger.info("---------- Processing Line " + line + "----------");
                        	logger.info("Invalid line entry : evcName: "+evcName + " interConnectType: "+ interconnectType);
                        	continue;
                        }
                    } else {
                        this.headerLength = line.split(",", -1).length;
                        logger.info("headerLength: " + headerLength);
                        if (this.headerLength < 23){
                            logger.info("ERROR: Input file should have 23 columns");
                            this.success = false;
                            return;
                        }
                    }
                }
               
                fileLineCounter++;
            }
            
            logger.info ("\n \n ******* Final Summary for INV FILE Migration ********* \n");
            logger.info("Evcs processed: "+processedEvcsCount);
            logger.info("Fallout Evcs count: "+falloutEvcsCount);
            if (!falloutEvcsMap.isEmpty()) {
            	logger.info("------ Fallout Details: ------");
            	falloutEvcsMap.forEach((lineNumber, errorMsg) -> {
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
            logger.info("encountered exception", e);
            success = false;
        }
    }
    
    
    private void updateEvcInterconnectType(Vertex evcVtx, String interconnectType) {
    	
    	String evcId = evcVtx.value("evc-id");
    	if (interconnectType != null && !interconnectType.isEmpty()){
			evcVtx.property("inter-connect-type-ingress", interconnectType);
	    	this.touchVertexProperties(evcVtx, false);
	    	logger.info("\t Updated inter-connect-type-ingress property for evc [" + evcId +"]");
	    	String dmaapMsg = System.nanoTime() + "_" + evcVtx.id().toString() + "_"	+ evcVtx.value("resource-version").toString();
			dmaapMsgList.add(dmaapMsg);
//			try {
//				final Introspector evcIntrospector = serializer.getLatestVersionView(evcVtx);
//				this.notificationHelper.addEvent(evcVtx, evcIntrospector, EventAction.UPDATE, this.serializer
//						.getURIForVertex(evcVtx, false));
//			} catch (UnsupportedEncodingException e) {
//				logger.info("\t ERROR: Could not send update notification for evc " + evcId, e.getMessage());
//			} catch (AAIException e) {
//				logger.info("\t ERROR: Could not send update notification for evc " + evcId, e.getMessage());
//			}
    	}
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
        return Optional.of(new String[]{MigrateINVEvcInventory.EVC_NODE_TYPE});
    }

    @Override
    public String getMigrationName() {
        return "MigrateINVEvcInventory";
    }
}
