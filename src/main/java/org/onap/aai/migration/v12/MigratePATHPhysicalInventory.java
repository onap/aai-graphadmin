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
import java.nio.file.Paths;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.Iterator;
import java.util.List;
import java.util.Map;
import java.util.Optional;

import org.apache.tinkerpop.gremlin.process.traversal.dsl.graph.GraphTraversal;
import org.apache.tinkerpop.gremlin.process.traversal.dsl.graph.GraphTraversalSource;
import org.apache.tinkerpop.gremlin.structure.Vertex;
import org.onap.aai.db.props.AAIProperties;
import org.onap.aai.edges.EdgeIngestor;
import org.onap.aai.introspection.Introspector;
import org.onap.aai.introspection.LoaderFactory;
import org.onap.aai.migration.MigrationDangerRating;
import org.onap.aai.migration.MigrationPriority;
import org.onap.aai.migration.Migrator;
import org.onap.aai.migration.Status;
import org.onap.aai.serialization.db.EdgeSerializer;
import org.onap.aai.serialization.engines.TransactionalGraphEngine;
import org.onap.aai.setup.SchemaVersions;
import org.onap.aai.util.AAIConfig;


@MigrationPriority(26)
@MigrationDangerRating(100)
public class MigratePATHPhysicalInventory extends Migrator {

	private static List<String> lagPortList = new ArrayList<String>();
	private static Map<String, Vertex> pnfList = new HashMap<String, Vertex>();
	private final String LAGINTERFACE_NODE_TYPE = "lag-interface";
	private final String PNF_NODE_TYPE = "pnf";
	private final String PROPERTY_PNF_NAME = "pnf-name";
	private final String PROPERTY_INTERFACE_NAME = "interface-name";
	private final String LAG_INTERFACE_NODE_TYPE = "lag-interface";
	private static boolean success = true;
    private static boolean checkLog = false;
    private static GraphTraversalSource g = null;
    private int headerLength;
    
    private static List<String> dmaapMsgList = new ArrayList<String>();
    private static final String homeDir = System.getProperty("AJSC_HOME");
    
    //Create a map to store the evcs processed where lag-interfaces were found to track the sequence of ports
    //key contains the evcName
    //value is a map that contains the mapping for sequence of forwarders and corresponding portAids in the order they are found 
    
    private static Map<String, Map<Vertex, String>> pathFileMap = new HashMap<String, Map<Vertex, String>>();
  
    private static int processedLagInterfacesCount = 0;
    private static int skippedRowCount = 0;
    //Map with lineNumber and the reason for failure for each interface
    private static Map<String, String> lagInterfacesNotProcessedMap = new HashMap<String, String>();
    
	
    public MigratePATHPhysicalInventory(TransactionalGraphEngine engine, LoaderFactory loaderFactory, EdgeIngestor edgeIngestor, EdgeSerializer edgeSerializer, SchemaVersions schemaVersions) {
        super(engine, loaderFactory, edgeIngestor, edgeSerializer, schemaVersions);
        this.g = this.engine.asAdmin().getTraversalSource();
    }

    @Override
    public void run() {
        logger.info("---------- Start migration of PATH file Physical Inventory  ----------");
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
        String fileName = feedDir+ "path.csv";
        logger.info(fileName);
        logger.info("---------- Processing PATH Entries from file  ----------");
        try  {
        	List<String> lines = Files.readAllLines(Paths.get(fileName));
            Iterator<String> lineItr = lines.iterator();
            while (lineItr.hasNext()){
                String line = lineItr.next().replace("\n", "").replace("\r", "");
                logger.info("\n");
                if (!line.isEmpty()) {
                    if (fileLineCounter != 0) {
                        String[] colList = line.split("\\s*,\\s*", -1);
                        Map<String, String> pathColValues = new HashMap<String, String>();
                        pathColValues.put("evcName", colList[1]);
                        pathColValues.put("bearerFacingCircuit", colList[4]);
                        pathColValues.put("bearerCvlan", colList[6]);
                    	pathColValues.put("bearerSvlan", colList[7]);
                    	pathColValues.put("bearerPtniiName", colList[8]);
                    	pathColValues.put("bearerPortAid", colList[12]);
                    	pathColValues.put("collectorFacingCircuit", colList[14]);
                    	pathColValues.put("collectorCvlan", colList[16]);
                    	pathColValues.put("collectorSvlan", colList[17]);
                    	pathColValues.put("collectorPtniiName", colList[18]);
                    	pathColValues.put("collectorPortAid", colList[22]);
                    	
                    	// For each row, check if the collector and bearerPnfs exist and create lag interfaces
                    	
                    	validateCollectorPnfAndCreateLagInterface(pathColValues, (fileLineCounter+1));
                    	validateBearerPnfAndCreateLagInterface(pathColValues, (fileLineCounter+1));
                    	
                    } else {
                        this.headerLength = line.split("\\s*,\\s*", -1).length;
                        logger.info("headerLength: " + headerLength);
                        if (this.headerLength < 21){
                            logger.info("ERROR: Input file should have 21 columns");
                            this.success = false;
                            return;
                        }
                    }
                }
                fileLineCounter++;
            }
            logger.info ("\n \n ******* Final Summary for PATH FILE Physical Inventory Migration ********* \n");
            logger.info("Lag Interfaces processed: "+processedLagInterfacesCount);
            logger.info("Total Rows Count: "+(fileLineCounter + 1));
            logger.info("Fallout Lag Interfaces Count : "+lagInterfacesNotProcessedMap.size() +"\n");
            
            if (!lagInterfacesNotProcessedMap.isEmpty()) {
            	logger.info("------ Fallout Details: ------");
            	lagInterfacesNotProcessedMap.forEach((lineEntry, errorMsg) -> {
            		int lineNumberIndex = lineEntry.indexOf("-");
            		String lineNumber = lineEntry.substring(0, lineNumberIndex);
            		String portDetail = lineEntry.substring(lineNumberIndex+1);
            		logger.info(errorMsg + ": on row "+ lineNumber +" for PortAid ["+ portDetail+"]");
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
            e.printStackTrace();
            success = false;
        }
    }
    
    
	private void validateBearerPnfAndCreateLagInterface(Map<String, String> pathColValues, int lineNumber) {
		String bearerPtniiName = pathColValues.get("bearerPtniiName");
    	String bearerPortAid = pathColValues.get("bearerPortAid");
    	Vertex pnfVtx = getPnf(bearerPtniiName);
    	if (pnfVtx != null){
    		//create lag-interface
    		createLagInterfaceObject(pnfVtx, bearerPortAid, lineNumber);
    	} else {
    		int lagIdentifierIndex = bearerPortAid.indexOf("_");
    		if (lagIdentifierIndex > 0) {
    			lagInterfacesNotProcessedMap.put(""+ lineNumber+ "-"+bearerPtniiName+"-"+bearerPortAid+"", "Pnf ["+bearerPtniiName+"] not found" );
    		}
    	}
		
	}

	private void validateCollectorPnfAndCreateLagInterface(Map<String, String> pathColValues, int lineNumber) {
		String collectorPtniiName = pathColValues.get("collectorPtniiName");
    	String collectorPortAid = pathColValues.get("collectorPortAid");
    	Vertex pnfVtx = getPnf(collectorPtniiName);
    	if (pnfVtx != null){
    		//create lag-interface
    		createLagInterfaceObject(pnfVtx, collectorPortAid, lineNumber);
    	}else {
    		int lagIdentifierIndex = collectorPortAid.indexOf("_");
    		if (lagIdentifierIndex > 0) {
    			lagInterfacesNotProcessedMap.put(""+ lineNumber+ "-"+collectorPtniiName+"-"+collectorPortAid+"", "Pnf ["+collectorPtniiName+"] not found" );
    		}
    	}
	}
	
	private void createLagInterfaceObject(Vertex pnfVtx, String portAid, int lineNumber) {
		String pnfName = pnfVtx.value(PROPERTY_PNF_NAME);
		
		if (pnfName != null && !pnfName.isEmpty()) {
			
			if(portAid == null ||  portAid.isEmpty()){
				logger.info("\t Invalid Port entry [" +portAid + "] - Invalid record - skipping..." );
			} else{
				if (!AAIConfig.isEmpty(portAid)) {
					GraphTraversal<Vertex, Vertex> portList;
					
					boolean isPortAidALagIntf = false;
					String interfaceName = null;
					
					int lagIdentifierIndex = portAid.indexOf("_");
					
					if (lagIdentifierIndex > 0) {
						String[] subStringList = portAid.split("_");
						interfaceName = subStringList[0];
						isPortAidALagIntf = true;
					}
					
					if (isPortAidALagIntf)
					{
						try {
							
							if (lagPortList != null && lagPortList.contains(pnfName+"_"+interfaceName)){
								logger.info("\t lag-interface [" + interfaceName	+ "] already exists in AAI - skipping");
								return;
							}
								
								
							portList = g.V(pnfVtx).in("tosca.relationships.network.BindsTo").has("interface-name", interfaceName).has("aai-node-type", "lag-interface");
					    	if (portList!= null && portList.hasNext()) {
					    		Vertex lagInterfaceVtx = portList.next();
					            if (lagInterfaceVtx != null && lagInterfaceVtx.property("interface-name").isPresent()) {
									logger.info("\t lag-interface [" + interfaceName	+ "] already exists in AAI - skipping");
//									lagInterfacesNotProcessedMap.put(""+lineNumber+"-"+pnfName+"-"+portAid+"", "lag-interface already exists for ["+interfaceName+"]" );
								}
					        }
					        else if (portList == null || !portList.hasNext()) {
					        	//Create lag-interface in pnf
								Introspector lagInterface = loader.introspectorFromName(LAG_INTERFACE_NODE_TYPE);
								
								Vertex lagIntVtx = serializer.createNewVertex(lagInterface);
								lagInterface.setValue("interface-name", interfaceName);
								this.createTreeEdge(pnfVtx, lagIntVtx);
								serializer.serializeSingleVertex(lagIntVtx, lagInterface, "migrations");
								
								logger.info("\t Created new lag-interface " + lagIntVtx + " with interface-name= " + lagIntVtx.value("interface-name"));
								
								processedLagInterfacesCount++;
								lagPortList.add(pnfName+"_"+interfaceName);
								
								String dmaapMsg = System.nanoTime() + "_" + lagIntVtx.id().toString() + "_"	+ lagIntVtx.value("resource-version").toString();
								dmaapMsgList.add(dmaapMsg);
//								Introspector introspector = serializer.getLatestVersionView(lagIntVtx);
//								this.notificationHelper.addEvent(lagIntVtx, introspector, EventAction.CREATE, this.serializer.getURIForVertex(lagIntVtx, false));
//								logger.info("\t Dmaap event sent for " + lagIntVtx + " with interface-name= " + lagIntVtx.value("interface-name").toString() );
					        }
						} catch (Exception e) {
							logger.info("\t ERROR: Failure to create lag-interface ["+ interfaceName + "]");
							lagInterfacesNotProcessedMap.put(""+lineNumber+"-"+pnfName+"-"+portAid+"", "Failed to create lag-interface ["+interfaceName+"]" );
						}
					} 
					else 
					{
						logger.info("\t Port-Aid[" +portAid +"] on PNF["+pnfName+"] not a lag-interface, skipping....");
					}
				}
				
			}
		}
	}

	
	private Vertex getPnf(String ptniiName) {
		Vertex pnfVtx = null;
		if (!AAIConfig.isEmpty(ptniiName)) {
			if (!pnfList.isEmpty() && pnfList.containsKey(ptniiName)){
				return pnfList.get(ptniiName);
			}
			List<Vertex> collectorPnfList = g.V().has(this.PROPERTY_PNF_NAME, ptniiName).has(AAIProperties.NODE_TYPE, PNF_NODE_TYPE).toList();
			if (collectorPnfList != null && collectorPnfList.size() == 1) {
				pnfVtx = collectorPnfList.get(0);
				pnfList.put(ptniiName, pnfVtx);
				logger.info("\t Pnf [" + ptniiName + "] found in AAI");
			} else if (collectorPnfList == null || collectorPnfList.size() == 0) {
				logger.info("\t ERROR: Failure to find Pnf [" + ptniiName	+ "]" );
			}
		} else {
			logger.info("\t ERROR: Failure to find Pnf [" + ptniiName	+ "]" );
		}
		return pnfVtx;
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
        return Optional.of(new String[]{this.LAG_INTERFACE_NODE_TYPE});
    }

    @Override
    public String getMigrationName() {
        return "MigratePATHPhysicalInventory";
    }
}
