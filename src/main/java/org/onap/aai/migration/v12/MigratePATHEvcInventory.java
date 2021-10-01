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
import org.apache.tinkerpop.gremlin.process.traversal.dsl.graph.__;
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


@MigrationPriority(29)
@MigrationDangerRating(100)
public class MigratePATHEvcInventory extends Migrator {

	private static Map<String, Vertex> portList = new HashMap<String, Vertex>();
	private static Map<String, Vertex> pnfList = new HashMap<String, Vertex>();
	private final String FORWARDER_EVC_NODE_TYPE = "forwarder-evc";
	private final String LAGINTERFACE_NODE_TYPE = "lag-interface";
	private final String CONFIGURATION_NODE_TYPE = "configuration";
	private final String FORWARDING_PATH_NODE_TYPE = "forwarding-path";
	private final String FORWARDING_PATH_ID = "forwarding-path-id";
	private final String PROPERTY_CONFIGURATION_ID = "configuration-id";
	private final String PNF_NODE_TYPE = "pnf";
	private final String PROPERTY_PNF_NAME = "pnf-name";
	private final String PROPERTY_INTERFACE_NAME = "interface-name";
	private final String PINTERFACE_NODE_TYPE = "p-interface";
	private static boolean success = true;
    private static boolean checkLog = false;
    private static GraphTraversalSource g = null;
    private int headerLength;
    
    //Create a map to store the evcs processed where lag-interfaces were found to track the sequence of ports
    //key contains the evcName
    //value is a map that contains the mapping for sequence of forwarders and corresponding portAids in the order they are found 
    
    private static Map<String, Map<Vertex, String>> pathFileMap = new HashMap<String, Map<Vertex, String>>();
  
    private static int processedEvcsCount = 0;
    private static int falloutEvcsCount = 0;
    
    //Map with lineNumber and the reason for failure for each EVC
    private static Map<Integer, String> falloutEvcsList = new HashMap<Integer, String>();
    private static final String homeDir = System.getProperty("AJSC_HOME");
	private static List<String> dmaapMsgList = new ArrayList<String>();
	
	
    public MigratePATHEvcInventory(TransactionalGraphEngine engine, LoaderFactory loaderFactory, EdgeIngestor edgeIngestor, EdgeSerializer edgeSerializer, SchemaVersions schemaVersions) {
        super(engine, loaderFactory, edgeIngestor, edgeSerializer, schemaVersions);
        this.g = this.engine.asAdmin().getTraversalSource();
    }

    @Override
    public void run() {
        logger.info("---------- Start migration of PATH EVC Inventory  ----------");
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
        try {
        	List<String> lines = Files.readAllLines(Paths.get(fileName));
            Iterator<String> lineItr = lines.iterator();
            while (lineItr.hasNext()){
                String line = lineItr.next().replace("\n", "").replace("\r", "");
                logger.info("\n");
                if (!line.isEmpty()) {
                    if (fileLineCounter != 0) {
                        String[] colList = line.split(",", -1);
                        if (colList.length != headerLength) {
                            logger.info("ERROR: PATH line should contain " + headerLength + " columns, contains " + colList.length + " instead.");
//                            success = false;
                            continue;
                        }
                        Map<String, String> pathColValues = new HashMap<String, String>();
                        pathColValues.put("evcName", colList[1].trim());
                        pathColValues.put("bearerFacingCircuit", colList[4].trim());
                        pathColValues.put("bearerCvlan", colList[6].trim());
                    	pathColValues.put("bearerSvlan", colList[7].trim());
                    	pathColValues.put("bearerPtniiName", colList[8].trim());
                    	String bearerPortAid = colList[12].trim().replaceAll("^\"|\"$", "").replaceAll("\\s+","");
                    	pathColValues.put("bearerPortAid", bearerPortAid);
                    	pathColValues.put("collectorFacingCircuit", colList[14].trim());
                    	pathColValues.put("collectorCvlan", colList[16].trim());
                    	pathColValues.put("collectorSvlan", colList[17].trim());
                    	pathColValues.put("collectorPtniiName", colList[18].trim());
                    	String collectorPortAid = colList[22].trim().replaceAll("^\"|\"$", "").replaceAll("\\s+","");
                    	pathColValues.put("collectorPortAid", collectorPortAid);
                    	
                    	
                    	String evcName = pathColValues.get("evcName");
                        if (!AAIConfig.isEmpty(evcName)) {
                        	logger.info("---------- Processing Line " + line + "----------");
                            logger.info("\t Evc Name = " + evcName );
                            
                            boolean isEntryValid = validatePnfsAndPorts(pathColValues, evcName);
                            
                            if (!isEntryValid){
                            	logger.info("\t ERROR: Skipping processing for line containing evc-name [" +evcName+ "]");
									falloutEvcsCount++;
									falloutEvcsList.put(Integer.valueOf(fileLineCounter -1 ), "["+ evcName +"] Ptnii or port does not exist");
								continue;
                            }
                            // Get the forwarding path containing forwarders
                            GraphTraversal<Vertex, Vertex> forwardingPathList = g.V().has(this.FORWARDING_PATH_ID, evcName).has(AAIProperties.NODE_TYPE, this.FORWARDING_PATH_NODE_TYPE)
                            		.where(__.in("org.onap.relationships.inventory.BelongsTo").has("aai-node-type","forwarder"));
                            
                            if (!forwardingPathList.hasNext()){
                            	createNewForwardersFromPATHData(pathColValues, evcName, fileLineCounter);
                            	processedEvcsCount++;
                            } else {
                            	Vertex forwardingPathVtx = forwardingPathList.next();
                            	List<Vertex> forwardersList = g.V(forwardingPathVtx.id()).in("org.onap.relationships.inventory.BelongsTo").toList();
                            	Iterator<Vertex> forwardersItr = forwardersList.iterator();
                            	List<String> forwarderRoleList =  new ArrayList<String>();
                            	while (forwardersItr.hasNext()){
                            		Vertex forwarderVtx = forwardersItr.next();
                            		String role = forwarderVtx.value("forwarder-role");
                            		if (role!= null ){
                            			forwarderRoleList.add(role);
                            		}
                            	}
                            	if (forwarderRoleList!= null && !forwarderRoleList.isEmpty()) {
                            		if (forwarderRoleList.contains("ingress") && forwarderRoleList.contains("egress")){
                            			logger.info("\t Skipping processing for EVC[" + evcName + "] because forwarders related to this EVC already exist.");
                            			falloutEvcsCount++;
                            			falloutEvcsList.put(Integer.valueOf(fileLineCounter -1 ), "["+ evcName +"] Forwarders already exists for EVC");
                            		} else {
                                		createNewForwardersFromPATHData(pathColValues, evcName, fileLineCounter);
                            		} 
                            	}
                            }
                        }
                    } else {
                        this.headerLength = line.split(",", -1).length;
                        logger.info("headerLength: " + headerLength);
                        if (this.headerLength < 24){
                            logger.info("ERROR: Input file should have 24 columns");
                            this.success = false;
                            return;
                        }
                    }
                }
                fileLineCounter++;
            }
            logger.info ("\n \n ******* Final Summary for PATH FILE Migration ********* \n");
            logger.info("Evcs processed: "+processedEvcsCount);
            logger.info("Total Rows Count: "+(fileLineCounter + 1));
            logger.info("Fallout Rows Count : "+falloutEvcsCount +"\n");
            if (!falloutEvcsList.isEmpty()) {
            	logger.info("------ Fallout Details: ------");
            	falloutEvcsList.forEach((lineNumber, errorMsg) -> {
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
    
    
	private boolean validatePnfsAndPorts(Map<String, String> pathColValues, String evcName) {
    	
    	String collectorPtniiName = pathColValues.get("collectorPtniiName");
    	String bearerPtniiName = pathColValues.get("bearerPtniiName");
    	String collectorPortAid = pathColValues.get("collectorPortAid");
    	String bearerPortAid = pathColValues.get("bearerPortAid");
		boolean isValid = validateCollectorPnf(collectorPtniiName, evcName) && validateBearerPnf(bearerPtniiName, evcName) 
				&& validateCollectorPort(pathColValues, collectorPortAid, collectorPtniiName, evcName) 
				&& validateBearerPort(pathColValues, bearerPortAid, bearerPtniiName, evcName) ;
		return isValid;
	}

	private boolean validateCollectorPnf(String collectorPtniiName, String evcName) {
		
		boolean isValid = false;
		if (!AAIConfig.isEmpty(collectorPtniiName)) {
			if (!pnfList.isEmpty() && pnfList.containsKey(collectorPtniiName)){
				isValid = true;
				logger.info("\t Pnf [" + collectorPtniiName + "] found in AAI");
				return isValid;
			}
			List<Vertex> collectorPnfList = g.V().has(this.PROPERTY_PNF_NAME, collectorPtniiName).has(AAIProperties.NODE_TYPE, PNF_NODE_TYPE).toList();
			if (collectorPnfList != null && collectorPnfList.size() == 1) {
				isValid = true;
				pnfList.put(collectorPtniiName, collectorPnfList.get(0));
				logger.info("\t Pnf [" + collectorPtniiName + "] found in AAI");
			} else if (collectorPnfList == null || collectorPnfList.size() == 0) {
				logger.info("\t ERROR: Failure to find Pnf [" + collectorPtniiName	+ "] for EVC ["	+ evcName + "]");
			}
		}
		return isValid;
	}
	
	private boolean validateBearerPnf(String bearerPtniiName, String evcName) {
		boolean isValid = false;
		if (!AAIConfig.isEmpty(bearerPtniiName)) {
			if (!pnfList.isEmpty() && pnfList.containsKey(bearerPtniiName)){
				isValid = true;
				logger.info("\t Pnf [" + bearerPtniiName + "] found in AAI");
				return isValid;
			}
			List<Vertex> bearerPnfList = g.V().has(this.PROPERTY_PNF_NAME, bearerPtniiName).has(AAIProperties.NODE_TYPE, PNF_NODE_TYPE).toList();
	    	if (bearerPnfList!= null && bearerPnfList.size() == 1){
	            isValid = true;
	            pnfList.put(bearerPtniiName, bearerPnfList.get(0));
	            logger.info("\t Pnf ["+ bearerPtniiName + "] found in AAI");
	        }
	        else if (bearerPnfList == null || bearerPnfList.size() == 0) {
	        	logger.info("\t ERROR:  Failure to find Pnf ["+ bearerPtniiName + "]  for EVC [" + evcName + "]");
	        }	
	    }
    	return isValid;
	}
	
	private boolean validateCollectorPort(Map<String, String> pathColValues, String collectorPortAid, String collectorPtniiName, String evcName) {
		boolean isValid = false;
		
		if (!AAIConfig.isEmpty(collectorPortAid)) {
			
			boolean isPortAidALagIntf = false;
			GraphTraversal<Vertex, Vertex> collectorPortList;
			String lagInterface = null;
			
			int lagIdentifierIndex = collectorPortAid.indexOf("_");
			
			if (lagIdentifierIndex > 0) {
				String[] subStringList = collectorPortAid.split("_");
				lagInterface = subStringList[0]; //forwarder will be related to this lagInterface
				isPortAidALagIntf = true;
			}
			
			if (isPortAidALagIntf)
			{
				if (!portList.isEmpty() && portList.containsKey(collectorPtniiName+"_"+lagInterface)){
					isValid = true;
					logger.info("\t lag-interface [" + lagInterface	+ "] found in AAI");
					populatePathFileMapWithForwarderInfo(collectorPtniiName, evcName, lagInterface, portList.get(collectorPtniiName+"_"+lagInterface));
					return isValid;
				}
				Vertex collectorPnfVtx  = pnfList.get(collectorPtniiName);
				if (collectorPnfVtx == null ) {
					logger.info("\t ERROR: Failure to find lag-interface ["+ lagInterface + "] for EVC [" + evcName + "]");
					return isValid;
				} else {
					collectorPortList = g.V(collectorPnfVtx).in("tosca.relationships.network.BindsTo").has("interface-name", lagInterface).has("aai-node-type", "lag-interface");
					 
			    	if (collectorPortList!= null && collectorPortList.hasNext()) {
			    		Vertex lagInterfaceVtx = collectorPortList.next();
			            if (lagInterfaceVtx != null && lagInterfaceVtx.property("interface-name").isPresent()) {
							isValid = true;
							portList.put(collectorPtniiName+"_"+lagInterface, lagInterfaceVtx);
							populatePathFileMapWithForwarderInfo(collectorPtniiName, evcName, lagInterface,	lagInterfaceVtx);
							logger.info("\t lag-interface [" + lagInterface
									+ "] found in AAI");
						}
			        }
			        else if (collectorPortList == null || !collectorPortList.hasNext()) {
			        	logger.info("\t ERROR: Failure to find lag-interface ["+ lagInterface + "] for EVC [" + evcName + "]");
			        }
				}
			} 
			else if (!isPortAidALagIntf)
			{
				if (!portList.isEmpty() && portList.containsKey(collectorPtniiName+"_"+collectorPortAid)){
					isValid = true;
					logger.info("\t p-interface [" + collectorPortAid + "] found in AAI");
					populatePathFileMapWithForwarderInfo(collectorPtniiName, evcName, collectorPortAid, portList.get(collectorPtniiName+"_"+collectorPortAid));
					return isValid;
				}
				
				Vertex collectorPnfVtx  = pnfList.get(collectorPtniiName);
				if (collectorPnfVtx == null ) {
					logger.info("\t ERROR: Failure to find p-interface ["+ collectorPortAid + "] for EVC [" + evcName + "]");
					return isValid;
				} else {
					collectorPortList =g.V(collectorPnfVtx).in("tosca.relationships.network.BindsTo").has("interface-name", collectorPortAid).has("aai-node-type", "p-interface");
							
			    	if (collectorPortList!= null && collectorPortList.hasNext()) {
			    		Vertex pInterfaceVtx = collectorPortList.next();
			    		if (pInterfaceVtx != null && pInterfaceVtx.property("interface-name").isPresent()) {
							isValid = true;
							portList.put(collectorPtniiName+"_"+collectorPortAid, pInterfaceVtx );
							populatePathFileMapWithForwarderInfo(collectorPtniiName, evcName, collectorPortAid, pInterfaceVtx);
							logger.info("\t p-interface [" + collectorPortAid
									+ "] found in AAI");
						}
			        }
			        else if (collectorPortList == null || !collectorPortList.hasNext()) {
			        	logger.info("\t ERROR: Failure to find p-interface ["+ collectorPortAid + "] for EVC [" + evcName + "]");
			        }
				}
			}
		}
    	return isValid;
	}

	private boolean validateBearerPort(Map<String, String> pathColValues, String bearerPortAid, String bearerPtniiName, String evcName) {
		boolean isValid = false;
		
		if (!AAIConfig.isEmpty(bearerPortAid)) {
			GraphTraversal<Vertex, Vertex> bearerPortList;
			
			boolean isPortAidALagIntf = false;
			GraphTraversal<Vertex, Vertex> collectorPortList;
			String lagInterface = null;
			
			int lagIdentifierIndex = bearerPortAid.indexOf("_");
			
			if (lagIdentifierIndex > 0) {
				String[] subStringList = bearerPortAid.split("_");
				lagInterface = subStringList[0]; //forwarder will be related to this lagInterface
				isPortAidALagIntf = true;
			}
			
			if (isPortAidALagIntf)
			{
				if (!portList.isEmpty() && portList.containsKey(bearerPtniiName+"_"+lagInterface)){
					isValid = true;
					logger.info("\t lag-interface [" + lagInterface	+ "] found in AAI");
					populatePathFileMapWithForwarderInfo(bearerPtniiName, evcName, lagInterface, portList.get(bearerPtniiName+"_"+lagInterface));
					return isValid;
				}
				Vertex bearerPnfVtx  = pnfList.get(bearerPtniiName);
				if (bearerPnfVtx == null ) {
					logger.info("\t ERROR: Failure to find lag-interface ["+ lagInterface + "] for EVC [" + evcName + "]");
					return isValid;
				} else {
					GraphTraversal<Vertex, Vertex> lagPortList = g.V(bearerPnfVtx).in("tosca.relationships.network.BindsTo").has("interface-name", lagInterface).has("aai-node-type", "lag-interface");
			    	if (lagPortList!= null && lagPortList.hasNext()) {
			    		Vertex lagInterfaceVtx = lagPortList.next();
			    		if (lagInterfaceVtx != null && lagInterfaceVtx.property("interface-name").isPresent()) {
							isValid = true;
							portList.put(bearerPtniiName+"_"+lagInterface, lagInterfaceVtx);
							populatePathFileMapWithForwarderInfo(bearerPtniiName, evcName, lagInterface, lagInterfaceVtx);
							logger.info("\t lag-interface [" + lagInterface
									+ "] found in AAI");
						}
			        }
			        else if (lagPortList == null || !lagPortList.hasNext()) {
			        	logger.info("\t ERROR: Failure to find lag-interface ["+ lagInterface + "] for EVC [" + evcName + "]");
			        }
				}
			} 
			else if (!isPortAidALagIntf) {
				if (!portList.isEmpty() && portList.containsKey(bearerPtniiName+"_"+bearerPortAid)){
					isValid = true;
					logger.info("\t p-interface [" + bearerPortAid + "] found in AAI");
					populatePathFileMapWithForwarderInfo(bearerPtniiName, evcName, bearerPortAid, portList.get(bearerPtniiName+"_"+bearerPortAid));
					return isValid;
				}
				Vertex bearerPnfVtx  = pnfList.get(bearerPtniiName);
				if (bearerPnfVtx == null ) {
					logger.info("\t ERROR: Failure to find p-interface ["+ bearerPortAid + "] for EVC [" + evcName + "]");
					return isValid;
				} else {
					bearerPortList = g.V(bearerPnfVtx).in("tosca.relationships.network.BindsTo").has("interface-name", bearerPortAid).has("aai-node-type","p-interface");
					if (bearerPortList!= null && bearerPortList.hasNext()){
						Vertex pInterfaceVtx = bearerPortList.next();
			    		if (pInterfaceVtx != null && pInterfaceVtx.property("interface-name").isPresent()) {
							isValid = true;
							portList.put(bearerPtniiName+"_"+bearerPortAid, pInterfaceVtx);
							populatePathFileMapWithForwarderInfo(bearerPtniiName, evcName, bearerPortAid, pInterfaceVtx);
							logger.info("\t p-interface [" + bearerPortAid
									+ "] found in AAI");
						}
					}
					else if (bearerPortList == null || !bearerPortList.hasNext()) {
						logger.info("\t ERROR: Failure to find p-interface ["+ bearerPortAid + "] for evc [" + evcName + "]");
					}
				}
			}
		}
    	return isValid;
	}

	private void populatePathFileMapWithForwarderInfo(String ptniiName, String evcName, String lagInterface, Vertex interfaceVtx) {
		int size = 0;
		Map<Vertex, String> interfaceMap = new HashMap<Vertex, String>();
		interfaceMap = pathFileMap.get(evcName);
		if (interfaceMap != null && !interfaceMap.isEmpty()) {
			size = interfaceMap.size();
		}
		String sequence = Integer.toString(size + 1);
		if (interfaceMap != null && size > 0){
			interfaceMap.put(interfaceVtx, sequence +"_"+ ptniiName+"_"+lagInterface);
		} else{
			interfaceMap = new HashMap<Vertex, String>();
			interfaceMap.put(interfaceVtx, sequence +"_"+ptniiName+"_"+lagInterface );
		}
		pathFileMap.put(evcName, interfaceMap);
	}

    private void createNewForwardersFromPATHData(Map<String, String> pathColValues, String evcName, int fileLineCounter) {
    	Map<Vertex, String> forwarderMap = pathFileMap.get(evcName);
    	List<Vertex> forwardingPathVtxList  = g.V().has(this.FORWARDING_PATH_ID, evcName).has(AAIProperties.NODE_TYPE, FORWARDING_PATH_NODE_TYPE).toList();
    	if (forwardingPathVtxList != null && !forwardingPathVtxList.isEmpty())  {
    		Vertex forwardingPathVtx = forwardingPathVtxList.get(0);
			if (forwarderMap != null && !forwarderMap.isEmpty()) {
				//for each forwarder, create the new forwarder object
				forwarderMap.forEach((portVtx, port) -> {

					Vertex forwarderVtx = createForwarderObject(evcName, portVtx, port, forwardingPathVtx);
					if (forwarderVtx != null) {
						String forwarderRole = forwarderVtx.value("forwarder-role").toString();
						Vertex configurationVtx = createConfigurationObject(evcName, portVtx, port, forwarderVtx);
						createForwarderEvcObject(pathColValues, forwarderRole, portVtx, port,
								configurationVtx);
					}
				});
			}
		} else {
			falloutEvcsList.put((fileLineCounter + 1), "["+ evcName +"] Forwarding-path does not exist for EVC");
			falloutEvcsCount++;
			//Reduce the count of processed evcs since this EVC row cannot be processed
			processedEvcsCount--;
			logger.info("\t ERROR: Forwarding-path does not exist for EVC [" + evcName + "] skipping processing for this EVC.");
		}
	}

	private Vertex createForwarderObject(String evcName, Vertex intfVertex, String port, Vertex forwardingPathVtx) {
    	Vertex forwarderVtx = null;

		try {
			//check if the forwarder was already created
			List<Vertex> forwardersList = g.V(forwardingPathVtx.id()).in("org.onap.relationships.inventory.BelongsTo").toList();
        	Iterator<Vertex> forwardersItr = forwardersList.iterator();
        	while (forwardersItr.hasNext()){
        		Vertex existingForwarderVtx = forwardersItr.next();
        		Vertex existingIntfVtx = g.V(existingForwarderVtx).out("org.onap.relationships.inventory.ForwardsTo").toList().get(0);
        		if( existingIntfVtx.id().equals(intfVertex.id())) {
        			//this forwarder has already been created from the forwarderMap
        			return null;
        		}
        	}
			Integer sequence = getSequenceFromPathMapPort(port);
			String role = getForwarderRole(port);
			
			Introspector forwarder = loader.introspectorFromName("forwarder");
			forwarderVtx = serializer.createNewVertex(forwarder);
			
			if (sequence != null && role != null) {
				forwarder.setValue("sequence", sequence);
				forwarder.setValue("forwarder-role", role );
				
				//Create tree edge from forwarding-path
				this.createTreeEdge(forwardingPathVtx, forwarderVtx);
				//Create cousin edge to p-interface or lag-interface
				this.createCousinEdge(intfVertex, forwarderVtx);
				
				serializer.serializeSingleVertex(forwarderVtx, forwarder, "migrations");
				
//				String forwarderVtxProps = this.asString(forwarderVtx);
//				logger.info(" forwarderVtxProps:" + forwarderVtxProps);
				
				String forwarderVtxSequence = forwarderVtx.value("sequence").toString() ;
				String forwarderVtxRole = forwarderVtx.value("forwarder-role").toString();
				String forwardingPathId = forwardingPathVtx.value("forwarding-path-id").toString();
				
				logger.info("\t Created new forwarder " + forwarderVtx + " with sequence = " + forwarderVtxSequence + " with role [" + forwarderVtxRole 
						+"] as a child of forwarding-path [" + forwardingPathId + "]" );
				
				String dmaapMsg = System.nanoTime() + "_" + forwarderVtx.id().toString() + "_"	+ forwarderVtx.value("resource-version").toString();
				dmaapMsgList.add(dmaapMsg);
				
//				Introspector forwarderIntrospector = serializer.getLatestVersionView(forwarderVtx);
//				this.notificationHelper.addEvent(forwarderVtx, forwarderIntrospector, EventAction.CREATE, this.serializer
//						.getURIForVertex(forwarderVtx, false));
//				logger.info("\t Dmaap event sent for " + forwarderVtx + " for port ["+intfVertex.toString() + "] with sequence = [" + sequence + "] and role [" + role +"]" );
			}
		} catch (Exception e) {
			logger.info("\t ERROR: Failure to PUT forwarder for EVC [" + evcName + "]" );
		}
		return forwarderVtx;
	}
	
	private Integer getSequenceFromPathMapPort(String port) {
		String[] subStringList = port.split("_");
		String sequenceStr = subStringList[0]; //forwarder will be have this sequence
		if (sequenceStr != null && !sequenceStr.isEmpty()) {
			return Integer.parseInt(sequenceStr);
		} else {
			return null;
		}
		
	}

	private Vertex createConfigurationObject(String evcName, Vertex portVtx, String port, Vertex forwarderVtx) {
		Vertex configurationVtx = null;
		String configurationId = null;
		try {
			Introspector configuration = loader.introspectorFromName(CONFIGURATION_NODE_TYPE);
			
			configurationVtx = serializer.createNewVertex(configuration);
			String sequence = forwarderVtx.value("sequence").toString();
			configurationId = evcName + "-" + sequence;
			configuration.setValue("configuration-id", configurationId);
			configuration.setValue("configuration-type", "forwarder");
			configuration.setValue("configuration-sub-type", "forwarder");
			this.createCousinEdge(forwarderVtx, configurationVtx);
			serializer.serializeSingleVertex(configurationVtx, configuration, "migrations");
			
			logger.info("\t Created new configuration for forwarder " + configurationVtx + " with configuration-id= " + configurationVtx.value("configuration-id").toString() );
			
			String dmaapMsg = System.nanoTime() + "_" + configurationVtx.id().toString() + "_"	+ configurationVtx.value("resource-version").toString();
			dmaapMsgList.add(dmaapMsg);
//			Introspector introspector = serializer.getLatestVersionView(configurationVtx);
//			this.notificationHelper.addEvent(configurationVtx, introspector, EventAction.CREATE, this.serializer.getURIForVertex(configurationVtx, false));
//			logger.info("\t Dmaap event sent for " + configurationVtx + " with configuration-id = " + configurationVtx.value("configuration-id").toString() );
			
			return configurationVtx;
		} catch (Exception e) {
			logger.info("\t ERROR: Failure to PUT Configuration for forwarder [" + configurationId + "]" );
		}
		return configurationVtx;
	}

	private Vertex createForwarderEvcObject(Map<String, String> pathColValues, String forwarderRole, Vertex portVtx, String port, Vertex configurationVtx) {
		Vertex forwarderEvcVtx = null;
		String configurationId = null;
		try {
			Introspector forwarderEvc = loader.introspectorFromName(FORWARDER_EVC_NODE_TYPE);
			forwarderEvcVtx = serializer.createNewVertex(forwarderEvc);
			configurationId = configurationVtx.value(this.PROPERTY_CONFIGURATION_ID).toString();
			
			String collectorFacingCircuit = pathColValues.get("collectorFacingCircuit");
			String bearerFacingCircuit = pathColValues.get("bearerFacingCircuit");
			String collectorCvlan = pathColValues.get("collectorCvlan");
			String bearerCvlan = pathColValues.get("bearerCvlan");
			String collectorSvlan = pathColValues.get("collectorSvlan");
			String bearerSvlan = pathColValues.get("bearerSvlan");
			
			forwarderEvc.setValue("forwarder-evc-id", configurationId);
			
			//Don't set circuitid for forwarder-evc connected to configuration that's connected to intermediate forwarder.
			if ("ingress".equalsIgnoreCase(forwarderRole)){
				forwarderEvc.setValue("circuit-id", checkForNull(collectorFacingCircuit));
				if (collectorCvlan != null && !collectorCvlan.isEmpty()) {
					forwarderEvc.setValue("cvlan", collectorCvlan);
				}
				if (collectorSvlan != null && !collectorSvlan.isEmpty()) {
					forwarderEvc.setValue("svlan", collectorSvlan);
				}
			} else if ("egress".equalsIgnoreCase(forwarderRole)){
				forwarderEvc.setValue("circuit-id", bearerFacingCircuit);
				if (bearerCvlan != null && !bearerCvlan.isEmpty()) {
					forwarderEvc.setValue("cvlan", bearerCvlan);
				}
				if (bearerSvlan != null && !bearerSvlan.isEmpty()) {
					forwarderEvc.setValue("svlan", bearerSvlan);
				}
			} else {
				int lastIndex = configurationId.lastIndexOf("-");
				String sequenceStr = configurationId.substring(lastIndex);
				int i = Integer.parseInt(sequenceStr);
				if (i%2 == 0){
					forwarderEvc.setValue("cvlan", checkForNull(bearerCvlan));
					forwarderEvc.setValue("svlan", checkForNull(bearerSvlan));
				} else {
					forwarderEvc.setValue("cvlan", checkForNull(collectorCvlan));
					forwarderEvc.setValue("svlan", checkForNull(collectorSvlan));
				}
			}
			this.createTreeEdge(configurationVtx, forwarderEvcVtx);
			serializer.serializeSingleVertex(forwarderEvcVtx, forwarderEvc, "migrations");
			
			logger.info("\t Created new forwarder-evc as a child of configuration " + forwarderEvcVtx + " with forwarder-evc-id= " + forwarderEvcVtx.value("forwarder-evc-id").toString() );
			String dmaapMsg = System.nanoTime() + "_" + forwarderEvcVtx.id().toString() + "_"	+ forwarderEvcVtx.value("resource-version").toString();
			dmaapMsgList.add(dmaapMsg);
			
//			logger.info("\t Forwarder-evc: "+ this.asString(forwarderEvcVtx));
			
//			Introspector introspector = serializer.getLatestVersionView(forwarderEvcVtx);
//			this.notificationHelper.addEvent(forwarderEvcVtx, introspector, EventAction.CREATE, this.serializer.getURIForVertex(forwarderEvcVtx, false));
//			logger.info("\t Dmaap event sent for " + forwarderEvcVtx + " with forwarder-evc-id = " + forwarderEvcVtx.value("forwarder-evc-id").toString() );
		}  catch (Exception e) {
			logger.info("\t ERROR: Failure to PUT fowarder-evc for configuration [" + configurationId + "]" );
		}
		return forwarderEvcVtx;

	}

	private String getForwarderRole( String port) {
		String role = null;
		Integer seq = getSequenceFromPathMapPort(port);
		if (seq != null ) {
			int sequence = seq.intValue();
			if (sequence == 1){
				role = "ingress";
			} else if (sequence > 1 && port.indexOf(".") > 0) {
				role = "egress";
			} else {
				role = "intermediate";
			}
		}
		return role;
	}
	
	private String checkForNull(String s){
    	if (s!= null && !s.isEmpty()){
    		return s;
    	}
    	return null;
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
        return Optional.of(new String[]{this.FORWARDING_PATH_NODE_TYPE});
    }

    @Override
    public String getMigrationName() {
        return "MigratePATHEvcInventory";
    }
}
