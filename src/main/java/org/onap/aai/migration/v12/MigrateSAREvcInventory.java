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
import java.util.Map;
import java.util.Optional;
import java.util.List;

import org.apache.tinkerpop.gremlin.process.traversal.dsl.graph.GraphTraversal;
import org.apache.tinkerpop.gremlin.process.traversal.dsl.graph.GraphTraversalSource;
import org.apache.tinkerpop.gremlin.structure.Vertex;
import org.onap.aai.edges.EdgeIngestor;
import org.onap.aai.introspection.Introspector;
import org.onap.aai.introspection.LoaderFactory;
import org.onap.aai.migration.*;
import org.onap.aai.db.props.AAIProperties;
import org.onap.aai.serialization.db.EdgeSerializer;
import org.onap.aai.serialization.engines.TransactionalGraphEngine;
import org.onap.aai.setup.SchemaVersions;
import org.onap.aai.util.AAIConfig;


@MigrationPriority(27)
@MigrationDangerRating(100)
public class MigrateSAREvcInventory extends Migrator {

	private static Map<String, Vertex> pnfList = new HashMap<String, Vertex>();
	private static List<String> portList = new ArrayList<String>();;
	private final String SAREA_GLOBAL_CUSTOMER_ID = "8a00890a-e6ae-446b-9dbe-b828dbeb38bd";
	private final String CONFIGURATION_NODE_TYPE = "configuration";
	private final String SERVICE_INSTANCE_NODE_TYPE = "service-instance";
	private final String SERVICE_SUBSCRIPTON_NODE_TYPE = "service-subscription";
	private final String PROPERTY_SERVICE_TYPE = "service-type";
	private final String SERVICE_INSTANCE_ID = "service-instance-id";
	private final String FORWARDING_PATH_NODE_TYPE = "forwarding-path";
	private final String FOWARDING_PATH_ID = "forwarding-path-id";
	private final String EVC_NODE_TYPE = "evc";
	private final String PROPERTY_CONFIGURATION_ID = "configuration-id";
	private final String PNF_NODE_TYPE = "pnf";
	private final String PROPERTY_PNF_NAME = "pnf-name";
	private final String PROPERTY_INTERFACE_NAME = "interface-name";
	private final String PINTERFACE_NODE_TYPE = "p-interface";
	private static boolean success = true;
    private static boolean checkLog = false;
    private static GraphTraversalSource g = null;
    private int headerLength;
  
    private static int processedEvcsCount = 0;
    private static int falloutEvcsCount = 0;
    private static Map<String, String> falloutEvcsMap = new HashMap<String, String>();
    
    private static final String homeDir = System.getProperty("AJSC_HOME");
	private static List<String> dmaapMsgList = new ArrayList<String>();
	
    public MigrateSAREvcInventory(TransactionalGraphEngine engine, LoaderFactory loaderFactory, EdgeIngestor edgeIngestor, EdgeSerializer edgeSerializer, SchemaVersions schemaVersions) {
        super(engine, loaderFactory, edgeIngestor, edgeSerializer, schemaVersions);
        this.g = this.engine.asAdmin().getTraversalSource();
    }

    @Override
    public void run() {
        logger.info("---------- Start migration of SAR EVC Inventory  ----------");
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
        String fileName = feedDir+ "sar.csv";
        logger.info(fileName);
        logger.info("---------- Processing SAR Entries from file  ----------");
        
        try  {
            String line;
            
            List<String> lines = Files.readAllLines(Paths.get(fileName));
            Iterator<String> lineItr = lines.iterator();
            while (lineItr.hasNext()){
            	line = lineItr.next().replace("\n", "").replace("\r", "");
                logger.info("\n");
                if (!line.isEmpty()) {
                    if (fileLineCounter != 0) {
                        String[] colList = line.split("\\s*,\\s*", -1);
//                        if (colList.length != headerLength) {
//                            logger.info("ERROR: SAR line should contain " + headerLength + " columns, contains " + colList.length + " instead.");
//                            success = false;
//                            continue;
//                        }
                        Map<String, String> sarColValues = new HashMap<String, String>();
                        sarColValues.put("evcName", colList[0]);
                        sarColValues.put("subscriberName", colList[1]);
                        sarColValues.put("espName", colList[2]);
                        sarColValues.put("bearerCircuitId", colList[3]);
                    	sarColValues.put("bearerTagMode", colList[4]);
                    	sarColValues.put("bearerCvlan", colList[5]);
                    	sarColValues.put("bearerSvlan", colList[6]);
                    	sarColValues.put("bearerPtniiName", colList[7]);
                    	sarColValues.put("bearerSlotName", colList[8]);
                    	String bearerPortAid = colList[9].replaceAll("^\"|\"$", "").replaceAll("\\s+","");
                    	sarColValues.put("bearerPortAid", bearerPortAid);
                    	sarColValues.put("bearerPortType", colList[10]);
                    	sarColValues.put("collectorCircuitId", colList[11]);
                    	sarColValues.put("collectorTagMode", colList[12]);
                    	sarColValues.put("collectorCvlan", colList[13]);
                    	sarColValues.put("collectorSvlan", colList[14]);
                    	sarColValues.put("collectorPtniiName", colList[15]);
                    	sarColValues.put("collectorSlotName", colList[16]);
                    	String collectorPortAid = colList[17].replaceAll("^\"|\"$", "").replaceAll("\\s+","");
                    	sarColValues.put("collectorPortAid", collectorPortAid);
                    	sarColValues.put("collectorPortType", colList[18]);
                    	sarColValues.put("espEvcCircuitId", colList[19]);
                    	sarColValues.put("evcAccessCIR", colList[20]);
                    	
                    	String evcName = sarColValues.get("evcName");
                        if (!AAIConfig.isEmpty(evcName)) {
                        	logger.info("---------- Processing Line " + line + "----------");
                            logger.info("\t Evc Name = " + evcName );
                            
                            boolean isEntryValid = validatePnfsAndPorts(sarColValues, evcName);
                            
                            if (!isEntryValid){
                            	logger.info("\t ERROR: Skipping processing for line containing evc-name [" +evcName+ "]");
                            	falloutEvcsCount++;
                            	falloutEvcsMap.put((fileLineCounter+1)+"", "["+evcName+"] - PortAid/Pnf does not exist" );
                            	fileLineCounter++;
                            	continue;
                            }
                            
                            createNewObjectsFromSARFile(sarColValues, evcName, fileLineCounter);
                        	
                        }
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
            
            logger.info ("\n \n ******* Final Summary for SAR FILE Migration ********* \n");
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
            e.printStackTrace();
            success = false;
        }
    }
    
    
    private boolean validatePnfsAndPorts(Map<String, String> sarColValues, String evcName) {
    	
    	String collectorPtniiName = sarColValues.get("collectorPtniiName");
    	String bearerPtniiName = sarColValues.get("bearerPtniiName");
    	String collectorPortAid = sarColValues.get("collectorPortAid");
    	String bearerPortAid = sarColValues.get("bearerPortAid");
		boolean isValid = validateCollectorPnf(collectorPtniiName, evcName) && validateBearerPnf(bearerPtniiName, evcName) 
				&& validateCollectorPort(collectorPortAid, collectorPtniiName, evcName) 
				&& validateBearerPort(bearerPortAid, bearerPtniiName, evcName) ;
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
	
	private boolean validateCollectorPort(String collectorPortAid, String collectorPtniiName, String evcName) {
		boolean isValid = false;
		if (!AAIConfig.isEmpty(collectorPortAid)) {
			if (!portList.isEmpty() && portList.contains(collectorPtniiName+"_"+collectorPortAid)){
				isValid = true;
				logger.info("\t Port ["+ collectorPortAid + "] found in AAI");
				return isValid;
			}
			GraphTraversal<Vertex, Vertex> collectorPortList;
			Vertex collectorPnfVtx  = pnfList.get(collectorPtniiName);
			if (collectorPnfVtx == null ) {
				logger.info("\t ERROR: Failure to find p-interface ["+ collectorPortAid + "] for EVC [" + evcName + "]");
				return isValid;
			} else {
				collectorPortList =g.V(collectorPnfVtx).in("tosca.relationships.network.BindsTo").has("interface-name", collectorPortAid).has("aai-node-type", "p-interface");
		    	if (collectorPortList!= null && collectorPortList.hasNext()) {
		            isValid = true;
		            portList.add(collectorPtniiName+"_"+collectorPortAid);
		            logger.info("\t Port ["+ collectorPortAid + "] found in AAI");
		        }
		        else if (collectorPortList == null || !collectorPortList.hasNext()) {
		        	logger.info("\t ERROR: Failure to find p-interface ["+ collectorPortAid + "] for EVC [" + evcName + "]");
		        }
			}
		}
    	return isValid;
	}
	
	private boolean validateBearerPort(String bearerPortAid, String bearerPtniiName, String evcName) {
		boolean isValid = false;
		
		if (!AAIConfig.isEmpty(bearerPortAid)) {
			if (!portList.isEmpty() && portList.contains(bearerPtniiName+"_"+bearerPortAid)){
				isValid = true;
				logger.info("\t Port ["+ bearerPortAid + "] found in AAI");
				return isValid;
			}
			GraphTraversal<Vertex, Vertex> bearerPortList;
			Vertex bearerPnfVtx  = pnfList.get(bearerPtniiName);
			if (bearerPnfVtx == null ) {
				logger.info("\t ERROR: Failure to find p-interface ["+ bearerPortAid + "] for EVC [" + evcName + "]");
				return isValid;
			} else {
				bearerPortList =g.V(bearerPnfVtx).in("tosca.relationships.network.BindsTo").has("interface-name", bearerPortAid).has("aai-node-type", "p-interface");
				if (bearerPortList!= null && bearerPortList.hasNext()){
				    isValid = true;
				    portList.add(bearerPtniiName+"_"+bearerPortAid);
				    logger.info("\t Port ["+ bearerPortAid + "] found in AAI");
				}
				else if (bearerPortList == null || !bearerPortList.hasNext()) {
					logger.info("\t ERROR: Failure to find p-interface ["+ bearerPortAid + "] for evc [" + evcName + "]");
				}
			}
		}
    	return isValid;
	}

	private void createNewObjectsFromSARFile(Map<String, String> sarColValues, String evcName, int lineNumber) {
    	Vertex serviceInstanceVtx = createNewServiceInstanceFromSARData(sarColValues, evcName, lineNumber);
    	if (serviceInstanceVtx != null && serviceInstanceVtx.property("service-instance-id").isPresent()) {
	    	Vertex forwardingPathVtx = createNewForwardingPathFromSARData(sarColValues, serviceInstanceVtx, lineNumber);
	    	Vertex configurationVtx = createNewConfigurationFromSARData(sarColValues, forwardingPathVtx, lineNumber);
	    	Vertex evcVtx = createNewEvcFromSARData(sarColValues, configurationVtx, lineNumber);
		}
	}

    private Vertex createNewServiceInstanceFromSARData(Map<String, String> sarColValues, String evcName, int lineNumber) {
    	
    	String serviceType = "SAREA";
    	Vertex serviceInstanceVtx = null;

		try {
			 
			GraphTraversal<Vertex, Vertex> servSubVtxList = g.V().has("global-customer-id", SAREA_GLOBAL_CUSTOMER_ID)
					.in("org.onap.relationships.inventory.BelongsTo").has("service-type", "SAREA");
			
			 if (servSubVtxList!= null && servSubVtxList.hasNext()){	
				 Vertex serviceSubscriptionVtx = servSubVtxList.next();
				if (serviceSubscriptionVtx != null ) {
					
					List<Vertex> existingServInstVtxList = g.V(serviceSubscriptionVtx).in("org.onap.relationships.inventory.BelongsTo").has("aai-node-type", "service-instance")
               				.has("service-instance-id",evcName).toList();
                   	
                    if (existingServInstVtxList!= null && existingServInstVtxList.size() >0){
                        logger.info("\t service-instance already exists for evc " + evcName + " - skipping");
                        
                        return existingServInstVtxList.iterator().next();
                    }
                    else if (existingServInstVtxList!= null && existingServInstVtxList.size() == 0) {
                    	Introspector servInstance = loader.introspectorFromName("service-instance");
    					serviceInstanceVtx = serializer.createNewVertex(servInstance);
    					String serviceInstanceId = (String) sarColValues.get("evcName");
    					servInstance.setValue("service-instance-id", serviceInstanceId);
    					servInstance.setValue("service-type", serviceType);
    					this.createTreeEdge(serviceSubscriptionVtx, serviceInstanceVtx);
    					serializer.serializeSingleVertex(serviceInstanceVtx, servInstance, "migrations");
    					
    					logger.info("\t Created new service-instance " + serviceInstanceVtx + " with service-instance-id = " + serviceInstanceId );
    					
    					String dmaapMsg = System.nanoTime() + "_" + serviceInstanceVtx.id().toString() + "_"	+ serviceInstanceVtx.value("resource-version").toString();
    					dmaapMsgList.add(dmaapMsg);
    					processedEvcsCount++;
                    }
                    else {
                        logger.info("\t ERROR: More than one service-instance found for evc-name: " + evcName);
                    }
				}
			} else {
				logger.info("\t ERROR: SAREA Subscription not found for Customer ["+SAREA_GLOBAL_CUSTOMER_ID+"]");
				falloutEvcsCount++;
				falloutEvcsMap.put((lineNumber+1)+"", "["+evcName+"] - SAREA Subscription not found for Customer ["+SAREA_GLOBAL_CUSTOMER_ID+"]" );
			}
		} catch (Exception e) {
			logger.info("\t ERROR: Failure to PUT service-instance for EVC [" + evcName + "]" );
			falloutEvcsCount++;
			falloutEvcsMap.put((lineNumber+1)+"", "["+evcName+"] - Failure to PUT service-instance for EVC" );
		}
		return serviceInstanceVtx;

	}
    
    private Vertex createNewForwardingPathFromSARData(Map<String, String> sarColValues, Vertex serviceInstanceVtx, int lineNumber) {
		Vertex fpVertex = null;
		String serviceInstanceId = serviceInstanceVtx.value(this.SERVICE_INSTANCE_ID);
		
		try {
			
			List<Vertex> fpList = g.V(serviceInstanceVtx).in("org.onap.relationships.inventory.AppliesTo").has("aai-node-type","forwarding-path")
					.has("forwarding-path-id", serviceInstanceId).toList();
			if (fpList != null && !fpList.isEmpty()){
				logger.info("\t forwarding-path already exists for evc " + serviceInstanceId + " - skipping");
				return fpList.iterator().next();
			} 
			
			//If forwarding-path does not exist, create it
			Introspector fpIntrospector = loader.introspectorFromName(FORWARDING_PATH_NODE_TYPE);
			fpVertex = serializer.createNewVertex(fpIntrospector);
			
			fpIntrospector.setValue("forwarding-path-id", serviceInstanceId);
			fpIntrospector.setValue("forwarding-path-name", serviceInstanceId);
			this.createCousinEdge(fpVertex, serviceInstanceVtx);
			serializer.serializeSingleVertex(fpVertex, fpIntrospector, "migrations");

			logger.info("\t Created new forwarding-path " + fpVertex + " with forwarding-path-id = " + fpVertex.value("forwarding-path-id").toString() );
			String dmaapMsg = System.nanoTime() + "_" + fpVertex.id().toString() + "_"	+ fpVertex.value("resource-version").toString();
			dmaapMsgList.add(dmaapMsg);

		} catch (Exception e) {
			logger.info("\t ERROR: Failure to PUT forwarding-path for EVC [" + serviceInstanceId + "]" );
			processedEvcsCount--;
			falloutEvcsCount++;
			falloutEvcsMap.put((lineNumber+1)+"", "["+serviceInstanceId+"] - Failure to PUT forwarding-path for EVC" );
		}
		return fpVertex;
	}
    
    private Vertex createNewConfigurationFromSARData(Map<String, String> sarColValues, Vertex forwardingPathVtx, int lineNumber) {
    	
    	Vertex configurationVtx = null;
    	String forwardingPathId = forwardingPathVtx.value(this.FOWARDING_PATH_ID);
    	try {
    		
    		List<Vertex> configList = g.V(forwardingPathVtx).out("org.onap.relationships.inventory.Uses").has("aai-node-type","configuration")
    				.has("configuration-id", forwardingPathId).toList();
    		if (configList != null && !configList.isEmpty()){
    			logger.info("\t configuration already exists for evc " + forwardingPathId + " - skipping");
    			return configList.iterator().next();
    		} 
    		
    		//If configuration does not exist, create it
    		Introspector configuration = loader.introspectorFromName(CONFIGURATION_NODE_TYPE);
			configurationVtx = serializer.createNewVertex(configuration);
			
			configuration.setValue("configuration-id", forwardingPathId);
			configuration.setValue("configuration-type", "forwarding-path");
			configuration.setValue("configuration-sub-type", "evc");
			this.createCousinEdge(forwardingPathVtx, configurationVtx);
			serializer.serializeSingleVertex(configurationVtx, configuration, "migrations");
			
			logger.info("\t Created new configuration for forwarding-path " + configurationVtx + " with configuration-id= " + configurationVtx.value("configuration-id").toString() );
			
			String dmaapMsg = System.nanoTime() + "_" + configurationVtx.id().toString() + "_"	+ configurationVtx.value("resource-version").toString();
			dmaapMsgList.add(dmaapMsg);
			
    	}catch (Exception e) {
			logger.info("\t ERROR: Failure to PUT configuration for EVC [" + forwardingPathId + "]" );
			processedEvcsCount--;
			falloutEvcsCount++;
			falloutEvcsMap.put((lineNumber+1)+"", "["+forwardingPathId+"] - Failure to PUT configuration for EVC" );
		}
		return configurationVtx;
	}
    
    private Vertex createNewEvcFromSARData(Map<String, String> sarColValues, Vertex configurationVtx, int lineNumber) {
    	String evcId = null;
    	Vertex evcVtx = null;
    	try {
	    	Introspector evc = loader.introspectorFromName(EVC_NODE_TYPE);
			evcVtx = serializer.createNewVertex(evc);
			evcId = configurationVtx.value(this.PROPERTY_CONFIGURATION_ID);
			
			String cir = sarColValues.get("evcAccessCIR");
			int length = cir.length();
			String cirValue =  cir.substring(0,(length-4));
			String cirUnits =  cir.substring((length-4), (length));
			
			String espEvcCircuitId = sarColValues.get("espEvcCircuitId");
			String espName = sarColValues.get("espName");
			String collectorTagMode = sarColValues.get("collectorTagMode");
			String bearerTagMode = sarColValues.get("bearerTagMode");
			
			evc.setValue("evc-id", evcId);
			evc.setValue("forwarding-path-topology", "PointToPoint");
			evc.setValue("cir-value", checkForNull(cirValue));
			evc.setValue("cir-units", checkForNull(cirUnits));
			evc.setValue("esp-evc-circuit-id", checkForNull(espEvcCircuitId));
			evc.setValue("esp-evc-cir-value", checkForNull(cirValue));
			evc.setValue("esp-evc-cir-units", checkForNull(cirUnits));
			evc.setValue("esp-itu-code", checkForNull(espName));
			evc.setValue("tagmode-access-ingress", checkForNull(collectorTagMode));
			evc.setValue("tagmode-access-egress", checkForNull(bearerTagMode));
			this.createTreeEdge(configurationVtx, evcVtx);
			serializer.serializeSingleVertex(evcVtx, evc, "migrations");
			
			logger.info("\t Created new evc as a child of configuration " + evcVtx + " with evc-id= " + evcVtx.value("evc-id").toString() );
			String dmaapMsg = System.nanoTime() + "_" + evcVtx.id().toString() + "_"	+ evcVtx.value("resource-version").toString();
			dmaapMsgList.add(dmaapMsg);
			
//			Introspector introspector = serializer.getLatestVersionView(evcVtx);
//			this.notificationHelper.addEvent(evcVtx, introspector, EventAction.CREATE, this.serializer.getURIForVertex(evcVtx, false));
//			logger.info("\t Dmaap event sent for " + evcVtx + " with evc-id = " + evcId);
    	}catch (Exception e) {
			logger.info("\t ERROR: Failure to PUT EVC for evc-name [" + evcId + "]" );
			processedEvcsCount--;
			falloutEvcsCount++;
			falloutEvcsMap.put((lineNumber+1)+"", "["+evcId+"] - Failure to PUT EVC" );
		}
		return evcVtx;

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
        return Optional.of(new String[]{this.SERVICE_INSTANCE_NODE_TYPE});
    }

    @Override
    public String getMigrationName() {
        return "MigrateSAREvcInventory";
    }
}
