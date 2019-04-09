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
package org.onap.aai.migration.v14;

import java.io.FileNotFoundException;
import java.io.IOException;
import java.nio.file.Files;
import java.nio.file.NoSuchFileException;
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
import org.onap.aai.introspection.LoaderFactory;
import org.onap.aai.migration.Enabled;
import org.onap.aai.migration.MigrationDangerRating;
import org.onap.aai.migration.MigrationPriority;
import org.onap.aai.migration.Migrator;
import org.onap.aai.migration.Status;
import org.onap.aai.serialization.db.EdgeSerializer;
import org.onap.aai.serialization.engines.TransactionalGraphEngine;
import org.onap.aai.setup.SchemaVersions;

@MigrationPriority(100)
@MigrationDangerRating(1)
//@Enabled
public class MigrateSdnaIvlanData extends Migrator {
	
	private final String CONFIGURATION_NODE_TYPE = "configuration";
	private final String EVC_NODE_TYPE = "evc";
	private final String FORWARDER_NODE_TYPE = "forwarder";
	private final String FORWRDER_EVC_NODE_TYPE = "forwarder-evc";	
	private final String FORWARDING_PATH_NODE_TYPE = "forwarding-path";
	private final String PNF_NODE_TYPE = "pnf";
	private final String  P_INTERFACE_NODE_TYPE = "p-interface";
	private final String  LAG_INTERFACE_NODE_TYPE = "lag-interface";
	private final String SAREA_GLOBAL_CUSTOMER_ID = "8a00890a-e6ae-446b-9dbe-b828dbeb38bd";
	
	GraphTraversal<Vertex, Vertex> serviceSubscriptionGt;
	
	private static GraphTraversalSource g = null;
	private static boolean success = true;
    private static boolean checkLog = false;
    private int headerLength;
    private int migrationSuccess = 0;
    private int migrationFailure = 0;
    private int invalidPInterfaceCount = 0;
    private int invalidLagInterfaceCount = 0;
    
    
    private static List<String> dmaapMsgList = new ArrayList<String>();
    private static final String homeDir = System.getProperty("AJSC_HOME");
    
    private static List<String> validPnfList = new ArrayList<String>();
    private static List<String> invalidPnfList = new ArrayList<String>();
   
    private static Map<String, List<String>> validInterfaceMap =  new HashMap<String, List<String>>();
    private static Map<String, List<String>> invalidInterfaceMap =  new HashMap<String, List<String>>();
       
    protected class SdnaIvlanFileData{
    	String evcName;
    	String pnfName;
		String interfaceAID;
    	int ivlanValue;
    	
    	public String getEvcName() {
			return evcName;
		}
		public void setEvcName(String evcName) {
			this.evcName = evcName;
		}
		
		public String getPnfName() {
			return pnfName;
		}
		public void setPnfName(String pnfName) {
			this.pnfName = pnfName;
		}
		public String getInterfaceAID() {
			return interfaceAID;
		}
		public void setInterfaceAID(String interfaceAID) {
			this.interfaceAID = interfaceAID;
		}
		
		public int getIvlanValue() {
			return ivlanValue;
		}
		public void setIvlanValue(int ivlanValue) {
			this.ivlanValue = ivlanValue;
		}
		
    }
    
    private static ArrayList<SdnaIvlanFileData> ivlanList = new ArrayList<SdnaIvlanFileData>();
   
	public MigrateSdnaIvlanData(TransactionalGraphEngine engine, LoaderFactory loaderFactory, EdgeIngestor edgeIngestor, EdgeSerializer edgeSerializer, SchemaVersions schemaVersions) {
		super(engine, loaderFactory, edgeIngestor, edgeSerializer, schemaVersions);
		
		this.g = this.engine.asAdmin().getTraversalSource();
		this.serviceSubscriptionGt = g.V().has("global-customer-id", SAREA_GLOBAL_CUSTOMER_ID).in("org.onap.relationships.inventory.BelongsTo").has("service-type", "SAREA");
	}

	@Override
	public void run() {
		logger.info("---------- Start migration ----------");
        String configDir = System.getProperty("BUNDLECONFIG_DIR");
        if (homeDir == null) {
            logger.info(this.MIGRATION_ERROR + "ERROR: Could not find sys prop AJSC_HOME");
            success = false;
            return;
        }
        if (configDir == null) {
            success = false;
            return;
        }
        
        String feedDir = homeDir + "/" + configDir + "/" + "migration-input-files/sarea-inventory/";

        int fileLineCounter = 0;

        String fileName = feedDir+ "ivlanData.csv";
        logger.info(fileName);
        logger.info("---------- Processing Entries from file  ----------");
        
				
        try  {
        	List<String> lines = Files.readAllLines(Paths.get(fileName));
            Iterator<String> lineItr = lines.iterator();
            while (lineItr.hasNext()){
                String line = lineItr.next().trim();
                if (!line.isEmpty()) {
                    if (fileLineCounter != 0) {
                    	
                    	try{
                    		String[] colList = line.split(",", -1);                      
                    		SdnaIvlanFileData lineData = new SdnaIvlanFileData();
                    		lineData.setEvcName(colList[0].trim());
                    		lineData.setPnfName(colList[1].trim());
                    		lineData.setInterfaceAID(colList[2].trim());
                        	lineData.setIvlanValue(Integer.valueOf(colList[3].trim()));
                        	ivlanList.add(lineData);
                    		
                    	} catch (Exception e){
                    		logger.info(this.MIGRATION_ERROR + " ERROR: Record Format is invalid.  Expecting Numeric value for Forwarder_Id and Ivlan_Value.  Skipping Record:  "  + line);
                    		this.migrationFailure++;
                    	}
             	
                    } else {
                        this.headerLength = line.split(",", -1).length;
                        if (this.headerLength < 4){
                            logger.info(this.MIGRATION_ERROR + "ERROR: Input file should have atleast 4 columns");
                            this.success = false;
                            return;
                        }
                    }
                }
                fileLineCounter++;
            }
            
            processSdnaIvlan();
            
            int invalidInterfacesCount = getInvalidInterfaceCount();
            
            logger.info ("\n \n ******* Final Summary for SDN-A IVLAN Migration ********* \n");
            logger.info(this.MIGRATION_SUMMARY_COUNT + "SDN-A forward-evcs: IVLANs updated: "+ migrationSuccess);
            logger.info(this.MIGRATION_SUMMARY_COUNT + "Total File Record Count: "+(fileLineCounter - 1));
            logger.info(this.MIGRATION_SUMMARY_COUNT + "Unprocessed SDNA File Records : "+ migrationFailure);
            logger.info(this.MIGRATION_SUMMARY_COUNT + "PNFs from Input File not found : "+ Integer.toString(invalidPnfList.size()) + "\n");
            
           
            logger.info(this.MIGRATION_SUMMARY_COUNT + "Total PNF + P-INTERFACEs from Input File not found : " + Integer.toString(invalidPInterfaceCount));
            logger.info(this.MIGRATION_SUMMARY_COUNT + "Total PNF + LAG-INTERFACEs from Input File not found : " + Integer.toString(invalidLagInterfaceCount));
            logger.info(this.MIGRATION_SUMMARY_COUNT + "Total PNF/INTERFACEs from Input File not found : " + Integer.toString(invalidInterfacesCount));

        } catch (FileNotFoundException e) {
            logger.info(this.MIGRATION_ERROR + "ERROR: Could not find file " + fileName, e.getMessage());
            success = false;
            checkLog = true;     
        }  catch (NoSuchFileException e) {
            logger.info(this.MIGRATION_ERROR + "ERROR: Could not find file " + fileName, e.getMessage());
            success = false;
            checkLog = true; 
        } catch (IOException e) {
            logger.info(this.MIGRATION_ERROR + "ERROR: Issue reading file " + fileName, e);
            success = false;
        } catch (Exception e) {
            logger.info(this.MIGRATION_ERROR + "encountered exception", e);
            e.printStackTrace();
            success = false;
        }        

	}
	private void processSdnaIvlan() {

		for(int i = 0; i < ivlanList.size(); i ++) {
			String evc = ivlanList.get(i).getEvcName();
			String pnf = ivlanList.get(i).getPnfName();
			String interfaceId = ivlanList.get(i).getInterfaceAID();
			String ivlanValue =  Integer.toString(ivlanList.get(i).getIvlanValue());
			
			Boolean pnfExists = pnfExists(pnf);
			GraphTraversal<Vertex, Vertex> forwarderEvcGT;
			Vertex forwarderEvcVtx = null;
			String interfaceNodeType;
			String forwarderEvcId = null;
			
			if (!pnfExists){
				migrationFailure++;
			}else{
				
				if (interfaceId.contains(".")){
					interfaceNodeType = P_INTERFACE_NODE_TYPE;					
				}else{
					interfaceNodeType = LAG_INTERFACE_NODE_TYPE;					
				}
				
				validateInterface(pnf, interfaceNodeType, interfaceId);

				forwarderEvcGT = g.V()
						.has("pnf-name", pnf).has(AAIProperties.NODE_TYPE, PNF_NODE_TYPE)
						.in("tosca.relationships.network.BindsTo")
						.has(AAIProperties.NODE_TYPE, interfaceNodeType).has("interface-name", interfaceId)
						.in("org.onap.relationships.inventory.ForwardsTo")
						.where(__.out("org.onap.relationships.inventory.BelongsTo").has("forwarding-path-id", evc))
						.out("org.onap.relationships.inventory.Uses")
						.in("org.onap.relationships.inventory.BelongsTo"); 
				
				// fwd-evc not found for pnf + interface
				if(!forwarderEvcGT.hasNext()){
					forwarderEvcId = pnf + " " + evc;
					migrationError(PNF_NODE_TYPE + "/" + EVC_NODE_TYPE, forwarderEvcId, "ivlan", ivlanValue);
					
				}
				
				while(forwarderEvcGT.hasNext()){
					forwarderEvcVtx = forwarderEvcGT.next();
					
					// fwd-evc vertex is null 
					if(forwarderEvcVtx == null){
						forwarderEvcId = pnf + " " + evc;
						migrationError(PNF_NODE_TYPE + "/" + EVC_NODE_TYPE, forwarderEvcId, "ivlan", ivlanValue);
					}
					// update fwd-evc with ivlan value
					else{
												
						forwarderEvcId = forwarderEvcVtx.property("forwarder-evc-id").value().toString();
						try{
							forwarderEvcVtx.property("ivlan", ivlanValue);
							logger.info(String.format("Updating Node Type forwarder-evc Property ivlan value %s", ivlanValue.toString()));
							this.touchVertexProperties(forwarderEvcVtx, false);
							updateDmaapList(forwarderEvcVtx);
							migrationSuccess++;	
							
						}catch (Exception e){
							logger.info(e.toString());
							migrationError(FORWRDER_EVC_NODE_TYPE, forwarderEvcId, "ivlan", ivlanValue);
						}							
					}
				}	
			}
		
		}
	}
	
	/** 
	 * Description: Validate if pnf node exists in Graph
	 * @param pnf 
	 * @return boolean
	 */
	private boolean pnfExists(String pnf){
		if (invalidPnfList.contains(pnf)){
			logger.info(this.MIGRATION_ERROR + "ERROR: PNF value " + pnf + " does not exist.");
			return false;
		}
		if (validPnfList.contains(pnf)){
			return true;
		}
		
		GraphTraversal<Vertex, Vertex> pnfGT = g.V()
				.has("pnf-name", pnf).has(AAIProperties.NODE_TYPE, PNF_NODE_TYPE);
		
		if(pnfGT.hasNext()){
			validPnfList.add(pnf);
			return true;
		}
		else{
			logger.info(this.MIGRATION_ERROR + "ERROR: PNF value " + pnf + " does not exist.");
			invalidPnfList.add(pnf);
			return false;
		}

	}
	
	/**
	 * Description: Validate if p-interface or lag-interface node exists in Graph
	 * @param pnf
	 * @param interfaceNodeType
	 * @param interfaceName
	 */
	private void validateInterface(String pnf, String interfaceNodeType, String interfaceName){
		
		List <String> validInterfaceList;
		List <String> invalidInterfaceList;
		
		if(!validInterfaceMap.containsKey(pnf) ){
			validInterfaceList = new ArrayList<String>();
		}else{
			validInterfaceList = validInterfaceMap.get(pnf);			
		}
		
		if(!invalidInterfaceMap.containsKey(pnf)){
			invalidInterfaceList = new ArrayList<String>();
		}else{
			invalidInterfaceList = invalidInterfaceMap.get(pnf);			
		}
		
		if(invalidInterfaceList.contains(interfaceName)){
			logger.info(this.MIGRATION_ERROR + "ERROR PNF " + pnf  + " with a " + interfaceNodeType + " of " + interfaceName + " does not exist.");
			return;
		}
		if(validInterfaceList.contains(interfaceName)){
			return;
		}
		
		GraphTraversal<Vertex, Vertex> interfaceGT = g.V()
				.has("pnf-name", pnf).has(AAIProperties.NODE_TYPE, PNF_NODE_TYPE)
				.in("tosca.relationships.network.BindsTo")
				.has("interface-name", interfaceName).has(AAIProperties.NODE_TYPE, interfaceNodeType);
		
		if(interfaceGT.hasNext()){
			validInterfaceList.add(interfaceName);	
			validInterfaceMap.put(pnf, validInterfaceList);
		}
		else{
			logger.info(this.MIGRATION_ERROR + "ERROR PNF " + pnf  + " with a " + interfaceNodeType + " of " + interfaceName + " does not exist.");
			invalidInterfaceList.add(interfaceName);	
			invalidInterfaceMap.put(pnf, invalidInterfaceList);
		}
	}
	
	
	/**
	 * Description: Error Routine if graph is not updated by input file record
	 * @param nodeType
	 * @param nodeId
	 * @param property
	 * @param propertyValue
	 */
	private void migrationError(String nodeType, String nodeId, String property, String propertyValue){
		logger.info(this.MIGRATION_ERROR + "ERROR: Failure to update " 
				+ nodeType + " ID " + nodeId + ", " + property + " to value " + propertyValue 
				+ ".  Node Not Found \n");
		migrationFailure++;
	}
	
	private int getInvalidInterfaceCount(){
		int interfaceCount = 0;

		for (Map.Entry<String, List<String>> entry: invalidInterfaceMap.entrySet()){
    		String key = entry.getKey();
    		List <String> invalidList = invalidInterfaceMap.get(key);
    		    		
    		for (int i = 0; i < invalidList.size(); i++){
    			if(invalidList.get(i).contains(".")){
    				invalidPInterfaceCount++;
    			}else{
    				invalidLagInterfaceCount++;
    			}
    			
    		} 				
    		interfaceCount = interfaceCount + invalidInterfaceMap.get(key).size();   			
    	} 
		return interfaceCount;
	}
	
	/**
	 * Description: Dmaap Routine
	 * @param v
	 */
	private void updateDmaapList(Vertex v){
    	String dmaapMsg = System.nanoTime() + "_" + v.id().toString() + "_"	+ v.value("resource-version").toString();
        dmaapMsgList.add(dmaapMsg);
        logger.info("\tAdding Updated Vertex " + v.id().toString() + " to dmaapMsgList....");
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
        return Optional.of(new String[]{this.FORWRDER_EVC_NODE_TYPE});
    }


	@Override
	public String getMigrationName() {
		return "MigrateSdnaIvlanData";
	}

}
