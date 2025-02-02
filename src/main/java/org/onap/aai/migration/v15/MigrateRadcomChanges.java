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
package org.onap.aai.migration.v15;
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
import java.util.Iterator;
import java.util.List;
import java.util.Optional;

import org.apache.tinkerpop.gremlin.process.traversal.dsl.graph.GraphTraversal;
import org.apache.tinkerpop.gremlin.process.traversal.dsl.graph.GraphTraversalSource;
import org.apache.tinkerpop.gremlin.structure.Vertex;
import org.javatuples.Pair;
import org.onap.aai.db.props.AAIProperties;
import org.onap.aai.edges.EdgeIngestor;
import org.onap.aai.edges.enums.EdgeType;
import org.onap.aai.introspection.LoaderFactory;
import org.onap.aai.migration.EdgeSwingMigrator;
import org.onap.aai.migration.Enabled;
import org.onap.aai.migration.MigrationDangerRating;
import org.onap.aai.migration.MigrationPriority;
import org.onap.aai.migration.Status;
import org.onap.aai.serialization.db.EdgeSerializer;
import org.onap.aai.serialization.engines.TransactionalGraphEngine;
import org.onap.aai.setup.SchemaVersions;


@MigrationPriority(26)
@MigrationDangerRating(100)
@Enabled
public class MigrateRadcomChanges extends EdgeSwingMigrator {

	private final String SERVICE_MODEL_TYPE = "Service";
	private final String RESOURCE_MODEL_TYPE = "VNF-Resource";
	private final String MODEL_INVARIANT_ID = "model-invariant-id";
	private final String MODEL_INVARIANT_ID_LOCAL = "model-invariant-id-local";
	private final String MODEL_VERSION_ID = "model-version-id";
	private final String MODEL_VERSION_ID_LOCAL = "model-version-id-local";
	private final String MODEL_CUSTOMIZATION_ID = "model-customization-id";
	private final String PERSONA_MODEL_VERSION = "persona-model-version";
	private final String GENERIC_VNF = "generic-vnf";
	private final String VNF_NAME = "vnf-name";
	private final String VNF_TYPE = "vnf-type";
	private final String SERVICE_INSTANCE = "service-instance";
	private final String SERVICE_INSTANCE_ID = "service-instance-id";
	private final String VF_MODULE = "vf-module";
	private final String VF_MODULE_ID = "vf-module-id";
	private final String MODEL = "model";
	private final String MODEL_VER = "model-ver";
	private final String MODEL_NAME = "model-name";
	private final String MODEL_VERSION = "model-version";
	private final String MODEL_ELEMENT = "model-element";
	private final String VSERVER = "vserver";
	private final String VSERVER_ID = "vserver-id";
	private final String IMAGE = "image";
	private final String IMAGE_NAME = "image-name";
	private final String TENANT = "tenant";
	private final String CLOUD_REGION = "cloud-region";
	
	private static boolean success = true;
    private static boolean checkLog = false;
    private static GraphTraversalSource g = null;
    private int headerLength;
    private int genericVnfMigrationSuccess = 0;
    private int genericVnfMigrationFailure = 0;
    private int serviceInstanceMigrationSuccess = 0;
    private int serviceInstanceMigrationFailure = 0;
    private int vfModuleMigrationSuccess = 0;
    private int vfModuleMigrationFailure = 0;
    private int imageMigrationSuccess = 0;
    private int imageMigrationFailure = 0;
    
    private static List<String> dmaapMsgList = new ArrayList<String>();
    private static final String homeDir = System.getProperty("AJSC_HOME");

    protected class VfModuleFileData {
    	String vfModuleId;
		String vfModuleModelName;
    	String imageName;	
    	
    	public VfModuleFileData(String vfModuleId, String vfModuleModelName, String imageName) {
    		this.vfModuleId = vfModuleId;
    		this.vfModuleModelName = vfModuleModelName;
    		this.imageName = imageName;
    	}
    	
    	public String getVfModuleId() {
			return vfModuleId;
		}
		public void setVfModuleId(String vfModuleId) {
			this.vfModuleId = vfModuleId;
		}
		public String getVfModuleModelName() {
			return vfModuleModelName;
		}
		public void setVfModuleModelName(String vfModuleModelName) {
			this.vfModuleModelName = vfModuleModelName;
		}
		public String getImageName() {
			return imageName;
		}
		public void setImageName(String imageName) {
			this.imageName = imageName;
		}
    }
    
    
    
    public MigrateRadcomChanges(TransactionalGraphEngine engine, LoaderFactory loaderFactory, EdgeIngestor edgeIngestor, EdgeSerializer edgeSerializer, SchemaVersions schemaVersions) {
        super(engine, loaderFactory, edgeIngestor, edgeSerializer, schemaVersions);
        g = this.engine.asAdmin().getTraversalSource();
    }
    
    @Override   
    public void executeModifyOperation() {
        logger.info("---------- Start migration ----------");
        String configDir = System.getProperty("BUNDLECONFIG_DIR");
        if (homeDir == null) {
            logger.info(MIGRATION_ERROR + "ERROR: Could not find sys prop AJSC_HOME");
            success = false;
            return;
        }
        if (configDir == null) {
            success = false;
            return;
        }
        
        ArrayList<VfModuleFileData> vfModuleFileLineList = new ArrayList<VfModuleFileData>();
    	
        String feedDir = homeDir + "/" + configDir + "/" + "migration-input-files/radcom-changes/";
        String fileName = feedDir+ "INPUT-MODEL.csv";
        int genericVnfFileLineCounter = 0;
      
        logger.info(fileName);
        logger.info("---------- Reading all file types and vf-modules ----------");
        ArrayList<String> fileTypeList = new ArrayList<String>();
        try  {
        	List<String> lines = Files.readAllLines(Path.of(fileName));
            Iterator<String> lineItr = lines.iterator();
            int typeFileLineCounter = 0;
            while (lineItr.hasNext()){
            	String line = lineItr.next().replace("\n", "").replace("\r", "");
            	if (!line.isEmpty()) {
                    if (typeFileLineCounter != 0) {
                        String[] colList = line.split(",", -1);
                        if(!colList[0].trim().equalsIgnoreCase(SERVICE_MODEL_TYPE) && !colList[0].trim().equalsIgnoreCase(RESOURCE_MODEL_TYPE)) {
    						vfModuleFileLineList.add(new VfModuleFileData(colList[0].trim(), colList[5].trim(), colList[6].trim()));
    					}
                        if(!colList[1].trim().isEmpty() && !fileTypeList.contains(colList[1].trim())) {
                        	fileTypeList.add(colList[1].trim());
                        }
                    } else {
                        this.headerLength = line.split(",", -1).length;
                        logger.info("headerLength: " + headerLength + "\n");
                        if (this.headerLength != 7 ){
                            logger.info(MIGRATION_ERROR + "ERROR: Input file should have 7 columns");
                            success = false;
                            return;
                        }
                    }
                }
                typeFileLineCounter++;
            }
        } catch (FileNotFoundException e) {
            logger.info(MIGRATION_ERROR + "ERROR: Could not file file " + fileName, e.getMessage());
            success = false;
            checkLog = true;
        } catch (IOException e) {
            logger.info(MIGRATION_ERROR + "ERROR: Issue reading file " + fileName, e);
            success = false;
        } catch (Exception e) {
            logger.info(MIGRATION_ERROR + "encountered exception", e);
            success = false;
        }
        
        int numberOfFileTypes = fileTypeList.size();
        for(int i = 0; i < numberOfFileTypes; i++) {
        	String newServiceModelInvariantId = "";
        	String newServiceModelVersionId = "";
    		String newResourceModelInvariantId = "";
        	String newResourceModelVersionId = "";
        	String newResourceModelCustomizationId = "";
        	ArrayList<String> genericVnfList = new ArrayList<String>();
        	

            int modelFileLineCounter = 0;
            genericVnfFileLineCounter = 0;
            fileName = feedDir+ "INPUT-MODEL.csv";
        	
        	logger.info(fileName);
        	logger.info("---------- Processing Entries from file  ----------");
        	try  {
        		List<String> lines = Files.readAllLines(Path.of(fileName));
        		Iterator<String> lineItr = lines.iterator();
        		while (lineItr.hasNext()){
        			String line = lineItr.next().replace("\n", "").replace("\r", "");
        			if (!line.isEmpty()) {
        				if (modelFileLineCounter != 0) {
        					String[] colList = line.split(",", -1);
        					if(colList[1].trim().equals(fileTypeList.get(i)) && colList[0].trim().equalsIgnoreCase(SERVICE_MODEL_TYPE)) {
        						newServiceModelInvariantId = colList[2].trim();
        						newServiceModelVersionId = colList[3].trim();
        					}
        					else if(colList[1].trim().equals(fileTypeList.get(i)) && colList[0].trim().equalsIgnoreCase(RESOURCE_MODEL_TYPE)) {
        						newResourceModelInvariantId = colList[2].trim();
        						newResourceModelVersionId = colList[3].trim();
        						newResourceModelCustomizationId = colList[4].trim();
        					}
        				}
        			}
        			modelFileLineCounter++;
        		}
        		fileName = feedDir+ "INPUT-VNF.csv";
        		logger.info(fileName);
        		logger.info("---------- Processing Entries from file  ----------");
        		lines = Files.readAllLines(Path.of(fileName));
        		lineItr = lines.iterator();
        		while (lineItr.hasNext()){
        			String line = lineItr.next().replace("\n", "").replace("\r", "");
        			if (!line.isEmpty()) {
        				if (genericVnfFileLineCounter != 0) {
        					String[] colList = line.split(",", -1);
        					if(colList[1].trim().equals(fileTypeList.get(i))) {
        						genericVnfList.add(colList[0].trim());
        					}
        				} else {
        					this.headerLength = line.split(",", -1).length;
        					logger.info("headerLength: " + headerLength + "\n");
        					if (this.headerLength != 2){
        						logger.info(MIGRATION_ERROR + "ERROR: Input file should have 2 columns");
        						success = false;
        						return;
        					}
        				}
        			}
        			genericVnfFileLineCounter++;
        		}	
        		updateGenericVnfs(fileTypeList.get(i), genericVnfList, newServiceModelInvariantId, newServiceModelVersionId,
            		newResourceModelInvariantId, newResourceModelVersionId, newResourceModelCustomizationId, vfModuleFileLineList);
        	} catch (FileNotFoundException e) {
                logger.info(MIGRATION_ERROR + "ERROR: Could not file file " + fileName, e.getMessage());
                success = false;
                checkLog = true;
            } catch (IOException e) {
                logger.info(MIGRATION_ERROR + "ERROR: Issue reading file " + fileName, e);
                success = false;
            } catch (Exception e) {
                logger.info(MIGRATION_ERROR + "encountered exception", e);
                success = false;
            }
        }
        logger.info ("\n \n ******* Final Summary for RADCOM Change Migration ********* \n");
        logger.info(MIGRATION_SUMMARY_COUNT + "Total generic-vnfs in File: "+(genericVnfFileLineCounter + 1));
        logger.info(MIGRATION_SUMMARY_COUNT + " generic-vnfs processed: "+ genericVnfMigrationSuccess);
        logger.info(MIGRATION_SUMMARY_COUNT + " generic-vnfs failed to process: "+ genericVnfMigrationFailure);
        logger.info(MIGRATION_SUMMARY_COUNT + " service-instances processed: "+ serviceInstanceMigrationSuccess);
        logger.info(MIGRATION_SUMMARY_COUNT + " service-instances failed to process: "+ serviceInstanceMigrationFailure);
        logger.info(MIGRATION_SUMMARY_COUNT + " vf-modules processed: "+ vfModuleMigrationSuccess);
        logger.info(MIGRATION_SUMMARY_COUNT + " vf-modules failed to process: "+ vfModuleMigrationFailure);
        logger.info(MIGRATION_SUMMARY_COUNT + " images processed: "+ imageMigrationSuccess);
        logger.info(MIGRATION_SUMMARY_COUNT + " images failed to process: "+ imageMigrationFailure +"\n");
    }
	
	private void updateGenericVnfs(String vnfType, ArrayList<String> genericVnfList, String newServiceModelInvariantId,
			String newServiceModelVersionId, String newResourceModelInvariantId, String newResourceModelVersionId, 
			String newResourceModelCustomizationId, ArrayList<VfModuleFileData> vfModuleFileLineList) {
		int numberOfNames = genericVnfList.size();
		Vertex newModelVerNode = null;
		GraphTraversal<Vertex, Vertex> modelVerNodeList = g.V().has(AAIProperties.NODE_TYPE, MODEL).
				has(MODEL_INVARIANT_ID, newResourceModelInvariantId).in("org.onap.relationships.inventory.BelongsTo").
				has(AAIProperties.NODE_TYPE, MODEL_VER).has(MODEL_VERSION_ID, newResourceModelVersionId);
		if(!modelVerNodeList.hasNext()) {
			logger.info(MIGRATION_ERROR + "ERROR: Model " + newResourceModelInvariantId + " with model-ver "
					 + newResourceModelVersionId + " does not exist in database \n");
			for(int i = 0; i < numberOfNames; i++) {
				genericVnfMigrationFailure++;
			}
		}
		else {
			newModelVerNode = modelVerNodeList.next();
			for(int i = 0; i < numberOfNames; i++) {
				GraphTraversal<Vertex, Vertex> genericVnfNodeList = g.V().has(AAIProperties.NODE_TYPE, GENERIC_VNF).
						has(VNF_NAME, genericVnfList.get(i)).has(VNF_TYPE, vnfType);
				if(!genericVnfNodeList.hasNext()) {
					logger.info(MIGRATION_ERROR + "ERROR: Failure to update generic-vnf " + genericVnfList.get(i) + 
							" Graph Traversal failed \n");
					genericVnfMigrationFailure++;
				}
				while (genericVnfNodeList.hasNext()) {
					Vertex genericVnfVtx = genericVnfNodeList.next();
					boolean updateSuccess = false;
					if (genericVnfVtx != null) {
						logger.info("Updating generic-vnf " + genericVnfVtx.value(VNF_NAME) + " with "
								+ "current model-invariant-id "
								+ (genericVnfVtx.property(MODEL_INVARIANT_ID).isPresent()
										? genericVnfVtx.value(MODEL_INVARIANT_ID) : "null")
										+ ", current model-version-id " 
										+ (genericVnfVtx.property(MODEL_VERSION_ID).isPresent()
												? genericVnfVtx.value(MODEL_VERSION_ID) : "null")
												+ ", and current model-customization-id " 
												+ (genericVnfVtx.property(MODEL_CUSTOMIZATION_ID).isPresent()
														? genericVnfVtx.value(MODEL_CUSTOMIZATION_ID) : "null")
														+ " to use model-invariant-id " + newResourceModelInvariantId + ","
														+ " model-version-id " + newResourceModelVersionId + " and model-customization-id "
														+ newResourceModelCustomizationId);
						try {
							Vertex oldModelVerNode = null;
							GraphTraversal<Vertex, Vertex> modelVerQuery= g.V(genericVnfVtx).out("org.onap.relationships.inventory.IsA")
									.has(AAIProperties.NODE_TYPE, MODEL_VER);
							if(modelVerQuery.hasNext()) {
								oldModelVerNode = modelVerQuery.next();
							}
							genericVnfVtx.property(MODEL_INVARIANT_ID_LOCAL, newResourceModelInvariantId);
							genericVnfVtx.property(MODEL_VERSION_ID_LOCAL, newResourceModelVersionId);
							genericVnfVtx.property(MODEL_CUSTOMIZATION_ID, newResourceModelCustomizationId);
							if(newModelVerNode.property(MODEL_VERSION).isPresent()) {
								genericVnfVtx.property(PERSONA_MODEL_VERSION, newModelVerNode.value(MODEL_VERSION));
							}
							this.touchVertexProperties(genericVnfVtx, false);
							if(oldModelVerNode != null) {
								this.swingEdges(oldModelVerNode, newModelVerNode, GENERIC_VNF, "org.onap.relationships.inventory.IsA", "IN");
							}
							else {
								this.createPrivateEdge(newModelVerNode, genericVnfVtx);
							}
							updateSuccess = true;	
						} catch (Exception e) {
							logger.info(e.toString());
							logger.info(MIGRATION_ERROR + "ERROR: Failure to update generic-vnf " + genericVnfList.get(i) + "\n");
							genericVnfMigrationFailure++;								
						}
						if(updateSuccess) {
							String dmaapMsg = System.nanoTime() + "_" + genericVnfVtx.id().toString() + "_"	+ 
									genericVnfVtx.value("resource-version").toString();
							dmaapMsgList.add(dmaapMsg);
							logger.info("Update of generic-vnf " + genericVnfList.get(i) + " successful \n");
							genericVnfMigrationSuccess++;
							updateServiceInstances(vnfType, genericVnfList.get(i), newServiceModelInvariantId,
									newServiceModelVersionId);
							updateVfModules(vnfType, genericVnfList.get(i), newResourceModelInvariantId, newResourceModelVersionId,
									vfModuleFileLineList);
						}
					}
					else {
						logger.info(MIGRATION_ERROR + "ERROR: Failure to update generic-vnf " + genericVnfList.get(i) + 
								" Graph Traversal returned an empty vertex \n");
						genericVnfMigrationFailure++;
					}
				}
			}
		}
	}
	
	private void updateServiceInstances(String vnfType, String vnfName, String newServiceModelInvariantId,
			String newServiceModelVersionId) {
		GraphTraversal<Vertex, Vertex> serviceInstanceNodeList = g.V().
			has(AAIProperties.NODE_TYPE, GENERIC_VNF).has(VNF_NAME, vnfName).has(VNF_TYPE, vnfType).
			in("org.onap.relationships.inventory.ComposedOf").has(AAIProperties.NODE_TYPE, SERVICE_INSTANCE);
		Vertex newModelVerNode = null;
		GraphTraversal<Vertex, Vertex> modelVerNodeList = g.V().has(AAIProperties.NODE_TYPE, MODEL).
				has(MODEL_INVARIANT_ID, newServiceModelInvariantId).in("org.onap.relationships.inventory.BelongsTo").
				has(AAIProperties.NODE_TYPE, MODEL_VER).has(MODEL_VERSION_ID, newServiceModelVersionId);
		if(!modelVerNodeList.hasNext()) {
			logger.info(MIGRATION_ERROR + "ERROR: Model " + newServiceModelInvariantId + " with model-ver "
					 + newServiceModelVersionId + " does not exist in database \n");
			while(serviceInstanceNodeList.hasNext()) { 
				serviceInstanceNodeList.next();
				serviceInstanceMigrationFailure++;
			}
		}
		else {
			newModelVerNode = modelVerNodeList.next();
			while (serviceInstanceNodeList.hasNext()) {
				Vertex serviceInstanceVtx = serviceInstanceNodeList.next();
				boolean updateSuccess = false;
				if (serviceInstanceVtx != null) {
					logger.info("Updating service-instance " + serviceInstanceVtx.value(SERVICE_INSTANCE_ID)
							+ " with current model-invariant-id " 
							+ (serviceInstanceVtx.property(MODEL_INVARIANT_ID).isPresent()
									? serviceInstanceVtx.value(MODEL_INVARIANT_ID) : "null")
									+ " and current model-version-id "  
									+ (serviceInstanceVtx.property(MODEL_VERSION_ID).isPresent()
											? serviceInstanceVtx.value(MODEL_VERSION_ID) : "null")
											+ " to use model-invariant-id " + newServiceModelInvariantId + " and"
											+ " model-version-id " + newServiceModelVersionId);
					try {
						Vertex oldModelVerNode = null;
						GraphTraversal<Vertex, Vertex> modelVerQuery= g.V(serviceInstanceVtx).out("org.onap.relationships.inventory.IsA")
								.has(AAIProperties.NODE_TYPE, MODEL_VER);
						if(modelVerQuery.hasNext()) {
							oldModelVerNode = modelVerQuery.next();
						}
						serviceInstanceVtx.property(MODEL_INVARIANT_ID_LOCAL, newServiceModelInvariantId);
						serviceInstanceVtx.property(MODEL_VERSION_ID_LOCAL, newServiceModelVersionId);
						if(newModelVerNode.property(MODEL_VERSION).isPresent()) {
							serviceInstanceVtx.property(PERSONA_MODEL_VERSION, newModelVerNode.value(MODEL_VERSION));
						}
						this.touchVertexProperties(serviceInstanceVtx, false);
						if(oldModelVerNode != null) {
							this.swingEdges(oldModelVerNode, newModelVerNode, SERVICE_INSTANCE, "org.onap.relationships.inventory.IsA", "IN");
						}
						else {
							this.createPrivateEdge(newModelVerNode, serviceInstanceVtx);
						}
						updateSuccess = true;
					} catch (Exception e) {
						logger.info(e.toString());
						logger.info(MIGRATION_ERROR + "ERROR: Failure to update service-instance " 
								+ serviceInstanceVtx.value(SERVICE_INSTANCE_ID) + "\n");
						serviceInstanceMigrationFailure++;								
					}
					if(updateSuccess) {
						String dmaapMsg = System.nanoTime() + "_" + serviceInstanceVtx.id().toString() + "_"	+ 
								serviceInstanceVtx.value("resource-version").toString();
						dmaapMsgList.add(dmaapMsg);
						logger.info("Update of service-instance " 
								+ serviceInstanceVtx.value(SERVICE_INSTANCE_ID) + " successful \n");
						serviceInstanceMigrationSuccess++;         
					}
				}
			}
		}
	}
	
	private void updateVfModules(String vnfType, String vnfName, String newResourceModelInvariantId,
			String newResourceModelVersionId, ArrayList<VfModuleFileData> vfModuleFileLineList) {
		int numberOfLines = vfModuleFileLineList.size();
		ArrayList<Integer> processedNodes = new ArrayList<Integer>();
		for(int i = 0; i < numberOfLines; i++) {
			VfModuleFileData currentLine = vfModuleFileLineList.get(i);
			String vfModuleId = currentLine.getVfModuleId();
			String vfModuleModelName = currentLine.getVfModuleModelName();
			String imageName = currentLine.getImageName();
			String vfModuleInvariantId = "";
			String vfModuleVersionId = "";
			GraphTraversal<Vertex, Vertex> vfModuleNodeList = g.V().
					has(AAIProperties.NODE_TYPE, GENERIC_VNF).has(VNF_NAME, vnfName).has(VNF_TYPE, vnfType).
					in("org.onap.relationships.inventory.BelongsTo").has(AAIProperties.NODE_TYPE, VF_MODULE).
					has(VF_MODULE_ID, vfModuleId);
			if(vfModuleNodeList.hasNext()) {
				GraphTraversal<Vertex, Vertex> modelElementNodeList = g.V().
						has(AAIProperties.NODE_TYPE, MODEL).has(MODEL_INVARIANT_ID, newResourceModelInvariantId).
						in("org.onap.relationships.inventory.BelongsTo").has(AAIProperties.NODE_TYPE, MODEL_VER).
						has(MODEL_VERSION_ID, newResourceModelVersionId).in("org.onap.relationships.inventory.BelongsTo").
						has(AAIProperties.NODE_TYPE, MODEL_ELEMENT);
				while(modelElementNodeList.hasNext()) {
					Vertex modelElement = modelElementNodeList.next();
					GraphTraversal<Vertex, Vertex> modelVersionLookup = g.V(modelElement).out("org.onap.relationships.inventory.IsA").
							has(AAIProperties.NODE_TYPE, MODEL_VER);
					while(modelVersionLookup.hasNext()) {
						Vertex modelVersionVertex = modelVersionLookup.next();
						if(modelVersionVertex.value(MODEL_NAME).equals(vfModuleModelName)) {
							vfModuleVersionId = modelVersionVertex.value(MODEL_VERSION_ID);
							vfModuleInvariantId = g.V(modelVersionVertex).out("org.onap.relationships.inventory.BelongsTo").
									has(AAIProperties.NODE_TYPE, MODEL).next().value(MODEL_INVARIANT_ID);
							break;
						}
					}
					if(!vfModuleVersionId.isEmpty() && !vfModuleInvariantId.isEmpty()) {
						break;
					}
					GraphTraversal<Vertex, Vertex> modelElementLookup = g.V(modelElement).in("org.onap.relationships.inventory.BelongsTo").
							has(AAIProperties.NODE_TYPE, MODEL_ELEMENT);
					while(modelElementLookup.hasNext()) {
						ArrayList<String> returnedValues = recursiveSearchForModelName(vfModuleModelName, modelElementLookup.next());
						if(!returnedValues.isEmpty()) {
							vfModuleInvariantId = returnedValues.get(0);
							vfModuleVersionId = returnedValues.get(1);
							break;
						}
					}	
					if(!vfModuleVersionId.isEmpty() && !vfModuleInvariantId.isEmpty()) {
						break;
					}
				}	
				while (vfModuleNodeList.hasNext()) {
					Vertex vfModuleVtx = vfModuleNodeList.next();
					boolean updateSuccess = false;
					if (vfModuleVtx != null) {
						if(vfModuleInvariantId.isEmpty() && vfModuleVersionId.isEmpty()) {
							logger.info(MIGRATION_ERROR + "ERROR: Failure to update vf-module " +vfModuleVtx.value(VF_MODULE_ID) + 
									". model-invariant-id and model-version-id not found \n");
							vfModuleMigrationFailure++;
						}
						else if(vfModuleInvariantId.isEmpty()) {
							logger.info(MIGRATION_ERROR + "ERROR: Failure to update vf-module " +vfModuleVtx.value(VF_MODULE_ID) + 
									". model-invariant-id not found \n");
							vfModuleMigrationFailure++;
						}
						else if(vfModuleVersionId.isEmpty()) {
							logger.info(MIGRATION_ERROR + "ERROR: Failure to update vf-module " +vfModuleVtx.value(VF_MODULE_ID) + 
									". model-version-id not found \n");
							vfModuleMigrationFailure++;
						}
						else {
							logger.info("Updating vf-module " + vfModuleVtx.value(VF_MODULE_ID)
									+ " with current model-invariant-id " 
									+ (vfModuleVtx.property(MODEL_INVARIANT_ID).isPresent()
									? vfModuleVtx.value(MODEL_INVARIANT_ID) : "null")
									+ " and current model-version-id "  
									+ (vfModuleVtx.property(MODEL_VERSION_ID).isPresent()
								    ? vfModuleVtx.value(MODEL_VERSION_ID) : "null")
								    + " to use model-invariant-id " + vfModuleInvariantId + " and"
								    + " model-version-id " + vfModuleVersionId);
							Vertex newModelVerNode = null;
							GraphTraversal<Vertex, Vertex> modelVerNodeList = g.V().has(AAIProperties.NODE_TYPE, MODEL).
									has(MODEL_INVARIANT_ID, vfModuleInvariantId).in("org.onap.relationships.inventory.BelongsTo").
									has(AAIProperties.NODE_TYPE, MODEL_VER).has(MODEL_VERSION_ID, vfModuleVersionId);
							if(!modelVerNodeList.hasNext()) {
								logger.info(MIGRATION_ERROR + "ERROR: Model " + vfModuleInvariantId + " with model-ver "
										 + vfModuleVersionId + " could not be found in traversal, error in finding vf-module model \n");
								vfModuleMigrationFailure++;
							}
							else {
								newModelVerNode = modelVerNodeList.next();
								try {
									Vertex oldModelVerNode = null;
									GraphTraversal<Vertex, Vertex> modelVerQuery= g.V(vfModuleVtx).out("org.onap.relationships.inventory.IsA")
											.has(AAIProperties.NODE_TYPE, MODEL_VER);
									if(modelVerQuery.hasNext()) {
										oldModelVerNode = modelVerQuery.next();
									}
									vfModuleVtx.property(MODEL_INVARIANT_ID_LOCAL, vfModuleInvariantId);
									vfModuleVtx.property(MODEL_VERSION_ID_LOCAL, vfModuleVersionId);
									if(newModelVerNode.property(MODEL_VERSION).isPresent()) {
										vfModuleVtx.property(PERSONA_MODEL_VERSION, newModelVerNode.value(MODEL_VERSION));
									}
									this.touchVertexProperties(vfModuleVtx, false);
									if(oldModelVerNode != null) {
										this.swingEdges(oldModelVerNode, newModelVerNode, VF_MODULE, "org.onap.relationships.inventory.IsA", "IN");
									}
									else {
										this.createPrivateEdge(newModelVerNode, vfModuleVtx);
									}
									updateSuccess = true;	
								} catch (Exception e) {
									logger.info(e.toString());
									logger.info(MIGRATION_ERROR + "ERROR: Failure to update vf-module " 
											+ vfModuleVtx.value(VF_MODULE_ID) + "\n");
									vfModuleMigrationFailure++;								
								}
							}
						}
					}
					if(updateSuccess) {
						String dmaapMsg = System.nanoTime() + "_" + vfModuleVtx.id().toString() + "_"	+ 
								vfModuleVtx.value("resource-version").toString();
						dmaapMsgList.add(dmaapMsg);
						logger.info("Update of vf-module " 
								+ vfModuleVtx.value(VF_MODULE_ID) + " successful \n");
						vfModuleMigrationSuccess++;
						if(!processedNodes.contains(i)) {
							processedNodes.add(i);
						}
						updateVserverAndImage(vfModuleId, imageName);
					}
				}
			}	
		}
		int processedNodesNum = processedNodes.size();
		for (int i = 0; i < processedNodesNum; i++) {
			vfModuleFileLineList.remove(i);
		}
	}		
	
	private ArrayList<String> recursiveSearchForModelName(String vfModuleModelName, Vertex modelElement) {
		ArrayList<String> returnedValues = new ArrayList<String>();
		GraphTraversal<Vertex, Vertex> modelVersionLookup = g.V(modelElement).out("org.onap.relationships.inventory.IsA").
				has(AAIProperties.NODE_TYPE, MODEL_VER);
		while(modelVersionLookup.hasNext()) {
			Vertex modelVersionVertex = modelVersionLookup.next();
			if(modelVersionVertex.value(MODEL_NAME).equals(vfModuleModelName)) {
				returnedValues.add(modelVersionVertex.value(MODEL_VERSION_ID));
				returnedValues.add(0, g.V(modelVersionVertex).out("org.onap.relationships.inventory.BelongsTo")
						.next().value(MODEL_INVARIANT_ID));
				return returnedValues;
			}
		}
		GraphTraversal<Vertex, Vertex> modelElementLookup = g.V(modelElement).in("org.onap.relationships.inventory.BelongsTo").
				has(AAIProperties.NODE_TYPE, MODEL_ELEMENT);
		while(modelElementLookup.hasNext()) {
			returnedValues = recursiveSearchForModelName(vfModuleModelName, modelElementLookup.next());
			if(!returnedValues.isEmpty()) {
				return returnedValues;
			}
		}
		return returnedValues;
	}
	
	private void updateVserverAndImage(String vfModuleId, String imageName) {
		GraphTraversal<Vertex, Vertex> vserverNodeList = g.V().
			has(AAIProperties.NODE_TYPE, VF_MODULE).has(VF_MODULE_ID, vfModuleId).
			out("org.onap.relationships.inventory.Uses").has(AAIProperties.NODE_TYPE, VSERVER);				
		while (vserverNodeList.hasNext()) {
			Vertex vserverVtx = vserverNodeList.next();
			boolean updateSuccess = false;
			GraphTraversal<Vertex, Vertex> oldImageLookup = g.V(vserverVtx).out("org.onap.relationships.inventory.Uses").
					has(AAIProperties.NODE_TYPE, IMAGE);
			Vertex oldImageVtx = null;
			if(oldImageLookup.hasNext()) {
				oldImageVtx = oldImageLookup.next();
			}
			GraphTraversal<Vertex, Vertex> newImageLookup = g.V(vserverVtx).out("org.onap.relationships.inventory.BelongsTo").
					has(AAIProperties.NODE_TYPE, TENANT).out("org.onap.relationships.inventory.BelongsTo").
					has(AAIProperties.NODE_TYPE, CLOUD_REGION).in("org.onap.relationships.inventory.BelongsTo").
					has(AAIProperties.NODE_TYPE, IMAGE).has(IMAGE_NAME, imageName);
			Vertex newImageVtx = null;
			if(newImageLookup.hasNext()) {
				newImageVtx = newImageLookup.next();
			}
			if (vserverVtx != null && newImageVtx!= null) {
				logger.info("Updating vserver " + vserverVtx.value(VSERVER_ID)
					+ " to replace all current image relationships with relationship to new image " + imageName);
				try {
					if(oldImageVtx != null) {
						this.swingEdges(oldImageVtx, newImageVtx, VSERVER, "org.onap.relationships.inventory.Uses", "IN");
					}
					else {
						this.createEdgeIfPossible(EdgeType.COUSIN, vserverVtx, newImageVtx);
					}
					updateSuccess = true;	
				} catch (Exception e) {
						logger.info(e.toString());
						logger.info(MIGRATION_ERROR + "ERROR: Failure to update vserver " 
						+ vserverVtx.value(VSERVER_ID) + " with image " + imageName + "\n");
						imageMigrationFailure++;								
				}
				if(updateSuccess) {
					logger.info("Update of vserver " 
							+ vserverVtx.value(VSERVER_ID) + " with image " + newImageVtx.value(IMAGE_NAME) 
							+ " successful \n");
					imageMigrationSuccess++;         
				}
			}
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
        return Optional.of(new String[]{GENERIC_VNF, SERVICE_INSTANCE, VF_MODULE, VSERVER, IMAGE});
    }

    @Override
    public String getMigrationName() {
        return "MigrateRadcomChanges";
    }

	@Override
	public List<Pair<Vertex, Vertex>> getAffectedNodePairs() {
		return null;
	}

	@Override
	public String getNodeTypeRestriction() {
		return VSERVER;
	}

	@Override
	public String getEdgeLabelRestriction() {
		return "org.onap.relationships.inventory.Uses";
	}

	@Override
	public String getEdgeDirRestriction() {
		return "IN";
	}

	@Override
	public void cleanupAsAppropriate(List<Pair<Vertex, Vertex>> nodePairL) {	
	}
}
