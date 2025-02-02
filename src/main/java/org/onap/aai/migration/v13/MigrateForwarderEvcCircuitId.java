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
package org.onap.aai.migration.v13;
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


@MigrationPriority(26)
@MigrationDangerRating(100)
//@Enabled
public class MigrateForwarderEvcCircuitId extends Migrator {

	private final String PNF_NODE_TYPE = "pnf";
	private final String PROPERTY_PNF_NAME = "pnf-name";
	private final String PROPERTY_INTERFACE_NAME = "interface-name";
	private final String PROPERTY_FORWARDER_ROLE = "forwarder-role";
	private final String VALUE_INGRESS = "ingress";
	private final String PROPERTY_SEQUENCE = "sequence";
	private final int VALUE_EXPECTED_SEQUENCE = 1;
	private final String FORWARDER_EVC_NODE_TYPE = "forwarder-evc";
	private final String PROPERTY_CIRCUIT_ID = "circuit-id";
	
	private static boolean success = true;
    private static boolean checkLog = false;
    private static GraphTraversalSource g = null;
    private int headerLength;
    private int migrationSuccess = 0;
    private int migrationFailure = 0;
    
    private static List<String> dmaapMsgList = new ArrayList<String>();
    private static final String homeDir = System.getProperty("AJSC_HOME");

    protected class CircuitIdFileData {
		String pnfName;
    	String interfaceName;

		String oldCircuitId;
    	String newCircuitId;
    	
    	public String getPnfName() {
			return pnfName;
		}
		public void setPnfName(String pnfName) {
			this.pnfName = pnfName;
		}
		public String getInterfaceName() {
			return interfaceName;
		}
		public void setInterfaceName(String interfaceName) {
			this.interfaceName = interfaceName;
		}
		
		public String getOldCircuitId() {
			return oldCircuitId;
		}
		public void setOldCircuitId(String oldCircuitId) {
			this.oldCircuitId = oldCircuitId;
		}
		public String getNewCircuitId() {
			return newCircuitId;
		}
		public void setNewCircuitId(String newCircutId) {
			this.newCircuitId = newCircutId;
		}
    }
    
    private static ArrayList<CircuitIdFileData> circuitIdList = new ArrayList<CircuitIdFileData>();
	
    public MigrateForwarderEvcCircuitId(TransactionalGraphEngine engine, LoaderFactory loaderFactory, EdgeIngestor edgeIngestor, EdgeSerializer edgeSerializer, SchemaVersions schemaVersions) {
        super(engine, loaderFactory, edgeIngestor, edgeSerializer, schemaVersions);
        this.g = this.engine.asAdmin().getTraversalSource();
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
        String fileName = feedDir+ "circuitIds.csv";
        logger.info(fileName);
        logger.info("---------- Processing Entries from file  ----------");
        try  {
        	List<String> lines = Files.readAllLines(Path.of(fileName));
            Iterator<String> lineItr = lines.iterator();
            while (lineItr.hasNext()){
                String line = lineItr.next().replace("\n", "").replace("\r", "");
                if (!line.isEmpty()) {
                    if (fileLineCounter != 0) {
                        String[] colList = line.split(",", -1);
                        CircuitIdFileData lineData = new CircuitIdFileData();
                        lineData.setPnfName(colList[0].trim().replaceAll("^\"|\"$", "")
                        		.replaceAll("[\t\n\r]+", "").trim());
                        lineData.setInterfaceName(colList[1].trim().replaceAll("^\"|\"$", "")
                        		.replaceAll("[\t\n\r]+", "").trim());
                        lineData.setOldCircuitId(colList[2].trim().replaceAll("^\"|\"$", "")
                        		.replaceAll("[\t\n\r]+", "").trim());
                        lineData.setNewCircuitId(colList[4].trim().replaceAll("^\"|\"$", "")
                        		.replaceAll("[\t\n\r]+", "").trim());
                        circuitIdList.add(lineData);
                    	
                    } else {
                        this.headerLength = line.split(",", -1).length;
                        logger.info("headerLength: " + headerLength + "\n");
                        if (this.headerLength != 6){
                            logger.info(this.MIGRATION_ERROR + "ERROR: Input file should have 6 columns");
                            this.success = false;
                            return;
                        }
                    }
                }
                fileLineCounter++;
            }
            updateCircuitIdCount();
            logger.info ("\n \n ******* Final Summary for Circuit Id Migration ********* \n");
            logger.info(this.MIGRATION_SUMMARY_COUNT + "CircuitIds processed: "+ migrationSuccess);
            logger.info(this.MIGRATION_SUMMARY_COUNT + "Total Rows Count: "+(fileLineCounter + 1));
            logger.info(this.MIGRATION_SUMMARY_COUNT + "Unprocessed CircuitIds : "+ migrationFailure +"\n");
     
        } catch (FileNotFoundException e) {
            logger.info(this.MIGRATION_ERROR + "ERROR: Could not file file " + fileName, e.getMessage());
            success = false;
            checkLog = true;
        } catch (IOException e) {
            logger.info(this.MIGRATION_ERROR + "ERROR: Issue reading file " + fileName, e);
            success = false;
        } catch (Exception e) {
            logger.info(this.MIGRATION_ERROR + "encountered exception", e);
            success = false;
        }
    }
	
	private void updateCircuitIdCount() {
		int numberOfLines = circuitIdList.size();
		for(int i = 0; i < numberOfLines; i ++) {
			GraphTraversal<Vertex, Vertex> nodeList = g.V().has(this.PROPERTY_PNF_NAME, circuitIdList.get(i).getPnfName())
					.has(AAIProperties.NODE_TYPE, PNF_NODE_TYPE).in("tosca.relationships.network.BindsTo")
					.has(this.PROPERTY_INTERFACE_NAME, circuitIdList.get(i).getInterfaceName()).in("org.onap.relationships.inventory.ForwardsTo")
					.has(this.PROPERTY_FORWARDER_ROLE, this.VALUE_INGRESS).has(this.PROPERTY_SEQUENCE, this.VALUE_EXPECTED_SEQUENCE)
					.out("org.onap.relationships.inventory.Uses").in("org.onap.relationships.inventory.BelongsTo");
				if(!nodeList.hasNext()) {
					logger.info(this.MIGRATION_ERROR + "ERROR: Failure to update Circuit Id " + circuitIdList.get(i).getOldCircuitId() +
							" to " + circuitIdList.get(i).getNewCircuitId() + " Graph Traversal failed \n");
					migrationFailure++;
				}
				while (nodeList.hasNext()) {
					Vertex forwarderEvcVtx = nodeList.next();
					boolean updateSuccess = false;
					if (forwarderEvcVtx != null) {
						logger.info("forwarder-evc-id is " + forwarderEvcVtx.value("forwarder-evc-id"));
						if(forwarderEvcVtx.property(PROPERTY_CIRCUIT_ID).isPresent() &&
						   forwarderEvcVtx.value(PROPERTY_CIRCUIT_ID).equals(circuitIdList.get(i).getNewCircuitId())) {
							logger.info("Skipping Record: Old Collector CircuitId " + forwarderEvcVtx.value(PROPERTY_CIRCUIT_ID) +
									" is the same as New Collector CircuitId " + circuitIdList.get(i).getNewCircuitId() + "\n");
							migrationFailure++;
						}
						else if(!circuitIdList.get(i).getNewCircuitId().isEmpty() &&
							forwarderEvcVtx.property(PROPERTY_CIRCUIT_ID).isPresent() &&
							circuitIdList.get(i).getOldCircuitId().equals(forwarderEvcVtx.value(PROPERTY_CIRCUIT_ID)))
						{
							try {
								forwarderEvcVtx.property(PROPERTY_CIRCUIT_ID, circuitIdList.get(i).getNewCircuitId());
								this.touchVertexProperties(forwarderEvcVtx, false);
								updateSuccess = true;

							} catch (Exception e) {
								logger.info(e.toString());
								logger.info(this.MIGRATION_ERROR + "ERROR: Failure to update Circuit Id " + circuitIdList.get(i).getOldCircuitId() +
										" to " + circuitIdList.get(i).getNewCircuitId() + "\n");
								migrationFailure++;

							}
							if(updateSuccess) {
								String dmaapMsg = System.nanoTime() + "_" + forwarderEvcVtx.id().toString() + "_"	+
										forwarderEvcVtx.value("resource-version").toString();
								dmaapMsgList.add(dmaapMsg);
								logger.info("Update of Circuit Id " + circuitIdList.get(i).getOldCircuitId() + " to " +
										circuitIdList.get(i).getNewCircuitId() + " successful \n");
								migrationSuccess++;
							}
						}
						else if(!forwarderEvcVtx.property(PROPERTY_CIRCUIT_ID).isPresent())
						{
							logger.info(this.MIGRATION_ERROR + "ERROR: Old Collector Circuit Id not found " + circuitIdList.get(i).getOldCircuitId() +
									" was not updated to " + circuitIdList.get(i).getNewCircuitId() + "\n");
							migrationFailure++;
						}
						else {
							logger.info(this.MIGRATION_ERROR + "ERROR: Failure to update Circuit Id " + circuitIdList.get(i).getOldCircuitId() +
									" to " + circuitIdList.get(i).getNewCircuitId() + "\n");
							migrationFailure++;
						}
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
        return Optional.of(new String[]{this.FORWARDER_EVC_NODE_TYPE});
    }

    @Override
    public String getMigrationName() {
        return "MigrateForwarderEvcCircuitId";
    }
}
