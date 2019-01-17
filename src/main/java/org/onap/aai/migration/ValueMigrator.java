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
package org.onap.aai.migration;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Map.Entry;

import org.apache.tinkerpop.gremlin.process.traversal.dsl.graph.GraphTraversal;
import org.apache.tinkerpop.gremlin.structure.Vertex;
import org.onap.aai.db.props.AAIProperties;
import org.onap.aai.edges.EdgeIngestor;
import org.onap.aai.introspection.LoaderFactory;
import org.onap.aai.serialization.db.EdgeSerializer;
import org.onap.aai.serialization.engines.TransactionalGraphEngine;
import org.janusgraph.core.schema.JanusGraphManagement;
import org.onap.aai.setup.SchemaVersions;

/**
 * A migration template for filling in default values that are missing or are empty
 */
@MigrationPriority(0)
@MigrationDangerRating(1)
public abstract class ValueMigrator extends Migrator {

    protected final Map<String, Map<String, ?>> propertyValuePairByNodeType;
    protected Map<String, List<?>> conditionsMap;
    protected final Boolean updateExistingValues;
	protected final JanusGraphManagement graphMgmt; 
	
	private int migrationSuccess = 0;
	private Map<String, String> nodeTotalSuccess = new HashMap<>();
	private int subTotal = 0;
	
	private static List<String> dmaapMsgList = new ArrayList<String>();
	
    /**
     *
     * @param engine
     * @param propertyValuePairByNodeType - format {nodeType: { property: newValue}}
     * @param updateExistingValues - if true, updates the value regardless if it already exists
     */
	public ValueMigrator(TransactionalGraphEngine engine, LoaderFactory loaderFactory, EdgeIngestor edgeIngestor, EdgeSerializer edgeSerializer, SchemaVersions schemaVersions, Map propertyValuePairByNodeType, Boolean updateExistingValues) {
		super(engine, loaderFactory, edgeIngestor, edgeSerializer, schemaVersions);
	    this.propertyValuePairByNodeType = propertyValuePairByNodeType;
	    this.updateExistingValues = updateExistingValues;
		this.graphMgmt = engine.asAdmin().getManagementSystem();
	}
	
	//Migrate with property conditions
	public ValueMigrator(TransactionalGraphEngine engine, LoaderFactory loaderFactory, EdgeIngestor edgeIngestor, EdgeSerializer edgeSerializer, SchemaVersions schemaVersions, Map propertyValuePairByNodeType, Map conditionsMap, Boolean updateExistingValues) {
		super(engine, loaderFactory, edgeIngestor, edgeSerializer, schemaVersions);
	    this.propertyValuePairByNodeType = propertyValuePairByNodeType;
	    this.updateExistingValues = updateExistingValues;
	    this.conditionsMap = conditionsMap;
		this.graphMgmt = engine.asAdmin().getManagementSystem();
	}
	
	@Override
	public void commit() {
        engine.commit();
        if(isUpdateDmaap()){
        	createDmaapFiles(this.dmaapMsgList);
        }
	}

	/**
	 * Do not override this method as an inheritor of this class
	 */
	@Override
	public void run() {
	    updateValues();
	}

    protected void updateValues() {
        for (Map.Entry<String, Map<String, ?>> entry: propertyValuePairByNodeType.entrySet()) {
            String nodeType = entry.getKey();  
            this.subTotal = 0;
            
            Map<String, ?> propertyValuePair = entry.getValue();
            for (Map.Entry<String, ?> pair : propertyValuePair.entrySet()) {
                String property = pair.getKey();  
                Object newValue = pair.getValue();
                try {
                    GraphTraversal<Vertex, Vertex> g = this.engine.asAdmin().getTraversalSource().V()
                            .has(AAIProperties.NODE_TYPE, nodeType);  
                    while (g.hasNext()) {
                        Vertex v = g.next();
                     
                        if (this.conditionsMap !=null){
                        	checkConditions( v, property, newValue, nodeType);
                        }else{                        	
                        	migrateValues( v, property, newValue, nodeType);
                        }                                         
                    }
                } catch (Exception e) {
                    logger.error(String.format("caught exception updating aai-node-type %s's property %s's value to " +
                            "%s: %s", nodeType, property, newValue.toString(), e.getMessage()));
                    logger.error(e.getMessage());
                }
            }            
              this.nodeTotalSuccess.put(nodeType, Integer.toString(this.subTotal));
        }
        
        logger.info ("\n \n ******* Final Summary for " + " " + getMigrationName() +" ********* \n");                
        for (Map.Entry<String, String> migratedNode: nodeTotalSuccess.entrySet()) {
        	logger.info("Total Migrated Records for " + migratedNode.getKey() +": " + migratedNode.getValue());
        	
        }
        logger.info(this.MIGRATION_SUMMARY_COUNT + "Total Migrated Records: "+ migrationSuccess);           
        
    }
    
    private void migrateValues (Vertex v, String property, Object newValue, String nodeType) throws Exception{
    	
    	if (v.property(property).isPresent() && !updateExistingValues) {
            String propertyValue = v.property(property).value().toString();
            if (propertyValue.isEmpty()) {
                v.property(property, newValue);
                logger.info(String.format("Node Type %s: Property %s is empty, adding value %s",
                        nodeType, property, newValue.toString()));
                this.touchVertexProperties(v, false);
                updateDmaapList(v);
                this.migrationSuccess++;
                this.subTotal++;
            } else {
                logger.info(String.format("Node Type %s: Property %s value already exists - skipping",
                        nodeType, property));
            }
        } else {
            logger.info(String.format("Node Type %s: Property %s does not exist or " +
                    "updateExistingValues flag is set to True - adding the property with value %s",
                    nodeType, property, newValue.toString()));
            v.property(property, newValue);
            this.touchVertexProperties(v, false);
            updateDmaapList(v);
            this.migrationSuccess++;
            this.subTotal++;
        }
    }
    
    private void checkConditions(Vertex v, String property, Object newValue, String nodeType) throws Exception{
    	
    	for (Map.Entry<String, List<?>> entry: conditionsMap.entrySet()){
    		String conditionType = entry.getKey();
    		List <?> conditionsValueList = conditionsMap.get(conditionType);
    		
    		if(v.property(conditionType).isPresent()){
    			for (int i = 0; i < conditionsValueList.size(); i++){			
        			if (v.property(conditionType).value().equals(conditionsValueList.get(i))){        				
        				migrateValues( v, property, newValue, nodeType);
        				break;
        			}
        		}    			
    		}    		
    	}    	
    }
    
    private void updateDmaapList(Vertex v){
    	String dmaapMsg = System.nanoTime() + "_" + v.id().toString() + "_"	+ v.value("resource-version").toString();
        dmaapMsgList.add(dmaapMsg);
        logger.info("\tAdding Updated Vertex " + v.id().toString() + " to dmaapMsgList....");
    }
    
    public boolean isUpdateDmaap(){
    	return false;
    }
}