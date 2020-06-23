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
import java.util.List;
import java.util.Optional;

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

@MigrationPriority(20)
@MigrationDangerRating(2)
//@Enabled
public class MigratePserverAndPnfEquipType extends Migrator{

    protected static final String EQUIP_TYPE_PROPERTY = "equip-type";
    protected static final String HOSTNAME_PROPERTY = "hostname";
    protected static final String PNF_NAME_PROPERTY = "pnf-name";
    protected static final String PNF_NODE_TYPE = "pnf";
    protected static final String PSERVER_NODE_TYPE = "pserver";
	private boolean success = true;

    public MigratePserverAndPnfEquipType(TransactionalGraphEngine engine, LoaderFactory loaderFactory, EdgeIngestor edgeIngestor, EdgeSerializer edgeSerializer, SchemaVersions schemaVersions) {
        super(engine, loaderFactory, edgeIngestor, edgeSerializer, schemaVersions);
    }



    @Override
    public void run() {
    	int pserverCount = 0;
    	int pnfCount = 0;
		int pserverErrorCount = 0;
		int pnfErrorCount  = 0;
		logger.info("---------- Start Updating equip-type for Pserver and Pnf  ----------");

    	List<Vertex> pserverList = this.engine.asAdmin().getTraversalSource().V().has(AAIProperties.NODE_TYPE, PSERVER_NODE_TYPE).toList();
    	List<Vertex> pnfList = this.engine.asAdmin().getTraversalSource().V().has(AAIProperties.NODE_TYPE, PNF_NODE_TYPE).toList();

    	for (Vertex vertex : pserverList) {
    		String currentValueOfEquipType = null;
    		String hostName = null;
    		try {
    			currentValueOfEquipType = getEquipTypeNodeValue(vertex);
    			hostName = getHostNameNodeValue(vertex);
    			if("Server".equals(currentValueOfEquipType) ||"server".equals(currentValueOfEquipType) ){
    				if(vertex != null) vertex.property(EQUIP_TYPE_PROPERTY, "SERVER");
    				this.touchVertexProperties(vertex, false);
    				logger.info("changed Pserver equip-type from " + currentValueOfEquipType + " to SERVER having hostname : " + hostName);
    				pserverCount++;
    			}
    		} catch (Exception e) {
    			success = false;
    			pserverErrorCount++;
    			logger.error(MIGRATION_ERROR + "encountered exception for equip-type:" + currentValueOfEquipType + " having hostName :" + hostName, e);
    		}
    	}
        
    	for (Vertex vertex : pnfList) {
    		String currentValueOfEquipType = null;
    		String pnfName = null;
    		try {
    			currentValueOfEquipType = getEquipTypeNodeValue(vertex);
    			pnfName = getPnfNameNodeValue(vertex);
    			if("Switch".equals(currentValueOfEquipType)||"switch".equals(currentValueOfEquipType)){
    				if(vertex != null) vertex.property(EQUIP_TYPE_PROPERTY, "SWITCH");
    				this.touchVertexProperties(vertex, false);
    				logger.info("changed Pnf equip-type from "+ currentValueOfEquipType +" to SWITCH having pnf-name :" + pnfName);
    				pnfCount++;
    			}

    		} catch (Exception e) {
    			success = false;
    			pnfErrorCount++;
    			logger.error(MIGRATION_ERROR + "encountered exception for equip-type:" + currentValueOfEquipType +" having pnf-name : "+ pnfName , e);
    		}
    	}
    	
    	logger.info ("\n \n ******* Final Summary Updated equip-type for Pserver and Pnf  Migration ********* \n");
        logger.info(MIGRATION_SUMMARY_COUNT+"Number of Pservers updated: "+ pserverCount +"\n");
        logger.info(MIGRATION_SUMMARY_COUNT+"Number of Pservers failed to update due to error : "+ pserverErrorCount  +"\n");
        
        logger.info(MIGRATION_SUMMARY_COUNT+"Number of Pnf updated: "+ pnfCount +"\n");
        logger.info(MIGRATION_SUMMARY_COUNT+"Number of Pnf failed to update due to error : "+ pnfErrorCount  +"\n");

    }

	private String getEquipTypeNodeValue(Vertex vertex) {
		String propertyValue = "";
		if(vertex != null && vertex.property(EQUIP_TYPE_PROPERTY).isPresent()){
			propertyValue = vertex.property(EQUIP_TYPE_PROPERTY).value().toString();
		}
		return propertyValue;
	}
	
	private String getHostNameNodeValue(Vertex vertex) {
		String propertyValue = "";
		if(vertex != null && vertex.property(HOSTNAME_PROPERTY).isPresent()){
			propertyValue = vertex.property(HOSTNAME_PROPERTY).value().toString();
		}
		return propertyValue;
	}
	
	private String getPnfNameNodeValue(Vertex vertex) {
		String propertyValue = "";
		if(vertex != null && vertex.property(PNF_NAME_PROPERTY).isPresent()){
			propertyValue = vertex.property(PNF_NAME_PROPERTY).value().toString();
		}
		return propertyValue;
	}
    
    @Override
    public Status getStatus() {
        if (success) {
            return Status.SUCCESS;
        } else {
            return Status.FAILURE;
        }
    }
    
    @Override
    public Optional<String[]> getAffectedNodeTypes() {
    	return Optional.of(new String[]{PSERVER_NODE_TYPE,PNF_NODE_TYPE});
    }

    @Override
    public String getMigrationName() {
        return "MigratePserverAndPnfEquipType";
    }

}
