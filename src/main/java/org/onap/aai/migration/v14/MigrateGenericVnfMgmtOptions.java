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

import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Optional;

import org.onap.aai.edges.EdgeIngestor;
import org.onap.aai.introspection.LoaderFactory;
import org.onap.aai.migration.MigrationDangerRating;
import org.onap.aai.migration.MigrationPriority;
import org.onap.aai.migration.Status;
import org.onap.aai.migration.ValueMigrator;
import org.onap.aai.migration.Enabled;
import org.onap.aai.serialization.db.EdgeSerializer;
import org.onap.aai.serialization.engines.TransactionalGraphEngine;
import org.onap.aai.setup.SchemaVersions;


@MigrationPriority(1)
@MigrationDangerRating(1)
//@Enabled
public class MigrateGenericVnfMgmtOptions extends ValueMigrator {

	protected static final String VNF_NODE_TYPE = "generic-vnf";
	
	
	private static Map<String, Map> map;
    private static Map<String, String> pair1;
    private static Map<String, List<String>> conditionsMap;
    
	public MigrateGenericVnfMgmtOptions(TransactionalGraphEngine engine, LoaderFactory loaderFactory, EdgeIngestor edgeIngestor, EdgeSerializer edgeSerializer, SchemaVersions schemaVersions) {
		super(engine, loaderFactory, edgeIngestor, edgeSerializer, schemaVersions, setMgmtOptions(), setConditionsMap(), false);
		
	}
	
	private static Map<String, Map> setMgmtOptions(){
		map = new HashMap<>();
        pair1 = new HashMap<>();      

		pair1.put("management-option", "AT&T Managed-Basic");		
		map.put("generic-vnf", pair1);
		
        return map;
	}
	
	

	public static Map<String, List<String>> setConditionsMap() {
		List<String> conditionsList = new ArrayList<String>();
		conditionsMap = new HashMap<>();
        
		conditionsList.add("HN");
        conditionsList.add("HP");
        conditionsList.add("HG");
        
        conditionsMap.put("vnf-type", conditionsList);
        
        return conditionsMap;
	}

	@Override
	public Status getStatus() {
		return Status.SUCCESS;
	}

	@Override
	public Optional<String[]> getAffectedNodeTypes() {
		return Optional.of(new String[]{VNF_NODE_TYPE});
	}

	@Override
	public String getMigrationName() {
		return "MigrateGenericVnfMgmtOptions";
	}
	
	@Override
	public boolean isUpdateDmaap(){
		return true;
	}
	

}
