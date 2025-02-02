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

import java.util.HashMap;
import java.util.Map;
import java.util.Optional;

import org.onap.aai.edges.EdgeIngestor;
import org.onap.aai.introspection.LoaderFactory;
import org.onap.aai.serialization.db.EdgeSerializer;
import org.onap.aai.serialization.engines.TransactionalGraphEngine;
import org.onap.aai.migration.MigrationDangerRating;
import org.onap.aai.migration.MigrationPriority;
import org.onap.aai.migration.Status;
import org.onap.aai.migration.ValueMigrator;
import org.onap.aai.setup.SchemaVersions;


@MigrationPriority(1)
@MigrationDangerRating(1)
public class MigrateInMaintDefaultToFalse extends ValueMigrator {
	
	protected static final String VNF_NODE_TYPE = "generic-vnf";
	protected static final String LINTERFACE_NODE_TYPE = "l-interface";
	protected static final String LAG_INTERFACE_NODE_TYPE = "lag-interface";
	protected static final String LOGICAL_LINK_NODE_TYPE = "logical-link";
	protected static final String PINTERFACE_NODE_TYPE = "p-interface";
	protected static final String VLAN_NODE_TYPE = "vlan";
	protected static final String VNFC_NODE_TYPE = "vnfc";
	protected static final String VSERVER_NODE_TYPE = "vserver";
	protected static final String PSERVER_NODE_TYPE = "pserver";
	protected static final String PNF_NODE_TYPE = "pnf";
	protected static final String NOS_SERVER_NODE_TYPE = "nos-server";
		
	private static Map<String, Map> map;
    private static Map<String, Boolean> pair;
 
	public MigrateInMaintDefaultToFalse(TransactionalGraphEngine engine, LoaderFactory loaderFactory, EdgeIngestor edgeIngestor, EdgeSerializer edgeSerializer, SchemaVersions schemaVersions) {
		super(engine, loaderFactory, edgeIngestor, edgeSerializer, schemaVersions, setInMaintToFalse(), false);
	}	
		
	private static Map<String, Map> setInMaintToFalse(){
		map = new HashMap<>();
        pair = new HashMap<>();

		pair.put("in-maint", false);
		
		map.put("generic-vnf", pair);
		map.put("l-interface", pair);
		map.put("lag-interface", pair);
		map.put("logical-link", pair);
		map.put("p-interface", pair);
		map.put("vlan", pair);
		map.put("vnfc", pair);
		map.put("vserver", pair);
		map.put("pserver", pair);
        map.put("pnf", pair);
        map.put("nos-server", pair);
        
        return map;
	}	

	@Override
	public Status getStatus() {
		return Status.SUCCESS;
	}

	@Override
	public Optional<String[]> getAffectedNodeTypes() {
		return Optional.of(new String[]{VNF_NODE_TYPE,LINTERFACE_NODE_TYPE,LAG_INTERFACE_NODE_TYPE,LOGICAL_LINK_NODE_TYPE,PINTERFACE_NODE_TYPE,VLAN_NODE_TYPE,VNFC_NODE_TYPE,VSERVER_NODE_TYPE,PSERVER_NODE_TYPE,PNF_NODE_TYPE,NOS_SERVER_NODE_TYPE});
	}

	@Override
	public String getMigrationName() {
		return "MigrateInMaintDefaultToFalse";
	}

}