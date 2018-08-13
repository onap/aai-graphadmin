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
package org.onap.aai.migration.v13;

import java.util.HashMap;
import java.util.Map;
import java.util.Optional;

import org.onap.aai.edges.EdgeIngestor;
import org.onap.aai.introspection.LoaderFactory;
import org.onap.aai.serialization.db.EdgeSerializer;
import org.onap.aai.serialization.engines.TransactionalGraphEngine;
import org.onap.aai.migration.Enabled;
import org.onap.aai.migration.MigrationDangerRating;
import org.onap.aai.migration.MigrationPriority;
import org.onap.aai.migration.Status;
import org.onap.aai.migration.ValueMigrator;
import org.onap.aai.setup.SchemaVersions;


@MigrationPriority(1)
@MigrationDangerRating(1)
public class MigrateBooleanDefaultsToFalse extends ValueMigrator {
	protected static final String VNF_NODE_TYPE = "generic-vnf";
	protected static final String VSERVER_NODE_TYPE = "vserver";
	protected static final String VNFC_NODE_TYPE = "vnfc";
	protected static final String L3NETWORK_NODE_TYPE = "l3-network";
	protected static final String SUBNET_NODE_TYPE = "subnet";
	protected static final String LINTERFACE_NODE_TYPE = "l-interface";
	protected static final String VFMODULE_NODE_TYPE = "vf-module";
	
	private static Map<String, Map> map;
    private static Map<String, Boolean> pair1;
    private static Map<String, Boolean> pair2;
    private static Map<String, Boolean> pair3;
    private static Map<String, Boolean> pair4;
    private static Map<String, Boolean> pair5;
    private static Map<String, Boolean> pair6;
 
	public MigrateBooleanDefaultsToFalse(TransactionalGraphEngine engine, LoaderFactory loaderFactory, EdgeIngestor edgeIngestor, EdgeSerializer edgeSerializer, SchemaVersions schemaVersions) {
		super(engine, loaderFactory, edgeIngestor, edgeSerializer, schemaVersions, setBooleanDefaultsToFalse(), false);
		
	}
	
	private static Map<String, Map> setBooleanDefaultsToFalse(){
		map = new HashMap<>();
        pair1 = new HashMap<>();
        pair2 = new HashMap<>();
        pair3 = new HashMap<>();
        pair4 = new HashMap<>();
        pair5 = new HashMap<>();
        pair6 = new HashMap<>();


		pair1.put("is-closed-loop-disabled", false);		
		map.put("generic-vnf", pair1);
		map.put("vnfc", pair1);
		map.put("vserver", pair1);
		
		pair2.put("is-bound-to-vpn", false);
		pair2.put("is-provider-network", false);
		pair2.put("is-shared-network", false);
		pair2.put("is-external-network", false);
		map.put("l3-network", pair2);
		
		pair3.put("dhcp-enabled", false);
		map.put("subnet", pair3);
		
		pair4.put("is-port-mirrored", false);
		pair4.put("is-ip-unnumbered", false);
		map.put("l-interface", pair4);
		
		pair5.put("is-base-vf-module", false);
		map.put("vf-module", pair5);
		
		pair6.put("is-ip-unnumbered", false);
		map.put("vlan", pair6);
        
        return map;
	}

	@Override
	public Status getStatus() {
		return Status.SUCCESS;
	}

	@Override
	public Optional<String[]> getAffectedNodeTypes() {
		return Optional.of(new String[]{VNF_NODE_TYPE,VSERVER_NODE_TYPE,VNFC_NODE_TYPE,L3NETWORK_NODE_TYPE,SUBNET_NODE_TYPE,LINTERFACE_NODE_TYPE,VFMODULE_NODE_TYPE});
	}

	@Override
	public String getMigrationName() {
		return "MigrateBooleanDefaultsToFalse";
	}

}