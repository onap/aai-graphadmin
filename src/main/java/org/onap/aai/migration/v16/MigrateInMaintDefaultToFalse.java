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
package org.onap.aai.migration.v16;

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


@MigrationPriority(20)
@MigrationDangerRating(2)
//@Enabled
public class MigrateInMaintDefaultToFalse extends ValueMigrator {
	
	protected static final String ZONE_NODE_TYPE = "zone";
	protected static final String CLOUD_REGION_NODE_TYPE = "cloud-region";
		
	private static Map<String, Map> map;
    private static Map<String, Boolean> pair;
 
	public MigrateInMaintDefaultToFalse(TransactionalGraphEngine engine, LoaderFactory loaderFactory, EdgeIngestor edgeIngestor, EdgeSerializer edgeSerializer, SchemaVersions schemaVersions) {
		super(engine, loaderFactory, edgeIngestor, edgeSerializer, schemaVersions, setInMaintToFalse(), false);
	}	
		
	private static Map<String, Map> setInMaintToFalse(){
		map = new HashMap<>();
        pair = new HashMap<>();

		pair.put("in-maint", false);
		
		map.put("zone", pair);
		map.put("cloud-region", pair);
		
        
        return map;
	}	

	@Override
	public Status getStatus() {
		return Status.SUCCESS;
	}

	@Override
	public Optional<String[]> getAffectedNodeTypes() {
		return Optional.of(new String[]{ZONE_NODE_TYPE,CLOUD_REGION_NODE_TYPE});
	}

	@Override
	public String getMigrationName() {
		return "MigrateInMaintDefaultToFalse";
	}

}