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
package org.onap.aai.migration.v20;

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


@MigrationPriority(20)
@MigrationDangerRating(2)
@Enabled
public class MigrateL2DefaultToFalse extends ValueMigrator {
	
	protected static final String L_INTERFACE_NODE_TYPE = "l-interface";
	protected static final String L2_MULTI_PROPERTY = "l2-multicasting";
		
	private static Map<String, Map<String, Boolean>> map;
    private static Map<String, Boolean> pair;
 
	public MigrateL2DefaultToFalse(TransactionalGraphEngine engine, LoaderFactory loaderFactory, EdgeIngestor edgeIngestor, EdgeSerializer edgeSerializer, SchemaVersions schemaVersions) {
		super(engine, loaderFactory, edgeIngestor, edgeSerializer, schemaVersions, setL2ToFalse(), false);
	}	
		
	private static Map<String, Map<String, Boolean>> setL2ToFalse(){
		map = new HashMap<>();
        pair = new HashMap<>();

		pair.put(L2_MULTI_PROPERTY, false);
		
		map.put(L_INTERFACE_NODE_TYPE, pair);		
        
        return map;
	}	

	@Override
	public Status getStatus() {
		return Status.SUCCESS;
	}

	@Override
	public Optional<String[]> getAffectedNodeTypes() {
		return Optional.of(new String[]{L_INTERFACE_NODE_TYPE});
	}

	@Override
	public String getMigrationName() {
		return "MigrateL2DefaultToFalse";
	}

}