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
package org.onap.aai.migration.v12;

import java.util.Optional;

import org.apache.commons.lang3.exception.ExceptionUtils;
import org.apache.tinkerpop.gremlin.structure.Edge;
import org.onap.aai.edges.EdgeIngestor;
import org.onap.aai.edges.enums.EdgeProperty;
import org.onap.aai.introspection.LoaderFactory;
import org.onap.aai.migration.MigrationDangerRating;
import org.onap.aai.migration.MigrationPriority;
import org.onap.aai.migration.Migrator;
import org.onap.aai.migration.Status;
import org.onap.aai.edges.enums.AAIDirection;
import org.onap.aai.serialization.db.EdgeSerializer;
import org.onap.aai.serialization.engines.TransactionalGraphEngine;
import org.onap.aai.setup.SchemaVersions;


//@Enabled
@MigrationPriority(-100)
@MigrationDangerRating(10)
public class ContainmentDeleteOtherVPropertyMigration extends Migrator {

	private boolean success = true;
	
	public ContainmentDeleteOtherVPropertyMigration(TransactionalGraphEngine engine, LoaderFactory loaderFactory, EdgeIngestor edgeIngestor, EdgeSerializer edgeSerializer, SchemaVersions schemaVersions) {
		super(engine, loaderFactory, edgeIngestor, edgeSerializer, schemaVersions);
	}
	
	//just for testing using test edge rule files
	public ContainmentDeleteOtherVPropertyMigration(TransactionalGraphEngine engine, LoaderFactory loaderFactory, EdgeIngestor edgeIngestor, EdgeSerializer edgeSerializer, SchemaVersions schemaVersions, String edgeRulesFile) {
		super(engine, loaderFactory, edgeIngestor, edgeSerializer, schemaVersions);
	}
	
	@Override
	public void run() {
		try {
			engine.asAdmin().getTraversalSource().E().sideEffect(t -> {
				Edge e = t.get();
				logger.info("out vertex: " + e.outVertex().property("aai-node-type").value() + 
								" in vertex: " + e.inVertex().property("aai-node-type").value() +
								" label : " + e.label());
				if (e.property(EdgeProperty.CONTAINS.toString()).isPresent() &&
						e.property(EdgeProperty.DELETE_OTHER_V.toString()).isPresent()) {
					//in case of orphans
					if (!("constrained-element-set".equals(e.inVertex().property("aai-node-type").value())
							&& "model-element".equals(e.outVertex().property("aai-node-type").value()))) {
						//skip the weird horrible problem child edge
						String containment = (String) e.property(EdgeProperty.CONTAINS.toString()).value();
						if (AAIDirection.OUT.toString().equalsIgnoreCase(containment) ||
								AAIDirection.IN.toString().equalsIgnoreCase(containment) ||
								AAIDirection.BOTH.toString().equalsIgnoreCase(containment)) {
							logger.info("updating delete-other-v property");
							e.property(EdgeProperty.DELETE_OTHER_V.toString(), containment);
						}
					}
				}
			}).iterate();
		} catch (Exception e) {
			logger.info("error encountered " + e.getClass() + " " + e.getMessage() + " " + ExceptionUtils.getFullStackTrace(e));
			logger.error("error encountered " + e.getClass() + " " + e.getMessage() + " " + ExceptionUtils.getFullStackTrace(e));
			success = false;
		}
		
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
		return Optional.empty();
	}

	@Override
	public String getMigrationName() {
		return "migrate-containment-delete-other-v";
	}

}
