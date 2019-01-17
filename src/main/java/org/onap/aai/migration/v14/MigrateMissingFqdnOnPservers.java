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

import java.util.List;
import java.util.Optional;
import java.util.concurrent.atomic.AtomicInteger;

import org.apache.tinkerpop.gremlin.process.traversal.dsl.graph.GraphTraversal;
import org.apache.tinkerpop.gremlin.process.traversal.dsl.graph.GraphTraversalSource;
import org.apache.tinkerpop.gremlin.process.traversal.dsl.graph.__;
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
@Enabled
public class MigrateMissingFqdnOnPservers extends Migrator {

	protected static final String PSERVER_NODE_TYPE = "pserver";
	protected static final String PSERVER_FQDN = "fqdn";
	protected static final String PSERVER_HOSTNAME = "hostname";
	protected static final String PSERVER_SOURCEOFTRUTH = "source-of-truth";
	
	private boolean success = true;
	private GraphTraversalSource g = null;
	
	protected final AtomicInteger falloutRowsCount = new AtomicInteger(0);

	public MigrateMissingFqdnOnPservers(TransactionalGraphEngine engine, LoaderFactory loaderFactory,
			EdgeIngestor edgeIngestor, EdgeSerializer edgeSerializer, SchemaVersions schemaVersions) {
		super(engine, loaderFactory, edgeIngestor, edgeSerializer, schemaVersions);
	}

	@Override
	public void run() {
		logger.info("---------- Start Updating fqdn for pserver  ----------");

		try {
			int pserverCount = 0;
			int pserverUpdatedCount = 0;
			int pserverSkippedCount = 0;
			int pserverErrorCount = 0;
			int pserverWithMissingSOTCount = 0;

			GraphTraversal<Vertex, Vertex> pserverList = this.engine.asAdmin().getTraversalSource().V()
					.has(AAIProperties.NODE_TYPE, PSERVER_NODE_TYPE).union(__.hasNot(PSERVER_FQDN),__.has(PSERVER_FQDN,""));//gets list of pservers with missing and empty fqdn
			
			 while (pserverList.hasNext()) {
				pserverCount++;
		        Vertex vertex = pserverList.next();
				String hostname = null;
				String sourceOfTruth = null;
				hostname = vertex.property(PSERVER_HOSTNAME).value().toString();
				
				if(vertex.property(PSERVER_SOURCEOFTRUTH).isPresent()) {
					sourceOfTruth = vertex.property(PSERVER_SOURCEOFTRUTH).value().toString();
				}else {
					logger.info("Missing source of truth for hostname : " + hostname);
					pserverWithMissingSOTCount++;
				}
				
				if (!hostname.contains(".")) {
					logger.info("Invalid format hostname :" + hostname + " and its source of truth is : " + sourceOfTruth);
					pserverSkippedCount++;
					continue;
				}

				try {
					vertex.property(PSERVER_FQDN, hostname);
					this.touchVertexProperties(vertex, false);
					logger.info("Updated fqdn from hostname : " + hostname + " and its source of truth is : " + sourceOfTruth);
					pserverUpdatedCount++;
				} catch (Exception e) {
					success = false;
					pserverErrorCount++;
					logger.error(MIGRATION_ERROR + "encountered exception for fqdn update for pserver with hostname :" + hostname
							+ " and source of truth : " + sourceOfTruth, e);
				}
			}
			
			logger.info("\n \n ******* Final Summary of Updated fqdn for pserver  Migration ********* \n");
			logger.info(MIGRATION_SUMMARY_COUNT + "Total Number of pservers with missing or empty fqdn : "+pserverCount + "\n");
			logger.info(MIGRATION_SUMMARY_COUNT + "Number of pservers updated: " + pserverUpdatedCount + "\n");
			logger.info(MIGRATION_SUMMARY_COUNT + "Number of pservers invalid: " + pserverSkippedCount + "\n");
			logger.info(MIGRATION_SUMMARY_COUNT + "Number of pservers failed to update due to error : " + pserverErrorCount + "\n");
			logger.info(MIGRATION_SUMMARY_COUNT + "Number of pservers with missing source of truth: " + pserverWithMissingSOTCount + "\n");
			
		} catch (Exception e) {
			logger.info("encountered exception", e);
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
		return Optional.of(new String[] { PSERVER_NODE_TYPE });
	}

	@Override
	public String getMigrationName() {
		return "MigrateMissingFqdnOnPserver";
	}

}