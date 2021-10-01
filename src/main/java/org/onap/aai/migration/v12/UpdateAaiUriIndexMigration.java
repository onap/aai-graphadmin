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

import org.janusgraph.core.Cardinality;
import org.janusgraph.core.PropertyKey;
import org.janusgraph.core.schema.SchemaAction;
import org.janusgraph.core.schema.SchemaStatus;
import org.janusgraph.core.schema.JanusGraphIndex;
import org.janusgraph.core.schema.JanusGraphManagement;
import org.janusgraph.graphdb.database.management.ManagementSystem;
import org.apache.tinkerpop.gremlin.process.traversal.dsl.graph.GraphTraversalSource;
import org.apache.tinkerpop.gremlin.structure.Vertex;
import org.onap.aai.db.props.AAIProperties;
import org.onap.aai.dbmap.AAIGraph;
import org.onap.aai.edges.EdgeIngestor;
import org.onap.aai.exceptions.AAIException;
import org.onap.aai.introspection.LoaderFactory;
import org.onap.aai.introspection.ModelType;
import org.onap.aai.serialization.db.EdgeSerializer;
import org.onap.aai.migration.*;
import org.onap.aai.serialization.engines.TransactionalGraphEngine;
import org.onap.aai.setup.SchemaVersion;
import org.onap.aai.setup.SchemaVersions;

import java.time.temporal.ChronoUnit;
import java.util.*;

/**
 * Remove old aai-uri index per
 * https://github.com/JanusGraph/janusgraph/wiki/Indexing
 */

//@Enabled

@MigrationPriority(500)
@MigrationDangerRating(1000)
public class UpdateAaiUriIndexMigration extends Migrator {

	private final SchemaVersion version;
	private final ModelType introspectorFactoryType;
	private GraphTraversalSource g;
	private JanusGraphManagement graphMgmt;
	private Status status = Status.SUCCESS;

	private String retiredName = AAIProperties.AAI_URI + "-RETIRED-" + System.currentTimeMillis();

	/**
	 * Instantiates a new migrator.
	 *
	 * @param engine
	 */
	public UpdateAaiUriIndexMigration(TransactionalGraphEngine engine, LoaderFactory loaderFactory, EdgeIngestor edgeIngestor, EdgeSerializer edgeSerializer, SchemaVersions schemaVersions) throws AAIException {
		super(engine, loaderFactory, edgeIngestor, edgeSerializer, schemaVersions);
		version = schemaVersions.getDefaultVersion();
		introspectorFactoryType = ModelType.MOXY;
		loader = loaderFactory.createLoaderForVersion(introspectorFactoryType, version);
		g = this.engine.asAdmin().getTraversalSource();
		this.engine.rollback();
		graphMgmt = engine.asAdmin().getManagementSystem();

	}

	@Override
	public Status getStatus() {
		return status;
	}

	@Override
	public Optional<String[]> getAffectedNodeTypes() {
		return Optional.empty();
	}

	@Override
	public String getMigrationName() {
		return "UpdateAaiUriIndex";
	}

	@Override
	public void run() {

		// close all but current open titan instances
		closeAllButCurrentInstances();

		// get all indexes containing aai-uri
		Set<IndexDetails> indexes = getIndexesWithAaiUri();
		logger.info("Found " + indexes.size() + " aai uri index.");
		indexes.stream().map(s -> "\t" + s.getIndexName() + " : " + s.getPropertyName() + " : " + s.getStatus() ).forEach(System.out::println);

		renameAaiUriIndex(indexes);

		// remove all of the aai-uri indexes that are in the list
		removeIndexes(indexes);

		//retire old property
		verifyGraphManagementIsOpen();
		PropertyKey aaiUri = graphMgmt.getPropertyKey(AAIProperties.AAI_URI);
		if (aaiUri != null) {
			graphMgmt.changeName(aaiUri, retiredName);
		}
		graphMgmt.commit();

		//remove all aai uri keys
		logger.info("Remove old keys.");
		dropAllKeyProperties(indexes);

		// add aai-uri unique index
		logger.info("Create new unique aai-uri index");
		createUniqueAaiUriIndex();


		// change index status to ENABLED STATE
		logger.info("Enable index");
		enableIndex();

		this.engine.startTransaction();

		logger.info("Checking and dropping retired properties.");
		g = this.engine.asAdmin().getTraversalSource();
		g.V().has(retiredName).properties(retiredName).drop().iterate();
		logger.info("Done.");
	}


	protected void createUniqueAaiUriIndex() {
		verifyGraphManagementIsOpen();
		// create new aaiuri property
		PropertyKey aaiUriProperty = graphMgmt.getPropertyKey(AAIProperties.AAI_URI);
		if (aaiUriProperty == null) {
			logger.info("Creating new aai-uri property.");
			aaiUriProperty = graphMgmt.makePropertyKey(AAIProperties.AAI_URI).dataType(String.class)
					.cardinality(Cardinality.SINGLE).make();
		}
		logger.info("Creating new aai-uri index.");
		graphMgmt.buildIndex(AAIProperties.AAI_URI, Vertex.class).addKey(aaiUriProperty).unique().buildCompositeIndex();
		graphMgmt.commit();
	}

	private void dropAllKeyProperties(Set<IndexDetails> indexes) {
		indexes.stream().map(e -> e.getPropertyName()).distinct().forEach(p -> {
			verifyGraphManagementIsOpen();
			if (graphMgmt.getPropertyKey(p) != null) {
				graphMgmt.getPropertyKey(p).remove();
			}
			graphMgmt.commit();
		});
	}

	private void renameAaiUriIndex(Set<IndexDetails> indexes) {
		verifyGraphManagementIsOpen();
		indexes.stream().filter(s -> s.getIndexName().equals(AAIProperties.AAI_URI)).forEach( s -> {
			JanusGraphIndex index = graphMgmt.getGraphIndex(s.getIndexName());
			graphMgmt.changeName(index, retiredName);
			s.setIndexName(retiredName);
		});
		graphMgmt.commit();
	}

	private void removeIndexes(Set<IndexDetails> indexes) {

		for (IndexDetails index : indexes) {
			verifyGraphManagementIsOpen();

			JanusGraphIndex aaiUriIndex = graphMgmt.getGraphIndex(index.getIndexName());

			if (!index.getStatus().equals(SchemaStatus.DISABLED)) {
				logger.info("Disabling index: " + index.getIndexName());
				logger.info("\tCurrent state: " + aaiUriIndex.getIndexStatus(graphMgmt.getPropertyKey(index.getPropertyName())));

				graphMgmt.updateIndex(aaiUriIndex, SchemaAction.DISABLE_INDEX);
				graphMgmt.commit();
				try {
					ManagementSystem.awaitGraphIndexStatus(AAIGraph.getInstance().getGraph(), index.getIndexName())
							.timeout(10, ChronoUnit.MINUTES)
							.status(SchemaStatus.DISABLED)
							.call();
				} catch (Exception e) {
					logger.info("AwaitGraphIndexStatus error: " + e.getMessage());
				}
			}

			verifyGraphManagementIsOpen();
			aaiUriIndex = graphMgmt.getGraphIndex(index.getIndexName());
			if (aaiUriIndex.getIndexStatus(graphMgmt.getPropertyKey(index.getPropertyName())).equals(SchemaStatus.DISABLED)) {
				logger.info("Removing index: " + index.getIndexName());
				graphMgmt.updateIndex(aaiUriIndex, SchemaAction.REMOVE_INDEX);
				graphMgmt.commit();
			}
			if(graphMgmt.isOpen()) {
				graphMgmt.commit();
			}
		}

	}

	protected Set<IndexDetails> getIndexesWithAaiUri() {
		verifyGraphManagementIsOpen();
		Set<IndexDetails> aaiUriIndexName = new HashSet<>();

		Iterator<JanusGraphIndex> titanIndexes = graphMgmt.getGraphIndexes(Vertex.class).iterator();
		JanusGraphIndex titanIndex;
		while (titanIndexes.hasNext()) {
			titanIndex = titanIndexes.next();
			if (titanIndex.name().contains(AAIProperties.AAI_URI) && titanIndex.getFieldKeys().length > 0) {
				logger.info("Found aai-uri index: " + titanIndex.name());
				aaiUriIndexName.add(new IndexDetails(titanIndex.name(), titanIndex.getIndexStatus(titanIndex.getFieldKeys()[0]), titanIndex.getFieldKeys()[0].name()));
			}
		}
		graphMgmt.rollback();
		return aaiUriIndexName;
	}

	private void closeAllButCurrentInstances() {
		verifyGraphManagementIsOpen();
		logger.info("Closing all but current titan instances.");
		graphMgmt.getOpenInstances().stream().filter(s -> !s.contains("(current)")).forEach(s -> {
			logger.info("\t"+s);
			graphMgmt.forceCloseInstance(s);
		});
		graphMgmt.commit();
	}


	private void verifyGraphManagementIsOpen() {
		if (!graphMgmt.isOpen()) {
			graphMgmt = this.engine.asAdmin().getManagementSystem();
		}
	}

	private void enableIndex() {
		verifyGraphManagementIsOpen();
		JanusGraphIndex aaiUriIndex = graphMgmt.getGraphIndex(AAIProperties.AAI_URI);
		SchemaStatus schemaStatus = aaiUriIndex.getIndexStatus(graphMgmt.getPropertyKey(AAIProperties.AAI_URI));
		if (schemaStatus.equals(SchemaStatus.INSTALLED)) {
			logger.info("Registering index: " + AAIProperties.AAI_URI);
			logger.info("\tCurrent state: " + schemaStatus);

			graphMgmt.updateIndex(aaiUriIndex, SchemaAction.REGISTER_INDEX);
			graphMgmt.commit();
			try {
				ManagementSystem.awaitGraphIndexStatus(AAIGraph.getInstance().getGraph(), AAIProperties.AAI_URI)
						.timeout(10, ChronoUnit.MINUTES)
						.status(SchemaStatus.REGISTERED)
						.call();
			} catch (Exception e) {
				logger.info("AwaitGraphIndexStatus error: " + e.getMessage());
			}
		}

		verifyGraphManagementIsOpen();
		aaiUriIndex = graphMgmt.getGraphIndex(AAIProperties.AAI_URI);
		schemaStatus = aaiUriIndex.getIndexStatus(graphMgmt.getPropertyKey(AAIProperties.AAI_URI));
		if (schemaStatus.equals(SchemaStatus.REGISTERED)) {
			logger.info("Enabling index: " + AAIProperties.AAI_URI);
			logger.info("\tCurrent state: " + schemaStatus);

			graphMgmt.updateIndex(aaiUriIndex, SchemaAction.ENABLE_INDEX);
			graphMgmt.commit();
			try {
				ManagementSystem.awaitGraphIndexStatus(AAIGraph.getInstance().getGraph(), AAIProperties.AAI_URI)
						.timeout(10, ChronoUnit.MINUTES)
						.status(SchemaStatus.ENABLED)
						.call();
			} catch (Exception e) {
				logger.info("AwaitGraphIndexStatus error: " + e.getMessage());
			}
		}

		verifyGraphManagementIsOpen();
		aaiUriIndex = graphMgmt.getGraphIndex(AAIProperties.AAI_URI);
		schemaStatus = aaiUriIndex.getIndexStatus(graphMgmt.getPropertyKey(AAIProperties.AAI_URI));
		logger.info("Final state: " + schemaStatus);
		graphMgmt.rollback();
	}

	private class IndexDetails {
		private String indexName;
		private SchemaStatus status;
		private String propertyName;

		public IndexDetails(String indexName, SchemaStatus status, String propertyName) {
			this.indexName = indexName;
			this.status = status;
			this.propertyName = propertyName;
		}

		public String getIndexName() {
			return indexName;
		}

		public SchemaStatus getStatus() {
			return status;
		}

		public String getPropertyName() {
			return propertyName;
		}

		public void setIndexName(String indexName) {
			this.indexName = indexName;
		}

		public void setStatus(SchemaStatus status) {
			this.status = status;
		}

		public void setPropertyName(String propertyName) {
			this.propertyName = propertyName;
		}
	}
}
