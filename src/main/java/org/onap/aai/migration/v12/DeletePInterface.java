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

import java.util.Collection;
import java.util.List;
import java.util.Optional;

import org.apache.tinkerpop.gremlin.process.traversal.dsl.graph.GraphTraversal;
import org.apache.tinkerpop.gremlin.process.traversal.dsl.graph.GraphTraversalSource;
import org.apache.tinkerpop.gremlin.structure.Vertex;
import org.onap.aai.db.props.AAIProperties;
import org.onap.aai.edges.EdgeIngestor;
import org.onap.aai.exceptions.AAIException;
import org.onap.aai.introspection.LoaderFactory;
import org.onap.aai.migration.MigrationDangerRating;
import org.onap.aai.migration.MigrationPriority;
import org.onap.aai.migration.Migrator;
import org.onap.aai.migration.Status;
import org.onap.aai.edges.enums.EdgeType;
import org.onap.aai.serialization.db.EdgeSerializer;
import org.onap.aai.serialization.engines.TransactionalGraphEngine;
import org.onap.aai.setup.SchemaVersions;

@MigrationPriority(0)
@MigrationDangerRating(0)
public class DeletePInterface extends Migrator {
	private boolean success = true;
	private final GraphTraversalSource g;
	public DeletePInterface(TransactionalGraphEngine engine, LoaderFactory loaderFactory, EdgeIngestor edgeIngestor, EdgeSerializer edgeSerializer, SchemaVersions schemaVersions) {
		super(engine, loaderFactory, edgeIngestor, edgeSerializer, schemaVersions);
		this.g = this.engine.asAdmin().getTraversalSource();
	}

	@Override
	public void run() {
		int count = 0;
		int skipCount = 0;
		int errorCount = 0;
		logger.info("---------- Start deleting p-interfaces  ----------");
		List<Vertex> pIntfList;
		try {
			pIntfList = g.V().has(AAIProperties.NODE_TYPE, "p-interface").has("source-of-truth", "AAI-CSVP-INSTARAMS")
					.where(this.engine.getQueryBuilder().createEdgeTraversal(EdgeType.TREE, "p-interface", "pnf")
					.<GraphTraversal<?, ?>>getQuery()).toList();
			
			if (pIntfList != null && !pIntfList.isEmpty()) {
				for (Vertex pInterfV : pIntfList) {
					try {
						Collection<Vertex> cousins = this.engine.getQueryEngine().findCousinVertices(pInterfV);
						
						Collection<Vertex> children = this.engine.getQueryEngine().findChildren(pInterfV);
						if (cousins == null || cousins.isEmpty()) {
							if (children == null || children.isEmpty()) {
								if(null!=pInterfV) {
									logger.info("Delete p-interface: " + getVertexURI(pInterfV));
									pInterfV.remove();
								}
								count++;
							} else {
								skipCount++;
								logger.info("skip p-interface " + getVertexURI(pInterfV) + " due to an existing relationship");
							}
						} else {
							skipCount++;
							logger.info("skip p-interface " + getVertexURI(pInterfV) + " due to an existing relationship");
						}
					} catch (Exception e) {
						success = false;
						errorCount++;
						logger.error("error occured in deleting p-interface " + getVertexURI(pInterfV) + ", "+ e);
					}
				}
		        logger.info ("\n \n ******* Final Summary for deleting p-interfaces Migration ********* \n");
		        logger.info("Number of p-interfaces removed: "+ count +"\n");
		        logger.info("Number of p-interfaces skipped: "+ skipCount  +"\n");
		        logger.info("Number of p-interfaces failed to delete due to error : "+ errorCount  +"\n");
			}
		} catch (AAIException e) {
			success = false;
			logger.error("error occured in deleting p-interfaces " + e);
		}
	}
	
	private String getVertexURI(Vertex v) {
		if (v != null) {
	    	if (v.property("aai-uri").isPresent()) {
	    		return v.property("aai-uri").value().toString();
			} else {
				return "Vertex ID: " + v.id().toString();
			}
		} else {
			return "";
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
		return Optional.of(new String[] { "p-interface" });
	}

	@Override
	public String getMigrationName() {
		return "DeletePInterface";
	}

}
