package org.onap.aai.migration.v12;
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


import org.apache.tinkerpop.gremlin.process.traversal.dsl.graph.GraphTraversalSource;
import org.apache.tinkerpop.gremlin.structure.Edge;
import org.apache.tinkerpop.gremlin.structure.Vertex;
import org.onap.aai.edges.EdgeIngestor;
import org.onap.aai.introspection.LoaderFactory;
import org.onap.aai.migration.*;
import org.onap.aai.serialization.db.EdgeSerializer;
import org.onap.aai.serialization.engines.TransactionalGraphEngine;
import org.onap.aai.setup.SchemaVersions;

import java.util.*;

@MigrationPriority(0)
@MigrationDangerRating(0)
public class EdgeReportForToscaMigration extends Migrator {

    private boolean success = true;

    public EdgeReportForToscaMigration(TransactionalGraphEngine engine, LoaderFactory loaderFactory, EdgeIngestor edgeIngestor, EdgeSerializer edgeSerializer, SchemaVersions schemaVersions){
		super(engine, loaderFactory, edgeIngestor, edgeSerializer, schemaVersions);
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
    public void run() {
		Vertex out = null;
		Vertex in = null;
		String label = "";
		String outURI = "";
		String inURI = "";
		String parentCousinIndicator = "NONE";
		String oldEdgeString = null;
		List<String> edgeMissingParentProperty = new ArrayList<>();
		StringBuilder sb = new StringBuilder();
		Set<String> noURI = new HashSet<>();
		sb.append("----------EDGES----------\n");

		GraphTraversalSource g = engine.asAdmin().getTraversalSource();

    	try {
			Set<Edge> edges = g.E().toSet();
        	for (Edge edge : edges) {
				out = edge.outVertex();
				in = edge.inVertex();
				label = edge.label();
				outURI = this.getVertexURI(out);
				inURI = this.getVertexURI(in);
				parentCousinIndicator = "NONE";
				oldEdgeString = this.toStringForPrinting(edge, 1);

				if (!outURI.startsWith("/")) {
					noURI.add(outURI);
				}
				if (!inURI.startsWith("/")) {
					noURI.add(inURI);
				}

				if (out == null || in == null) {
					logger.error(edge.id() + " invalid because one vertex was null: out=" + edge.outVertex() + " in=" + edge.inVertex());
				} else {

					if (edge.property("contains-other-v").isPresent()) {
						parentCousinIndicator = edge.property("contains-other-v").value().toString();
					} else if (edge.property("isParent").isPresent()) {
						if ((Boolean)edge.property("isParent").value()) {
							parentCousinIndicator = "OUT";
						} else if (edge.property("isParent-REV").isPresent() && (Boolean)edge.property("isParent-REV").value()) {
							parentCousinIndicator = "IN";
						}
					} else {
						edgeMissingParentProperty.add(this.toStringForPrinting(edge, 1));
					}

					sb.append(outURI + "|" + label + "|" + inURI + "|" + parentCousinIndicator + "\n");
				}
			}
        } catch(Exception ex){
        	logger.error("exception occurred during migration, failing: out=" + out + " in=" + in + "edge=" + oldEdgeString, ex);
        	success = false;
        }
		sb.append("--------EDGES END--------\n");

		logger.info(sb.toString());
		edgeMissingParentProperty.forEach(s -> logger.warn("Edge Missing Parent Property: " + s));
		logger.info("Edge Missing Parent Property Count: " + edgeMissingParentProperty.size());
		logger.info("Vertex Missing URI Property Count: " + noURI.size());

	}

	private String getVertexURI(Vertex v) {
    	if (v.property("aai-uri").isPresent()) {
    		return v.property("aai-uri").value().toString();
		} else {
			return v.id().toString() + "(" + v.property("aai-node-type").value().toString() + ")";
		}
	}

	@Override
	public Optional<String[]> getAffectedNodeTypes() {
		return Optional.empty();
	}

	@Override
	public String getMigrationName() {
		return "edge-report-for-tosca-migration";
	}

	@Override
	public void commit() {
		engine.rollback();
	}

}
