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
package org.onap.aai.dbgen.schemamod;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.janusgraph.core.JanusGraphTransaction;
import org.apache.tinkerpop.gremlin.process.traversal.dsl.graph.GraphTraversalSource;
import org.apache.tinkerpop.gremlin.structure.Vertex;
import org.junit.After;
import org.junit.Before;
import org.junit.FixMethodOrder;
import org.junit.Test;
import org.junit.runners.MethodSorters;
import org.onap.aai.AAISetup;
import org.onap.aai.dbmap.AAIGraph;
import org.onap.aai.exceptions.AAIException;
import static org.junit.Assert.*;

@FixMethodOrder(MethodSorters.NAME_ASCENDING)
public class SchemaModTest extends AAISetup {

	private static final Logger logger = LoggerFactory.getLogger(SchemaModTest.class);

	private SchemaMod schemaMod;

	private Vertex cloudRegionVertex;

	private boolean setUp = false;

	@Before
	public void setup() {
		schemaMod = new SchemaMod(loaderFactory, schemaVersions);
		// deleteTool.SHOULD_EXIT_VM = false;
		JanusGraphTransaction transaction = AAIGraph.getInstance().getGraph().newTransaction();
		boolean success = true;
		try {
			GraphTraversalSource g = transaction.traversal();
			cloudRegionVertex = g.addV().property("aai-node-type", "cloud-region").property("cloud-owner", "test-owner")
					.property("cloud-region-id", "test-region").property("source-of-truth", "JUNIT")
					.property("aai-last-mod-ts","19191919").next();

		

			Vertex pserverVertex = g.addV().property("aai-node-type", "pserver").property("hostname", "test-pserver")
					.property("in-maint", false).property("source-of-truth", "JUNIT").next();

		
			edgeSerializer.addEdge(g, cloudRegionVertex, pserverVertex);
		

		} catch (Exception ex) {
			success = false;
			logger.error("Unable to create the vertexes", ex);
		} finally {
			if (success) {
				transaction.commit();
			} else {
				transaction.rollback();
				fail("Unable to setup the graph");
			}
		}
	}

	
	
	@Test
	public void testSchemaModDataType() throws AAIException {
		String usageString = "Usage: SchemaMod propertyName targetDataType targetIndexInfo preserveDataFlag consistencyLock \n";
		String[] args = {
				"hostname", "String", "noIndex", "false", "false"
		};

		schemaMod.execute(args);
		/*
		 * 2 GhostNodes - CloudRegions 1 OrphaNode - tenant
		 */
		
	}


	
	@After
	public void tearDown() {

		JanusGraphTransaction transaction = AAIGraph.getInstance().getGraph().newTransaction();
		boolean success = true;
		try {
			GraphTraversalSource g = transaction.traversal();
			g.V().has("source-of-truth", "JUNIT").toList().forEach(v -> v.remove());

		} catch (Exception ex) {
			success = false;
			logger.error("Unable to remove the vertexes", ex);
		} finally {
			if (success) {
				transaction.commit();
			} else {
				transaction.rollback();
				fail("Unable to teardown the graph");
			}
		}
	}
}