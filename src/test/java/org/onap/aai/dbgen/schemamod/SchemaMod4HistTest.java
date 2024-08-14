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
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.MethodOrderer.MethodName;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.TestMethodOrder;
import org.onap.aai.AAISetup;
import org.onap.aai.dbmap.AAIGraph;
import org.onap.aai.exceptions.AAIException;

import static org.junit.jupiter.api.Assertions.*;

@TestMethodOrder(MethodName.class)
public class SchemaMod4HistTest extends AAISetup {

	private static final Logger logger = LoggerFactory.getLogger(SchemaMod4HistTest.class);

	private SchemaMod4Hist schemaMod4H;


	@BeforeEach
	public void setup() {
		schemaMod4H = new SchemaMod4Hist(loaderFactory, schemaVersions);
		
		JanusGraphTransaction transaction = AAIGraph.getInstance().getGraph().newTransaction();
		boolean success = true;
		try {
			GraphTraversalSource g = transaction.traversal();

			g.addV().property("aai-node-type", "pserver").property("hostname", "test-pserver1")
					.property("in-maint", false).property("source-of-truth", "JUNIT").next();			

		} catch (Exception ex) {
			success = false;
			logger.error("Unable to create the vertices", ex);
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
		// Note: Usage: SchemaMod4Hist propertyName targetDataType targetIndexInfo preserveDataFlag 
		String[] args = {
				"in-maint", "String", "index", "true"
		};
		
		
		boolean executedWithoutError = true;
		try {
			schemaMod4H.execute(args);
		}
		catch (Exception e) {
			executedWithoutError = false;
		}

		assertTrue(executedWithoutError, "Ran schemaMod without throwing exception ");
				
	}

	
	@AfterEach
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