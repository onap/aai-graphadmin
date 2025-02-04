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
package org.onap.aai.migration.v13;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.mockito.Mockito.spy;
import static org.mockito.Mockito.when;

import org.apache.tinkerpop.gremlin.process.traversal.dsl.graph.GraphTraversalSource;
import org.apache.tinkerpop.gremlin.process.traversal.strategy.verification.ReadOnlyStrategy;
import org.apache.tinkerpop.gremlin.structure.Vertex;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.onap.aai.AAISetup;
import org.onap.aai.introspection.Loader;
import org.onap.aai.introspection.ModelType;
import org.onap.aai.serialization.engines.QueryStyle;
import org.onap.aai.serialization.engines.JanusGraphDBEngine;
import org.onap.aai.serialization.engines.TransactionalGraphEngine;

import org.janusgraph.core.JanusGraphFactory;
import org.janusgraph.core.JanusGraph;
import org.janusgraph.core.JanusGraphTransaction;

public class MigrateVnfcModelVersionIdTest extends AAISetup{

	private final static ModelType introspectorFactoryType = ModelType.MOXY;
	private final static QueryStyle queryStyle = QueryStyle.TRAVERSAL;

	private Loader loader;
	private TransactionalGraphEngine dbEngine;
	private JanusGraph graph;
	private MigrateVnfcModelVersionId migration;
	private JanusGraphTransaction tx;
	private GraphTraversalSource g;

	@BeforeEach
	public void setUp() throws Exception {
		graph = JanusGraphFactory.build().set("storage.backend","inmemory").open();
		tx = graph.newTransaction();
		g = tx.traversal();
		loader = loaderFactory.createLoaderForVersion(introspectorFactoryType, schemaVersions.getDefaultVersion());
		dbEngine = new JanusGraphDBEngine(
				queryStyle,
				loader);

		Vertex vnfc1 = g.addV().property("aai-node-type", "vnfc").property("model-version-id", "vnfc-version-id-1")
                .property("vnfcName", "vnfc-name-1").property("nfcNamingCode", "naming-code-1")
                .property("nfcFunction", "function-1")
                .property("model-version-id", "vnfc-version-id-1").next();

        Vertex vnfc2 = g.addV().property("aai-node-type", "vnfc").property("model-version-id-local", "vnfc-version-id-1")
                .property("vnfcName", "vnfc-name-2").property("nfcNamingCode", "naming-code-1")
                .property("nfcFunction", "function-2")
                .property("model-version-id-local", "vnfc-version-id-2").next();

		TransactionalGraphEngine spy = spy(dbEngine);
		TransactionalGraphEngine.Admin adminSpy = spy(dbEngine.asAdmin());

		GraphTraversalSource traversal = g;
		GraphTraversalSource readOnly = graph.traversal().withStrategies(ReadOnlyStrategy.instance());
		when (spy.tx()).thenReturn(tx);
		when(spy.asAdmin()).thenReturn(adminSpy);
		when(adminSpy.getTraversalSource()).thenReturn(traversal);
		when(adminSpy.getReadOnlyTraversalSource()).thenReturn(readOnly);

		migration = new MigrateVnfcModelVersionId(spy, loaderFactory, edgeIngestor, edgeSerializer, schemaVersions);
		migration.run();
	}

	@AfterEach
	public void cleanUp() {
		tx.tx().rollback();
		graph.close();
	}

	@Test
    public void testIdsUpdated() throws Exception {
        assertEquals(true,
                g.V().has("aai-node-type", "vnfc").has("vnfcName", "vnfc-name-1").has("model-version-id-local").next().property("model-version-id-local").isPresent());
        assertEquals("vnfc-version-id-1",
                g.V().has("aai-node-type", "vnfc").has("vnfcName", "vnfc-name-1").next().value("model-version-id-local").toString(),
                "model-version-id renamed to model-invariant-id-local for vnfc");
    }

    @Test
    public void testIdsNotUpdated() throws Exception {
        assertEquals("vnfc-version-id-2",
                g.V().has("aai-node-type", "vnfc").has("vnfcName", "vnfc-name-2").next().value("model-version-id-local").toString(),
                "model-version-id-local should not be renamed for vnfc");
    }
}
