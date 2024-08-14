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

import org.apache.tinkerpop.gremlin.process.traversal.dsl.graph.GraphTraversalSource;
import org.apache.tinkerpop.gremlin.process.traversal.strategy.verification.ReadOnlyStrategy;
import org.apache.tinkerpop.gremlin.structure.Vertex;
import org.janusgraph.core.JanusGraph;
import org.janusgraph.core.JanusGraphFactory;
import org.janusgraph.core.JanusGraphTransaction;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.onap.aai.AAISetup;
import org.onap.aai.dbmap.DBConnectionType;
import org.onap.aai.introspection.Loader;
import org.onap.aai.introspection.ModelType;
import org.onap.aai.setup.SchemaVersions;
import org.onap.aai.setup.SchemaVersion;
import org.onap.aai.serialization.engines.JanusGraphDBEngine;
import org.onap.aai.serialization.engines.QueryStyle;
import org.onap.aai.serialization.engines.TransactionalGraphEngine;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.mockito.Mockito.spy;
import static org.mockito.Mockito.when;

public class MigrateInstanceGroupModelInvariantIdTest extends AAISetup{

	private final static ModelType introspectorFactoryType = ModelType.MOXY;
	private final static QueryStyle queryStyle = QueryStyle.TRAVERSAL;

	private Loader loader;
	private TransactionalGraphEngine dbEngine;
	private JanusGraph graph;
	private MigrateInstanceGroupModelInvariantId migration;
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

		Vertex instancegroup1 = g.addV().property("aai-node-type", "instance-group").property("id", "instance-id-1")
                .property("description","instance-description-1").property("instanceGroupType","instance-type-1")
                .property("model-invariant-id", "instance-invariant-id-1").next();

        Vertex instancegroup2 = g.addV().property("aai-node-type", "instance-group").property("id", "instance-id-2")
                .property("description","instance-description-2").property("instanceGroupType","instance-type-1")
                .property("model-invariant-id-local", "instance-invariant-id-2").next();

		TransactionalGraphEngine spy = spy(dbEngine);
		TransactionalGraphEngine.Admin adminSpy = spy(dbEngine.asAdmin());

		GraphTraversalSource traversal = g;
		GraphTraversalSource readOnly = graph.traversal().withStrategies(ReadOnlyStrategy.instance());
		when (spy.tx()).thenReturn(tx);
		when(spy.asAdmin()).thenReturn(adminSpy);
		when(adminSpy.getTraversalSource()).thenReturn(traversal);
		when(adminSpy.getReadOnlyTraversalSource()).thenReturn(readOnly);

		migration = new MigrateInstanceGroupModelInvariantId(spy, loaderFactory, edgeIngestor, edgeSerializer, schemaVersions);
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
                g.V().has("aai-node-type", "instance-group").has("id", "instance-id-1").has("model-invariant-id-local").next().property("model-invariant-id-local").isPresent());
	    assertEquals("instance-invariant-id-1",
                g.V().has("aai-node-type", "instance-group").has("id", "instance-id-1").next().value("model-invariant-id-local").toString(),
                "model-invariant-id renamed to model-invariant-id-local for instance-group");
    }

    @Test
    public void testIdsNotUpdated() throws Exception {
        assertEquals("instance-invariant-id-2",
                g.V().has("aai-node-type", "instance-group").has("id", "instance-id-2").next().value("model-invariant-id-local").toString(),
                "model-invariant-id-local remains the same for instance-group");
    }
}
