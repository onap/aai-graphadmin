package org.onap.aai.migration.v13;

import org.apache.tinkerpop.gremlin.process.traversal.dsl.graph.GraphTraversalSource;
import org.apache.tinkerpop.gremlin.process.traversal.strategy.verification.ReadOnlyStrategy;
import org.apache.tinkerpop.gremlin.structure.Vertex;
import org.janusgraph.core.JanusGraph;
import org.janusgraph.core.JanusGraphFactory;
import org.janusgraph.core.JanusGraphTransaction;
import org.junit.AfterClass;
import org.junit.Before;
import org.junit.Test;
import org.onap.aai.AAISetup;
import org.onap.aai.dbmap.DBConnectionType;
import org.onap.aai.introspection.Loader;
import org.onap.aai.introspection.LoaderFactory;
import org.onap.aai.introspection.ModelType;
import org.onap.aai.setup.SchemaVersions;
import org.onap.aai.setup.SchemaVersion;
import org.onap.aai.serialization.engines.JanusGraphDBEngine;
import org.onap.aai.serialization.engines.QueryStyle;
import org.onap.aai.serialization.engines.TransactionalGraphEngine;

import static org.junit.Assert.*;
import static org.mockito.Mockito.spy;
import static org.mockito.Mockito.when;

public class MigrateInstanceGroupModelVersionIdTest extends AAISetup {

	private final static ModelType introspectorFactoryType = ModelType.MOXY;
	private final static QueryStyle queryStyle = QueryStyle.TRAVERSAL;
	private final static DBConnectionType type = DBConnectionType.REALTIME;

	private static Loader loader;
	private static TransactionalGraphEngine dbEngine;
	private static JanusGraph graph;
	private static MigrateInstanceGroupModelVersionId migration;
	private static JanusGraphTransaction tx;
	private static GraphTraversalSource g;

	@Before
	public void setUp() throws Exception {
		graph = JanusGraphFactory.build().set("storage.backend","inmemory").open();
		tx = graph.newTransaction();
		g = tx.traversal();
		loader = loaderFactory.createLoaderForVersion(introspectorFactoryType, schemaVersions.getDefaultVersion());
		dbEngine = new JanusGraphDBEngine(
				queryStyle,
				type,
				loader);

		Vertex instancegroup1 = g.addV().property("aai-node-type", "instance-group").property("id", "instance-id-1")
                .property("description","instance-description-1").property("instanceGroupType","instance-type-1")
                .property("model-version-id", "instance-version-id-1").next();

        Vertex instancegroup2 = g.addV().property("aai-node-type", "instance-group").property("id", "instance-id-2")
                .property("description","instance-description-2").property("instanceGroupType","instance-type-1")
                .property("model-version-id-local", "instance-version-id-2").next();

		TransactionalGraphEngine spy = spy(dbEngine);
		TransactionalGraphEngine.Admin adminSpy = spy(dbEngine.asAdmin());

		GraphTraversalSource traversal = g;
		GraphTraversalSource readOnly = tx.traversal(GraphTraversalSource.build().with(ReadOnlyStrategy.instance()));
		when (spy.tx()).thenReturn(tx);
		when(spy.asAdmin()).thenReturn(adminSpy);
		when(adminSpy.getTraversalSource()).thenReturn(traversal);
		when(adminSpy.getReadOnlyTraversalSource()).thenReturn(readOnly);

		migration = new MigrateInstanceGroupModelVersionId(spy, loaderFactory, edgeIngestor, edgeSerializer, schemaVersions);
		migration.run();
	}

	@AfterClass
	public static void cleanUp() {
		tx.tx().rollback();
		graph.close();
	}

	@Test
    public void testIdsUpdated() throws Exception {
        assertEquals(true,
                g.V().has("aai-node-type", "instance-group").has("id", "instance-id-1").has("model-version-id-local").next().property("model-version-id-local").isPresent());
        assertEquals("model-version-id renamed to model-version-id-local for instance-group", "instance-version-id-1",
                g.V().has("aai-node-type", "instance-group").has("id", "instance-id-1").next().value("model-version-id-local").toString());
    }

    @Test
    public void testIdsNotUpdated() throws Exception {
        assertEquals("model-version-id-local remains the same for instance-group", "instance-version-id-2",
                g.V().has("aai-node-type", "instance-group").has("id", "instance-id-2").next().value("model-version-id-local").toString());
    }
}
