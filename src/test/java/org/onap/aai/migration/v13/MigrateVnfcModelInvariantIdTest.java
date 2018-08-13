package org.onap.aai.migration.v13;

import org.apache.tinkerpop.gremlin.process.traversal.dsl.graph.GraphTraversalSource;
import org.apache.tinkerpop.gremlin.process.traversal.strategy.verification.ReadOnlyStrategy;
import org.apache.tinkerpop.gremlin.structure.Vertex;
import org.janusgraph.core.JanusGraph;
import org.janusgraph.core.JanusGraphFactory;
import org.janusgraph.core.JanusGraphTransaction;
import org.junit.*;
import org.onap.aai.AAISetup;
import org.onap.aai.dbmap.DBConnectionType;
import org.onap.aai.introspection.Loader;
import org.onap.aai.introspection.ModelType;
import org.onap.aai.setup.SchemaVersions;
import org.onap.aai.setup.SchemaVersion;
import org.onap.aai.serialization.engines.JanusGraphDBEngine;
import org.onap.aai.serialization.engines.QueryStyle;
import org.onap.aai.serialization.engines.TransactionalGraphEngine;

import static org.junit.Assert.assertEquals;
import static org.mockito.Mockito.spy;
import static org.mockito.Mockito.when;

public class MigrateVnfcModelInvariantIdTest extends AAISetup{

	private final static ModelType introspectorFactoryType = ModelType.MOXY;
	private final static QueryStyle queryStyle = QueryStyle.TRAVERSAL;
	private final static DBConnectionType type = DBConnectionType.REALTIME;

	private Loader loader;
	private TransactionalGraphEngine dbEngine;
	private JanusGraph graph;
	private MigrateVnfcModelInvariantId migration;
	private JanusGraphTransaction tx;
	private GraphTraversalSource g;

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

		Vertex vnfc1 = g.addV().property("aai-node-type", "vnfc").property("model-invariant-id", "vnfc-invariant-id-1")
                .property("vnfcName", "vnfc-name-1").property("nfcNamingCode", "naming-code-1")
                .property("nfcFunction", "function-1")
                .property("model-version-id", "vnfc-variant-id-1").next();

        Vertex vnfc2 = g.addV().property("aai-node-type", "vnfc").property("model-invariant-id-local", "vnfc-invariant-id-2")
                .property("vnfcName", "vnfc-name-2").property("nfcNamingCode", "naming-code-2")
                .property("nfcFunction", "function-2")
                .property("model-version-id-local", "vnfc-version-id-2").next();

		TransactionalGraphEngine spy = spy(dbEngine);
		TransactionalGraphEngine.Admin adminSpy = spy(dbEngine.asAdmin());

		GraphTraversalSource traversal = g;
		GraphTraversalSource readOnly = tx.traversal(GraphTraversalSource.build().with(ReadOnlyStrategy.instance()));
		when (spy.tx()).thenReturn(tx);
		when(spy.asAdmin()).thenReturn(adminSpy);
		when(adminSpy.getTraversalSource()).thenReturn(traversal);
		when(adminSpy.getReadOnlyTraversalSource()).thenReturn(readOnly);

		migration = new MigrateVnfcModelInvariantId(spy, loaderFactory, edgeIngestor, edgeSerializer, schemaVersions);
		migration.run();
	}

	@After
	public void cleanUp() {
		tx.tx().rollback();
		graph.close();
	}

	@Test
    public void testIdsUpdated() throws Exception {
        assertEquals(true,
                g.V().has("aai-node-type", "vnfc").has("vnfcName", "vnfc-name-1").has("model-invariant-id-local").next().property("model-invariant-id-local").isPresent());
        assertEquals("model-invariant-id renamed to model-invariant-id-local for vnfc", "vnfc-invariant-id-1",
                g.V().has("aai-node-type", "vnfc").has("vnfcName", "vnfc-name-1").next().value("model-invariant-id-local").toString());
    }

    @Test
    public void testIdsNotUpdated() throws Exception {
	    assertEquals("model-invariant-id-local should not be renamed for vnfc", "vnfc-invariant-id-2",
                g.V().has("aai-node-type", "vnfc").has("vnfcName", "vnfc-name-2").next().value("model-invariant-id-local").toString());
    }
}
