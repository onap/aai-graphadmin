package org.onap.aai.migration.v12;

import org.janusgraph.core.JanusGraphFactory;
import org.janusgraph.core.JanusGraph;
import org.janusgraph.core.JanusGraphTransaction;
import org.apache.tinkerpop.gremlin.process.traversal.dsl.graph.GraphTraversalSource;
import org.apache.tinkerpop.gremlin.structure.Vertex;
import org.junit.After;
import org.junit.Before;
import org.junit.Test;
import org.onap.aai.AAISetup;
import org.onap.aai.dbmap.DBConnectionType;
import org.onap.aai.introspection.Loader;
import org.onap.aai.introspection.ModelType;
import org.onap.aai.setup.SchemaVersions;
import org.onap.aai.setup.SchemaVersion;
import org.onap.aai.serialization.engines.QueryStyle;
import org.onap.aai.serialization.engines.JanusGraphDBEngine;
import org.onap.aai.serialization.engines.TransactionalGraphEngine;
import static org.junit.Assert.assertEquals;
import static org.mockito.Mockito.spy;
import static org.mockito.Mockito.when;


public class MigrateModelVerDistributionStatusPropertyTest extends AAISetup{

    private final static ModelType introspectorFactoryType = ModelType.MOXY;
    private final static QueryStyle queryStyle = QueryStyle.TRAVERSAL;
    private final static DBConnectionType type = DBConnectionType.REALTIME;
    private Loader loader;
    private TransactionalGraphEngine dbEngine;
    private JanusGraph graph;
    private MigrateModelVerDistriubutionStatusProperty migration;
    private GraphTraversalSource g;
    private JanusGraphTransaction tx;
    Vertex modelVer1;
    Vertex modelVer2;

    @Before
    public void setUp() throws Exception {
        graph = JanusGraphFactory.build().set("storage.backend", "inmemory").open();
        tx = graph.newTransaction();
        g = tx.traversal();
        loader = loaderFactory.createLoaderForVersion(introspectorFactoryType, schemaVersions.getDefaultVersion());
        dbEngine = new JanusGraphDBEngine(
                queryStyle,
                type,
                loader);
         modelVer1 = g.addV().property("aai-node-type", "model-ver")
                .property("model-version-id", "modelVer1")
                .property("distribution-status", "test1")
                .next();

        modelVer2 = g.addV().property("aai-node-type", "model-ver")
                .property("model-version-id", "modelVer1")
                .next();

        TransactionalGraphEngine spy = spy(dbEngine);
        TransactionalGraphEngine.Admin adminSpy = spy(dbEngine.asAdmin());
        GraphTraversalSource traversal = g;
        when(spy.asAdmin()).thenReturn(adminSpy);
        when(adminSpy.getTraversalSource()).thenReturn(traversal);
        migration = new MigrateModelVerDistriubutionStatusProperty(spy, loaderFactory, edgeIngestor, edgeSerializer, schemaVersions);
        migration.run();
    }

    @After
    public void cleanUp() {
        tx.rollback();
        graph.close();
    }


    /***
     * checks if the Distribution Status value was changed
     */

    @Test
    public void confirmDistributionStatusChanged() {

        assertEquals("DISTRIBUTION_COMPLETE_OK",modelVer1.property("distribution-status").value());
        assertEquals("DISTRIBUTION_COMPLETE_OK",modelVer2.property("distribution-status").value());

    }


}