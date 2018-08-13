package org.onap.aai.migration;

import com.att.eelf.configuration.EELFLogger;
import com.att.eelf.configuration.EELFManager;
import org.janusgraph.core.Cardinality;
import org.janusgraph.core.JanusGraphTransaction;
import org.apache.tinkerpop.gremlin.process.traversal.dsl.graph.GraphTraversalSource;
import org.apache.tinkerpop.gremlin.structure.Vertex;
import org.junit.Before;
import org.junit.Test;
import org.onap.aai.AAISetup;
import org.onap.aai.dbmap.AAIGraph;
import org.onap.aai.dbmap.DBConnectionType;
import org.onap.aai.edges.EdgeIngestor;
import org.onap.aai.introspection.Loader;
import org.onap.aai.introspection.LoaderFactory;
import org.onap.aai.introspection.ModelType;
import org.onap.aai.serialization.db.EdgeSerializer;
import org.onap.aai.setup.SchemaVersions;
import org.onap.aai.setup.SchemaVersion;
import org.onap.aai.serialization.engines.QueryStyle;
import org.onap.aai.serialization.engines.JanusGraphDBEngine;
import org.onap.aai.serialization.engines.TransactionalGraphEngine;

import java.util.List;
import java.util.Optional;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertTrue;

public class PropertyMigratorTest extends AAISetup {

    private static final EELFLogger logger = EELFManager.getInstance().getLogger(PropertyMigratorTest.class);

    public static class PserverPropMigrator extends PropertyMigrator {

        public PserverPropMigrator(TransactionalGraphEngine engine, LoaderFactory loaderFactory, EdgeIngestor edgeIngestor, EdgeSerializer edgeSerializer, SchemaVersions schemaVersions, String oldName, String newName, Class<?> type, Cardinality cardinality) {
            super(engine, loaderFactory, edgeIngestor, edgeSerializer, schemaVersions);
            this.initialize(oldName, newName, type, cardinality);
        }

        @Override
        public boolean isIndexed() {
            return true;
        }

        @Override
        public Optional<String[]> getAffectedNodeTypes() {
            return Optional.of(new String[]{ "pserver" });
        }

        @Override
        public String getMigrationName() {
            return "PserverPropMigrator";
        }
    }

    @Before
    public void setup(){
        AAIGraph.getInstance();
        JanusGraphTransaction janusgraphTransaction = AAIGraph.getInstance().getGraph().newTransaction();
        boolean success = true;

        try {
            GraphTraversalSource g = janusgraphTransaction.traversal();
            g.addV()
                 .property("aai-node-type", "pserver")
                 .property("hostname", "fake-hostname")
                 .property("inv-status", "some status")
                 .property("source-of-truth", "JUNIT")
                 .next();
        } catch(Exception ex){
           success = false;
           logger.error("Unable to commit the transaction {}", ex);

        } finally {
            if(success){
                janusgraphTransaction.commit();
            } else {
                janusgraphTransaction.rollback();
            }

        }
    }

    @Test
    public void testAfterPropertyMigration(){

        String oldPropName = "inv-status";
        String newPropName = "inventory-status";

        Loader loader = loaderFactory.createLoaderForVersion(ModelType.MOXY, schemaVersions.getDefaultVersion());
        JanusGraphDBEngine dbEngine = new JanusGraphDBEngine(QueryStyle.TRAVERSAL, DBConnectionType.REALTIME, loader);
        dbEngine.startTransaction();

        PropertyMigrator propertyMigrator = new PserverPropMigrator(dbEngine, loaderFactory, edgeIngestor, edgeSerializer, schemaVersions, oldPropName, newPropName, String.class, Cardinality.SINGLE);
        propertyMigrator.run();
        assertEquals("Expecting the property to be success", Status.SUCCESS, propertyMigrator.getStatus());
        dbEngine.commit();

        JanusGraphTransaction janusgraphTransaction = AAIGraph.getInstance().getGraph().newTransaction();
        GraphTraversalSource g = janusgraphTransaction.traversal();

        List<Vertex> oldVList = g.V().has("aai-node-type", "pserver").has(oldPropName).toList();
        List<Vertex> newVList = g.V().has("aai-node-type", "pserver").has(newPropName).toList();

        assertEquals("Expecting the vertex list with old property to be zero", 0, oldVList.size());
        assertEquals("Expecting the vertex list with new property to be 1", 1, newVList.size());
        assertEquals("Expecting the equipment type to be some equipment", "some status", newVList.get(0).property(newPropName).value());
    }
}