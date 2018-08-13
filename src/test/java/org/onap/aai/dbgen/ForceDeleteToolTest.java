package org.onap.aai.dbgen;

import com.att.eelf.configuration.EELFLogger;
import com.att.eelf.configuration.EELFManager;
import org.janusgraph.core.JanusGraphTransaction;
import org.apache.tinkerpop.gremlin.process.traversal.dsl.graph.GraphTraversalSource;
import org.apache.tinkerpop.gremlin.structure.Edge;
import org.apache.tinkerpop.gremlin.structure.Vertex;
import org.junit.After;
import org.junit.Before;
import org.junit.FixMethodOrder;
import org.junit.Test;
import org.junit.runners.MethodSorters;
import org.onap.aai.AAISetup;
import org.onap.aai.dbmap.AAIGraph;

import java.io.ByteArrayInputStream;
import java.io.InputStream;
import java.util.List;

import static org.junit.Assert.fail;

@FixMethodOrder(MethodSorters.NAME_ASCENDING)
public class ForceDeleteToolTest extends AAISetup {

    private static final EELFLogger logger = EELFManager.getInstance().getLogger(ForceDeleteToolTest.class);

    private ForceDeleteTool deleteTool;

    private Vertex cloudRegionVertex;

    @Before
    public void setup(){
        deleteTool = new ForceDeleteTool();
        deleteTool.SHOULD_EXIT_VM = false;
        JanusGraphTransaction transaction = AAIGraph.getInstance().getGraph().newTransaction();

        boolean success = true;

        try {

            GraphTraversalSource g = transaction.traversal();

            cloudRegionVertex = g.addV()
                    .property("aai-node-type", "cloud-region")
                    .property("cloud-owner", "test-owner")
                    .property("cloud-region-id", "test-region")
                    .property("source-of-truth", "JUNIT")
                    .next();

            Vertex tenantVertex = g.addV()
                    .property("aai-node-type", "tenant")
                    .property("tenant-id", "test-tenant")
                    .property("source-of-truth", "JUNIT")
                    .next();

            Vertex pserverVertex = g.addV()
                    .property("aai-node-type", "pserver")
                    .property("hostname", "test-pserver")
                    .property("in-maint", false)
                    .property("source-of-truth", "JUNIT")
                    .next();

            edgeSerializer.addTreeEdge(g, cloudRegionVertex, tenantVertex);
            edgeSerializer.addEdge(g, cloudRegionVertex, pserverVertex);

        } catch(Exception ex){
            success = false;
            logger.error("Unable to create the vertexes", ex);
        } finally {
            if(success){
                transaction.commit();
            } else {
                transaction.rollback();
                fail("Unable to setup the graph");
            }
        }


    }

    @Test
    public void testCollectDataForVertex(){

        String [] args = {

                "-action",
                "COLLECT_DATA",
                "-userId",
                "someuser",
                "-params4Collect",
                "cloud-owner|test-owner"
        };

        deleteTool.main(args);
    }

    @Test
    public void testDeleteNode(){

        String id = cloudRegionVertex.id().toString();

        String [] args = {

                "-action",
                "DELETE_NODE",
                "-userId",
                "someuser",
                "-vertexId",
                id
        };

        deleteTool.main(args);
    }

    @Test
    public void testCollectDataForEdge(){

        JanusGraphTransaction transaction = AAIGraph.getInstance().getGraph().newTransaction();
        GraphTraversalSource g = transaction.traversal();
        List<Edge> edges = g.E().toList();
        String cloudRegionToPserverId = edges.get(0).id().toString();

        String [] args = {

                "-action",
                "COLLECT_DATA",
                "-userId",
                "someuser",
                "-edgeId",
                cloudRegionToPserverId
        };

        deleteTool.main(args);
    }

    @Test
    public void testDeleteForEdge(){

        InputStream systemInputStream = System.in;
        ByteArrayInputStream in = new ByteArrayInputStream("y".getBytes());
        System.setIn(in);
        JanusGraphTransaction transaction = AAIGraph.getInstance().getGraph().newTransaction();
        GraphTraversalSource g = transaction.traversal();
        List<Edge> edges = g.E().toList();
        String cloudRegionToPserverId = edges.get(0).id().toString();

        String [] args = {

                "-action",
                "DELETE_EDGE",
                "-userId",
                "someuser",
                "-edgeId",
                cloudRegionToPserverId
        };

        deleteTool.main(args);
        System.setIn(systemInputStream);
    }
    @After
    public void tearDown(){

        JanusGraphTransaction transaction = AAIGraph.getInstance().getGraph().newTransaction();
        boolean success = true;

        try {

            GraphTraversalSource g = transaction.traversal();

            g.V().has("source-of-truth", "JUNIT")
                 .toList()
                 .forEach(v -> v.remove());

        } catch(Exception ex){
            success = false;
            logger.error("Unable to remove the vertexes", ex);
        } finally {
            if(success){
                transaction.commit();
            } else {
                transaction.rollback();
                fail("Unable to teardown the graph");
            }
        }
    }
}