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
package org.onap.aai.dbgen;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.janusgraph.core.JanusGraphTransaction;

import static org.junit.jupiter.api.Assertions.fail;
import org.apache.tinkerpop.gremlin.process.traversal.dsl.graph.GraphTraversalSource;
import org.apache.tinkerpop.gremlin.structure.Edge;
import org.apache.tinkerpop.gremlin.structure.Vertex;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.MethodOrderer.MethodName;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.TestMethodOrder;
import org.onap.aai.AAISetup;
import org.onap.aai.dbmap.AAIGraph;

import java.io.ByteArrayInputStream;
import java.io.InputStream;
import java.util.List;

@TestMethodOrder(MethodName.class)
public class ForceDeleteToolTest extends AAISetup {

    private static final Logger logger = LoggerFactory.getLogger(ForceDeleteToolTest.class);

    private ForceDeleteTool deleteTool;

    private Vertex cloudRegionVertex;

    @BeforeEach
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
    @AfterEach
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