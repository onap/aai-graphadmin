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
import org.apache.tinkerpop.gremlin.process.traversal.dsl.graph.GraphTraversalSource;
import org.apache.tinkerpop.gremlin.structure.Vertex;
import org.janusgraph.core.JanusGraph;
import org.janusgraph.core.JanusGraphTransaction;
import org.junit.After;
import org.junit.Before;
import org.junit.Ignore;
import org.junit.Test;
import org.onap.aai.AAISetup;
import org.onap.aai.dbmap.AAIGraph;
import org.onap.aai.util.AAIConstants;

import static org.junit.Assert.fail;
import static org.junit.Assert.*;

@Ignore("Test prevents Janusgraph update. Fails only in the pipeline")
public class UpdateToolTest extends AAISetup {

    private static final Logger logger = LoggerFactory.getLogger(UpdateToolTest.class);
    private static String vertexId1, vertexId2;

    private UpdatePropertyTool updatePropertyTool;
    private UpdatePropertyToolInternal updatePropertyToolInternal;

    JanusGraph graph;
    JanusGraphTransaction transaction;
    GraphTraversalSource g;

    @Before
    public void setup(){
        updatePropertyTool = new UpdatePropertyTool();
        updatePropertyToolInternal = new UpdatePropertyToolInternal();
        createGraph();
    }

    private void createGraph() {
        graph = updatePropertyToolInternal.openGraph(AAIConstants.REALTIME_DB_CONFIG);
        transaction = graph.newTransaction();
        boolean success = true;

        try {
            g = transaction.traversal();

            Vertex pserverVertex1 = g.addV()
                    .property("aai-uri", "aai-uri-1")
                    .property("aai-node-type", "pserver")
                    .property("hostname", "test-pserver1")
                    .property("in-maint", false)
                    .property("source-of-truth", "JUNIT")
                    .next();
            vertexId1 = pserverVertex1.id().toString();

            Vertex pserverVertex2 = g.addV()
                    .property("aai-uri", "aai-uri-2")
                    .property("aai-node-type", "pserver")
                    .property("hostname", "test-pserver2")
                    .property("in-maint", false)
                    .property("source-of-truth", "JUNIT")
                    .next();
            vertexId2 = pserverVertex2.id().toString();

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
    public void testUpdatePropertyToolWithVertexIds(){

        String[] args = {
                "--vertexId", vertexId1,
                "--vertexId", vertexId2,
                "--property", "aai-uri",
        };

        assertTrue(updatePropertyToolInternal.run(graph, args));
    }

    @Test
    public void testUpdatePropertyToolWithFileName() {
        String filename = "src/test/resources/vertexIds-test1.txt";
        String[] args = {
                "--filename", filename,
                "--property", "aai-uri",
        };

        assertTrue(updatePropertyToolInternal.run(graph, args));
    }

    @Test
    public void testUpdatePropertyToolWithAbbrFileNameAndVertexIds() {
        String filename = "src/test/resources/vertexIds-test1.txt";
        String[] args = {
                "-f", filename,
                "-v", vertexId1,
                "-v", vertexId2,
                "-p", "aai-uri",
        };

        assertTrue(updatePropertyToolInternal.run(graph, args));
    }

    @Test
    public void testProcessCommandLineArgumentsWithNoVertexIdsArgs() {
        String[] args = {
                "-p", "aai-uri",
        };

        assertFalse(updatePropertyToolInternal.run(graph, args));
    }

    @Test
    public void testProcessCommandLineArgumentsWithInvalidArgs() {
        String[] args = {
                "-vertexId", vertexId1,
                "--property", "aai-uri",
        };

        assertFalse(updatePropertyToolInternal.run(graph, args));
    }


    @Test
    public void testProcessCommandLineArgumentsWithNoProperty() {
        String[] args = {
                "-v", vertexId1,
                "-v", vertexId2,
        };

        assertFalse(updatePropertyToolInternal.run(graph, args));
    }

    @Test
    public void testUpdatePropertyToolInvalidFilePath() {
        String filename = "src/test/resources/InvalidFileName.txt";
        String[] args = {
                "-f", filename,
                "-p", "aai-uri",
        };

        assertFalse(updatePropertyToolInternal.run(graph, args));
    }

    @Test
    public void testUpdatePropertyToolInvalidVertexId() {
        String[] args = {
                "-v", "!#$%",
                "-p", "aai-uri",
        };

        assertFalse(updatePropertyToolInternal.run(graph, args));
    }

    @Test
    public void testSetUpAAIConfigWithNullGraph() {
        String filename = "src/test/resources/InvalidFileName.txt";
        String[] args = {
                "-v", vertexId1,
                "-p", "aai-uri",
        };
        assertFalse(updatePropertyToolInternal.run(null, args));
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
        updatePropertyToolInternal.closeGraph(graph);
    }
}
