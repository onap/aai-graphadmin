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
import org.springframework.boot.test.rule.OutputCapture;
import org.janusgraph.core.JanusGraphTransaction;
import org.apache.tinkerpop.gremlin.process.traversal.dsl.graph.GraphTraversalSource;
import org.apache.tinkerpop.gremlin.structure.Edge;
import org.apache.tinkerpop.gremlin.structure.Vertex;
import org.junit.After;
import org.junit.Before;
import org.junit.FixMethodOrder;
import org.junit.Rule;
import org.junit.Test;
import org.junit.runners.MethodSorters;
import org.onap.aai.AAISetup;
import org.onap.aai.dbmap.AAIGraph;

import java.io.ByteArrayInputStream;
import java.io.InputStream;
import java.util.List;

import static org.hamcrest.Matchers.containsString;
import static org.junit.Assert.assertThat;
import static org.junit.Assert.fail;

@FixMethodOrder(MethodSorters.NAME_ASCENDING)
public class ForceDeleteToolTest extends AAISetup {

    private static final Logger logger = LoggerFactory.getLogger(ForceDeleteToolTest.class);

    private ForceDeleteTool deleteTool;

    private Vertex cloudRegionVertex;
    
    @Rule
    public OutputCapture outputCapture = new OutputCapture();

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

        InputStream systemInputStream = System.in;
        ByteArrayInputStream in = new ByteArrayInputStream("y".getBytes());
        System.setIn(in);
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
        System.setIn(systemInputStream);
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
    
 //------------------------------ Adding some new tests --------------

    
    @Test
    public void testCollectDataForVertexId(){
        String id = cloudRegionVertex.id().toString();
        
        String [] args = {
                "-action",
                "COLLECT_DATA",
                "-userId",
                "someuser",
                "-vertexId",
                id
        };

        deleteTool.main(args);
    }
       
    
    @Test
    public void testInputParamsBadAction(){
        JanusGraphTransaction transaction = AAIGraph.getInstance().getGraph().newTransaction();
        GraphTraversalSource g = transaction.traversal();
        List<Edge> edges = g.E().toList();
        String cloudRegionToPserverId = edges.get(0).id().toString();

        String [] args = {
                "-action",
                "JUNK-ACTION",
                "-userId",
                "someuser",
                "-edgeId",
                cloudRegionToPserverId
        };

        deleteTool.main(args);
       // Capture the standard output and see if the following text is there
        assertThat(outputCapture.toString(), containsString("Bad action parameter"));
       
    }
 
   
    @Test
    public void testMissingInputs(){

        JanusGraphTransaction transaction = AAIGraph.getInstance().getGraph().newTransaction();
        GraphTraversalSource g = transaction.traversal();

        String [] args = {
                "-action"
        };
        deleteTool.main(args);
        assertThat(outputCapture.toString(), containsString("No value passed with"));
        

        args = new String []{
                "-vertexId"
        };
        deleteTool.main(args);
        assertThat(outputCapture.toString(), containsString("No value passed with"));
        
        
        args = new String []{
                "-edgeId"
        };
        deleteTool.main(args);
        assertThat(outputCapture.toString(), containsString("No value passed with"));
       
        
        args = new String []{
                "-params4Collect"
        };
        deleteTool.main(args);
        assertThat(outputCapture.toString(), containsString("No value passed with"));
       
    }
       
        
    
    @Test
    public void testInvalidUserIds(){

        JanusGraphTransaction transaction = AAIGraph.getInstance().getGraph().newTransaction();
        GraphTraversalSource g = transaction.traversal();
        List<Edge> edges = g.E().toList();
        String cloudRegionToPserverId = edges.get(0).id().toString();

        String [] args = {              
                "-userId"
        };
        deleteTool.main(args);
        assertThat(outputCapture.toString(), containsString("No value passed with"));
        
        args = new String []{
                "-userId",
                "bad"
        };
        deleteTool.main(args);
        assertThat(outputCapture.toString(), containsString("Bad userId parameter"));
        
        args = new String []{
                "-userId",
                "AAIADMIN"
        };
        deleteTool.main(args);
        assertThat(outputCapture.toString(), containsString("Bad userId parameter"));
    }
        
        
    @Test
    public void testBadInputs2(){

    	// pass in a bad/unknown argument (-junkParam)
        String [] args = {
                "-junkParam",
                "COLLECT_DATA",
                "-userId",
                "someuser",
                "-edgeId",
                "999"
        };
        
        deleteTool.main(args);
        assertThat(outputCapture.toString(), containsString("Unrecognized argument"));           
    }
        
    @Test
    public void testBadInputs3(){

    	// pass in a nonExistant edgeId for DELETE EDGE
        JanusGraphTransaction transaction = AAIGraph.getInstance().getGraph().newTransaction();
        GraphTraversalSource g = transaction.traversal();
    
        String [] args = {
                "-action",
                "DELETE_EDGE",
                "-userId",
                "someuser",
                "-edgeId",
                "NotRealId"
        };
        
        deleteTool.main(args);
        assertThat(outputCapture.toString(), containsString("Edge with edgeId = NotRealId not found"));
             
    }
    
    @Test
    public void testBadInputs4(){

        JanusGraphTransaction transaction = AAIGraph.getInstance().getGraph().newTransaction();
        GraphTraversalSource g = transaction.traversal();
        
        // pass in a bad vertex Id when collecting data
    
        String [] args = {
        		"-action",
                "COLLECT_DATA",
                "-userId",
                "someuser",
                "-vertexId",
                "NotANumber"
        };
        
        deleteTool.main(args);
        assertThat(outputCapture.toString(), containsString("Bad value passed"));
             
    }
         
    
    @Test
    public void testBadInputs5(){

        JanusGraphTransaction transaction = AAIGraph.getInstance().getGraph().newTransaction();
        GraphTraversalSource g = transaction.traversal();
        
        // pass in a bad vertex Id when collecting data
    
        String [] args = {
        		"-action",
                "DELETE_NODE",
                "-userId",
                "someuser",
                "-vertexId",
                "555"
        };
        
        deleteTool.main(args);
        assertThat(outputCapture.toString(), containsString("Vertex with vertexId = 555 not found"));
             
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