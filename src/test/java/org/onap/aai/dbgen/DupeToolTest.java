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
import org.apache.tinkerpop.gremlin.process.traversal.dsl.graph.GraphTraversalSource;
import org.apache.tinkerpop.gremlin.structure.Vertex;
import org.junit.After;
import org.junit.Before;
import org.junit.Test;
import org.onap.aai.AAISetup;
import org.onap.aai.dbmap.AAIGraph;

import static org.hamcrest.CoreMatchers.is;
import static org.hamcrest.MatcherAssert.assertThat;
import static org.junit.Assert.*;

public class DupeToolTest extends AAISetup {

    private static final Logger logger = LoggerFactory.getLogger(DupeToolTest.class);

    private DupeTool dupeTool;

    @Before
    public void setup(){
        dupeTool = new DupeTool(loaderFactory, schemaVersions, false);
        createGraph();
    }

    private void createGraph() {

        JanusGraphTransaction transaction = AAIGraph.getInstance().getGraph().newTransaction();

        boolean success = true;

        try {

            GraphTraversalSource g = transaction.traversal();
            
            Vertex pserverVertex = g.addV()
                    .property("aai-node-type", "pserver")
                    .property("hostname", "test-pserver")
                    .property("in-maint", false)
                    .property("source-of-truth", "JUNIT")
                    .next();

            // Dupe set #1
            Vertex pInterfaceVertex1 = g.addV()
                    .property("aai-node-type", "p-interface")
                    .property("interface-name", "p-interface-name1")
                    .property("in-maint", false)
                    .property("source-of-truth", "JUNIT")
                    .next();
            edgeSerializer.addTreeEdge(g, pserverVertex, pInterfaceVertex1);
                
            Vertex pInterfaceVertex2 = g.addV()
                    .property("aai-node-type", "p-interface")
                    .property("interface-name", "p-interface-name1")
                    .property("in-maint", false)
                    .property("source-of-truth", "JUNIT")
                    .next();
            edgeSerializer.addTreeEdge(g, pserverVertex, pInterfaceVertex2);
            
            // Dupe Set #2
            Vertex pInterfaceVertex3 = g.addV()
                    .property("aai-node-type", "p-interface")
                    .property("interface-name", "p-interface-name2")
                    .property("in-maint", false)
                    .property("source-of-truth", "JUNIT")
                    .next();
            edgeSerializer.addTreeEdge(g, pserverVertex, pInterfaceVertex3);
                
            Vertex pInterfaceVertex4 = g.addV()
                    .property("aai-node-type", "p-interface")
                    .property("interface-name", "p-interface-name2")
                    .property("in-maint", false)
                    .property("source-of-truth", "JUNIT")
                    .next();
            edgeSerializer.addTreeEdge(g, pserverVertex, pInterfaceVertex4);
            
         // Dupe Set #3
            Vertex pInterfaceVertex5 = g.addV()
                    .property("aai-node-type", "p-interface")
                    .property("interface-name", "p-interface-name3")
                    .property("in-maint", false)
                    .property("source-of-truth", "JUNIT")
                    .next();
            edgeSerializer.addTreeEdge(g, pserverVertex, pInterfaceVertex5);
                
            Vertex pInterfaceVertex6 = g.addV()
                    .property("aai-node-type", "p-interface")
                    .property("interface-name", "p-interface-name3")
                    .property("in-maint", false)
                    .property("source-of-truth", "JUNIT")
                    .next();
            edgeSerializer.addTreeEdge(g, pserverVertex, pInterfaceVertex6);


            Vertex pInterfaceVertex7 = g.addV()
                .property("aai-node-type", "p-interface")
                .property("interface-name", "p-interface-name3")
                .property("in-maint", false)
                .property("source-of-truth", "JUNIT")
                .next();
            edgeSerializer.addTreeEdge(g, pserverVertex, pInterfaceVertex7);

            // Now add a few more pservers  - they won't be dupes because the db won't let us
            Vertex pserverVertexX = g.addV()
                    .property("aai-node-type", "pserver")
                    .property("hostname", "test-pserverX")
                    .property("in-maint", false)
                    .property("source-of-truth", "JUNIT")
                    .next();

            Vertex pserverVertexY = g.addV()
                    .property("aai-node-type", "pserver")
                    .property("hostname", "test-pserverY")
                    .property("in-maint", false)
                    .property("source-of-truth", "JUNIT")
                    .next();
      

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
    public void testDupeToolForPInterface(){
        
        String[] args = {
                "-userId", "testuser",
                "-nodeType", "p-interface",
                "-timeWindowMinutes", "30",
                "-maxFix", "30",
                "-sleepMinutes", "0"
        };

        dupeTool.execute(args);
        assertThat(dupeTool.getDupeGroupCount(), is(3));
        
    }


    @Test
    public void testDupeToolForPInterfaceWithAutoFixOn(){
        
        String[] args = {
                "-userId", "testuser",
                "-nodeType", "p-interface",
                "-timeWindowMinutes", "30",
                "-maxFix", "30",
                "-sleepMinutes", "5",
                "-autoFix"
        };

        dupeTool.execute(args);
        assertThat(dupeTool.getDupeGroupCount(), is(3));
        
    }


    @Test
    public void testDupeToolForPServer(){
	    
	String[] args = {
                "-userId", "testuser",
                "-nodeType", "pserver",
                "-timeWindowMinutes", "30",
                "-maxFix", "30",
                "-sleepMinutes", "0"
        };
     
        dupeTool.execute(args);
        assertThat(dupeTool.getDupeGroupCount(), is(0));
        
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
