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

import com.att.eelf.configuration.EELFLogger;
import com.att.eelf.configuration.EELFManager;
import org.janusgraph.core.JanusGraphTransaction;
import org.apache.tinkerpop.gremlin.process.traversal.dsl.graph.GraphTraversalSource;
import org.apache.tinkerpop.gremlin.structure.Vertex;
import org.junit.After;
import org.junit.Before;
import org.junit.Test;
import org.onap.aai.AAISetup;
import org.onap.aai.dbmap.AAIGraph;

import static org.junit.Assert.*;

public class DupeToolTest extends AAISetup {

    private static final EELFLogger logger = EELFManager.getInstance().getLogger(DupeToolTest.class);

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

            Vertex cloudRegionVertex = g.addV()
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

            for(int i = 0; i < 100; ++i){
                g.addV()
                        .property("aai-node-type", "p-interface")
                        .property("interface-name", "p-interface-name")
                        .property("in-maint", false)
                        .property("source-of-truth", "JUNIT")
                        .next();
            }

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
    public void testDupeToolForPInterface(){
        //TODO: test does not find duplicates
        String[] args = {
                "-userId", "testuser",
                "-nodeType", "p-interface",
                "-timeWindowMinutes", "30",
                "-autoFix",
                "-maxFix", "30",
                "-sleepMinutes", "0"
        };

        dupeTool.execute(args);
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