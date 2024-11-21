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
package org.onap.aai.rest;

import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertTrue;
import static org.junit.jupiter.api.Assertions.fail;

import org.apache.tinkerpop.gremlin.process.traversal.dsl.graph.GraphTraversalSource;
import org.janusgraph.core.JanusGraphTransaction;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.onap.aai.AAISetup;
import org.onap.aai.dbmap.AAIGraph;
import org.onap.aai.exceptions.AAIException;
import org.onap.aai.service.SchemaJobStatusService;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class SchemaJobStatusServiceTest extends AAISetup {

    private static final Logger logger = LoggerFactory.getLogger(SchemaJobStatusServiceTest.class);

    private SchemaJobStatusService schemaJobStatusService;

    @BeforeEach
    public void setup(){
        schemaJobStatusService = new SchemaJobStatusService();
    }

    private void createGraph(boolean initialized) {
       JanusGraphTransaction transaction = AAIGraph.getInstance().getGraph().newTransaction();
        boolean success = true;
        try {
            GraphTraversalSource g = transaction.traversal();
            g.addV().property("schema-initialized", initialized)
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

    private void createGraphWithoutSchemaVertex() {
        JanusGraphTransaction transaction = AAIGraph.getInstance().getGraph().newTransaction();
         boolean success = true;
         try {
            GraphTraversalSource g = transaction.traversal();
        
              g.addV().property("aai-node-type", "pserver")
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
    public void testSchemaInitializedTrue() throws AAIException{   
        createGraph(true);
        boolean result = schemaJobStatusService.isSchemaInitialized();    
        assertTrue(result);
    }

    @Test
    public void testSchemaInitializedFalse() throws AAIException{   
        createGraph(false);
        boolean result = schemaJobStatusService.isSchemaInitialized();    
        assertFalse(result);
    }

    @Test
    public void testVertexNotPresent() throws AAIException{   
        createGraphWithoutSchemaVertex();
        boolean result = schemaJobStatusService.isSchemaInitialized();    
        assertFalse(result);
    }

    @AfterEach
    public void tearDown(){
        JanusGraphTransaction transaction = AAIGraph.getInstance().getGraph().newTransaction();
        boolean success = true;
        try {
            GraphTraversalSource g = transaction.traversal();
            g.V().has("schema-initialized")
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
