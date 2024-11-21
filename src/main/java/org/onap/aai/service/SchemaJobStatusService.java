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
package org.onap.aai.service;

import org.apache.tinkerpop.gremlin.process.traversal.dsl.graph.GraphTraversalSource;
import org.janusgraph.core.JanusGraph;
import org.onap.aai.dbmap.AAIGraph;
import org.onap.aai.exceptions.AAIException;
import org.onap.aai.logging.ErrorLogHelper;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.stereotype.Service;


@Service
public class SchemaJobStatusService {
    private static final Logger LOGGER = LoggerFactory.getLogger(SchemaJobStatusService.class);

    public boolean isSchemaInitialized() throws AAIException {
        JanusGraph graph = AAIGraph.getInstance().getGraph();
        if (graph == null) {
            ErrorLogHelper.logError("AAI_5102", "Error creating JanusGraph graph.");
            throw new AAIException("AAI_5102", "Graph instance is null.");
        }
        LOGGER.debug("Successfully loaded a JanusGraph graph.");

        GraphTraversalSource g = graph.traversal();
        try {
            // Check if there is a vertex with the property "schema-initialized" set to "true"
            return g.V().has("schema-initialized", "true").hasNext();
        } catch (Exception e) {
            LOGGER.error("Error during schema initialization check", e);
            throw new AAIException("Error checking schema initialization: " + e.getMessage(), e);

        }finally {
            try {
                g.close();
            } catch (Exception e) {
                LOGGER.error("Failed to close GraphTraversalSource", e);
            }
            LOGGER.info("Closed the traversal source");
        }
    }
}
