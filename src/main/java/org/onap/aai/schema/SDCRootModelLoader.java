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

package org.onap.aai.schema;

import org.apache.tinkerpop.gremlin.process.traversal.dsl.graph.GraphTraversalSource;
import org.apache.tinkerpop.gremlin.structure.Vertex;
import org.janusgraph.core.JanusGraph;
import org.janusgraph.core.JanusGraphTransaction;
import org.onap.aai.db.props.AAIProperties;
import org.onap.aai.dbmap.AAIGraph;
import org.onap.aai.exceptions.AAIException;
import org.onap.aai.logging.ErrorLogHelper;
import org.onap.aai.util.AAIConfig;
import org.onap.aai.util.AAIConstants;
import org.onap.aai.util.ExceptionTranslator;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.context.annotation.AnnotationConfigApplicationContext;

import java.io.*;
import java.nio.charset.StandardCharsets;
import java.util.*;

/*
* Class SDCRootModelLoader loads the root models (model invariant-ids and model-version-ids ) through 
* Artifact-Generator.properties file in to AAI DB
*/
public class SDCRootModelLoader {
    private static Logger LOGGER;

    public static void main(String[] args) throws AAIException {
        LOGGER = LoggerFactory.getLogger(SDCRootModelLoader.class);
        try {
            loadRootModels();
        } catch (Exception e) {
            AAIException aai = ExceptionTranslator.schemaServiceExceptionTranslator(e);
            LOGGER.error("Problems during loading root models " + aai.getStackTrace());
            throw aai;
        }
        LOGGER.debug("All done, if the program does not exit, please kill it manually.");
        System.exit(0);
    }

    private static void loadRootModels() throws AAIException {
        JanusGraph graph = AAIGraph.getInstance().getGraph();
        if (graph == null) {
            ErrorLogHelper.logError("AAI_5102", "Error creating JanusGraph graph. ");
            return;
        }
        if (checkDBContainsModelData(graph)) {
            LOGGER.info("Model data already present in DB, skipping root model loading.");
            return;
        }

        long startTime = System.currentTimeMillis();
        // This file with the root model details is referenced & taken from SDC under
        // below repository path
        // https://gerrit.onap.org/r/gitweb?p=sdc.git;a=blob;f=catalog-be/src/main/resources/config/Artifact-Generator.properties
        String filePath = AAIConstants.AAI_HOME_ETC_APP_PROPERTIES + "artifact-generator.properties";

        Map<String, Map<String, String>> modelData = loadModelProperties(filePath);
        final int BATCH_SIZE = 100;
        int counter = 0;
        int totalInserted = 0;

        JanusGraphTransaction tx = graph.newTransaction();
        final GraphTraversalSource g = tx.traversal();

        try {
            Vertex rootModel = g.V()
                    .has("aai-node-type", "service-design-and-creation")
                    .tryNext()
                    .orElseGet(() -> g.addV("service-design-and-creation")
                            .property("aai-node-type", "service-design-and-creation")
                            .next());

            for (Map.Entry<String, Map<String, String>> entry : modelData.entrySet()) {

                String modelName = entry.getKey();
                Map<String, String> ids = entry.getValue();
                // loading single model data (invariant-id, version-id)
                loadSingleModelEntry(g, tx, rootModel, modelName, ids);

                counter++;
                totalInserted++;
                if (counter >= BATCH_SIZE) {
                    tx.commit();
                    LOGGER.info("Committed batch of {} models ({} total so far)", counter, totalInserted);
                    counter = 0;
                }
            }

            tx.commit();
            LOGGER.info("Final commit — total models inserted/updated: {}", totalInserted);

        } catch (Exception e) {
            LOGGER.error("Error inserting/updating model data", e);
            if (tx != null && tx.isOpen())
                tx.rollback();
        } finally {
            if (tx != null && tx.isOpen())
                tx.close();
        }
        graph.close();
        long elapsed = System.currentTimeMillis() - startTime;
        LOGGER.info("Model data loading completed in {} ms, total models inserted: {}", elapsed, totalInserted);
    }

    private static void loadSingleModelEntry(GraphTraversalSource g, JanusGraphTransaction tx, Vertex rootModel,
            String modelName, Map<String, String> ids) {

        String modelInvariantId = ids.get("model-invariant-id");
        String modelVersionId = ids.get("model-version-id");
        String modelType = ids.getOrDefault("model-type", "widget");

        if (modelInvariantId == null || modelVersionId == null) {
            LOGGER.warn("Skipping {} — missing model IDs", modelName);
            return;
        }

        // --- Create or fetch model vertex ---
        Vertex modelVertex = g.V()
                .has("aai-node-type", "model")
                .has("model-invariant-id", modelInvariantId)
                .tryNext()
                .orElseGet(() -> {
                    long currentTimeModelInvariant = System.currentTimeMillis();
                    Vertex v = g.addV("model")
                            .property("aai-node-type", "model")
                            .property("model-invariant-id", modelInvariantId)
                            .property("model-type", modelType)
                            .property("aai-uuid", UUID.randomUUID().toString())
                            .property("model-name", modelName)
                            .property("source-of-truth", "ModelLoaderTool")
                            .property("last-mod-source-of-truth", "ModelLoaderTool")
                            .property("aai-created-ts", currentTimeModelInvariant)
                            .property("aai-last-mod-ts", currentTimeModelInvariant)
                            .property("aai-uri", String.format(
                                    "/service-design-and-creation/models/model/%s", modelInvariantId))
                            .property(AAIProperties.RESOURCE_VERSION, String.valueOf(currentTimeModelInvariant))
                            .next();
                    LOGGER.info("Created model vertex for invariantId: {}", modelInvariantId);
                    return v;
                });

        // --- Create or fetch model-version vertex ---
        Vertex modelVerVertex = g.V()
                .has("aai-node-type", "model-ver")
                .has("model-version-id", modelVersionId)
                .tryNext()
                .orElseGet(() -> {
                    long currentTimeModelVersion = System.currentTimeMillis();
                    Vertex v = g.addV("model-ver")
                            .property("aai-node-type", "model-ver")
                            .property("model-version-id", modelVersionId)
                            .property("model-name", modelName)
                            .property("aai-uuid", UUID.randomUUID().toString())
                            .property("source-of-truth", "ModelLoaderTool")
                            .property("last-mod-source-of-truth", "ModelLoaderTool")
                            .property("aai-created-ts", currentTimeModelVersion)
                            .property("model-version", "2.0")
                            .property("aai-last-mod-ts", currentTimeModelVersion)
                            .property("aai-uri", String.format(
                                    "/service-design-and-creation/models/model/%s/model-vers/model-ver/%s",
                                    modelInvariantId, modelVersionId))
                            .property(AAIProperties.RESOURCE_VERSION, String.valueOf(currentTimeModelVersion))
                            .next();
                    LOGGER.info("Created model-version vertex: {}", modelVersionId);
                    return v;
                });

        if (!g.V(rootModel).out("models").has("model-invariant-id", modelInvariantId).hasNext()) {
            g.addE("models").from(rootModel).to(modelVertex).next();
            LOGGER.info("Linked rootModel → model {}", modelInvariantId);
        }
        if (!g.V(modelVertex).out("model-vers").has("model-version-id", modelVersionId).hasNext()) {
            g.addE("model-vers").from(modelVertex).to(modelVerVertex).next();
            LOGGER.info("Linked model → model-ver {}", modelVersionId);
        }
    }

    private static boolean checkDBContainsModelData(JanusGraph graph) throws AAIException {
        try (JanusGraphTransaction tx = graph.newTransaction()) {
            GraphTraversalSource g = tx.traversal();
            boolean hasWidgetModel = g.V()
                    .has("aai-node-type", "model")
                    .has("model-type", "widget")
                    .hasNext();
            tx.commit();
            return hasWidgetModel;
        } catch (Exception e) {
            LOGGER.error("Error during model data loading", e);
            return false;
        }
    }

    private static Map<String, Map<String, String>> loadModelProperties(String filePath) throws AAIException {
        Map<String, Map<String, String>> modelData = new LinkedHashMap<>();
        Properties props = new Properties();

        try (BufferedReader reader = new BufferedReader(new FileReader(filePath))) {
            StringBuilder cleanContent = new StringBuilder();
            String line;
            boolean insideCommentBlock = false;

            while ((line = reader.readLine()) != null) {
                line = line.trim();

                if (line.startsWith("{{/*")) {
                    insideCommentBlock = true;
                    continue;
                } else if (line.endsWith("*/}}")) {
                    insideCommentBlock = false;
                    continue;
                }

                if (insideCommentBlock || line.isEmpty() || line.startsWith("#")) {
                    continue;
                }
                cleanContent.append(line).append(System.lineSeparator());
            }

            try (InputStream is = new ByteArrayInputStream(cleanContent.toString().getBytes(StandardCharsets.UTF_8))) {
                props.load(is);
            }

        } catch (IOException e) {
            LOGGER.error("Error reading properties file: {}", filePath, e);
            return Collections.emptyMap();
        }

        // Parse AAI model properties: e.g. AAI.model-version-id.action=UUID
        for (String key : props.stringPropertyNames()) {
            String value = props.getProperty(key).trim();
            String[] parts = key.split("\\.");

            if (parts.length == 3) {
                String idType = parts[1];
                String nodeType = parts[2];
                modelData.computeIfAbsent(nodeType, k -> new HashMap<>())
                        .put(idType, value);
            } else {
                LOGGER.warn("Skipping invalid key: {}", key);
            }
        }

        LOGGER.info("Loaded {} model entries from {}", modelData.size(), filePath);
        for (Map.Entry<String, Map<String, String>> entry : modelData.entrySet()) {
            LOGGER.debug("→ nodeType={}, model-invariant-id={}, model-version-id={}",
                    entry.getKey(),
                    entry.getValue().get("model-invariant-id"),
                    entry.getValue().get("model-version-id"));
        }

        return modelData;
    }
}
