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

import java.io.*;
import java.nio.charset.StandardCharsets;
import java.util.*;

import org.apache.tinkerpop.gremlin.process.traversal.dsl.graph.GraphTraversal;
import org.apache.tinkerpop.gremlin.process.traversal.dsl.graph.GraphTraversalSource;
import org.apache.tinkerpop.gremlin.process.traversal.dsl.graph.__;
import org.apache.tinkerpop.gremlin.structure.Vertex;
import org.janusgraph.core.JanusGraph;
import org.janusgraph.core.JanusGraphTransaction;
import org.janusgraph.core.PropertyKey;
import org.janusgraph.core.schema.JanusGraphIndex;
import org.janusgraph.core.schema.JanusGraphManagement;
import org.janusgraph.graphdb.database.StandardJanusGraph;
import org.janusgraph.graphdb.database.management.ManagementSystem;
import org.onap.aai.db.props.AAIProperties;
import org.onap.aai.dbgen.SchemaGenerator;
import org.onap.aai.dbmap.AAIGraph;
import org.onap.aai.exceptions.AAIException;
import org.onap.aai.logging.ErrorLogHelper;
import org.onap.aai.setup.SchemaVersion;
import org.onap.aai.util.AAIConfig;
import org.onap.aai.util.AAIConstants;
import org.onap.aai.util.ExceptionTranslator;
import org.onap.aai.util.GraphAdminDBUtils;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.context.annotation.AnnotationConfigApplicationContext;

public class GenTester {

    private static Logger LOGGER;
    private static boolean historyEnabled;
    private static boolean success = true;
    private static SchemaVersion version;
    private static final String SCHEMA_INITIALIZED  = "schema-initialized";
    static GraphTraversal<Vertex, Vertex> widgetModelTraversal;
    @Value("${aai.root.model.path}")
    static String modelPath;
    /**
     * The main method.
     *
     * @param args the arguments
     */
    public static void main(String[] args) throws AAIException {

        try {
            createSchema(args);
        } catch (Exception e) {
            LOGGER.error("Failed to run schema creation", e);
            System.exit(1);
        }

        LOGGER.debug("All done, if the program does not exit, please kill it manually.");
        System.exit(0);
    }

    private static void createSchema(String[] args) throws AAIException {
        LOGGER = LoggerFactory.getLogger(GenTester.class);
        JanusGraph graph = null;
        System.setProperty("aai.service.name", GenTester.class.getSimpleName());
        LOGGER.info("Inside createSchema with args");
        boolean addDefaultCR = true;

        AnnotationConfigApplicationContext ctx = new AnnotationConfigApplicationContext();
        try {
            ctx.scan(
                    "org.onap.aai");
            ctx.refresh();
        } catch (Exception e) {
            AAIException aai = ExceptionTranslator.schemaServiceExceptionTranslator(e);
            LOGGER.error("Problems running the tool " + aai.getMessage());
            ErrorLogHelper.logError(aai.getCode(), e.getMessage() + ", resolve and retry");
            throw aai;
        }
        historyEnabled = Boolean.parseBoolean(ctx.getEnvironment().getProperty("history.enabled", "false"));
        if (historyEnabled) {
            String amsg = "GenTester may only be used when history.enabled=false. ";
            System.out.println(amsg);
            LOGGER.debug(amsg);
            return;
        }
        try {
            LOGGER.debug("GenTester uses either cql jar or Cassandra jar");

            AAIConfig.init();
            if (args != null && args.length > 0) {
                if ("genDbRulesOnly".equals(args[0])) {
                    ErrorLogHelper.logError("AAI_3100",
                            " This option is no longer supported. What was in DbRules is now derived from the OXM files. ");
                    return;
                } else if ("GEN_DB_WITH_NO_SCHEMA".equals(args[0])) {
                    // Note this is done to create an empty DB with no Schema so that
                    // an HBase copyTable can be used to set up a copy of the db.
                    String imsg = "    ---- NOTE --- about to load a graph without doing any schema processing (takes a little while) --------   ";
                    System.out.println(imsg);
                    LOGGER.debug(imsg);
                    graph = AAIGraph.getInstance().getGraph();

                    if (graph == null) {
                        ErrorLogHelper.logError("AAI_5102", "Error creating JanusGraph graph.");
                        return;
                    } else {
                        LOGGER.debug("Successfully loaded a JanusGraph graph without doing any schema work.");
                        return;
                    }
                } else if ("GEN_DB_WITH_NO_DEFAULT_CR".equals(args[0])) {
                    addDefaultCR = false;
                } else {
                    ErrorLogHelper.logError("AAI_3000",
                            "Unrecognized argument passed to GenTester.java: [" + args[0] + "]. ");

                    String emsg = "Unrecognized argument passed to GenTester.java: [" + args[0] + "]. ";
                    System.out.println(emsg);
                    LOGGER.error(emsg);

                    emsg = "Either pass no argument for normal processing, or use 'GEN_DB_WITH_NO_SCHEMA'.";
                    System.out.println(emsg);
                    LOGGER.error(emsg);

                    return;
                }
            }

            // AAIConfig.init();
            ErrorLogHelper.loadProperties();

            LOGGER.debug("about to open graph (takes a little while)");
            graph = AAIGraph.getInstance().getGraph();

            if (graph == null) {
                ErrorLogHelper.logError("AAI_5102", "Error creating JanusGraph graph. ");
                return;
            }
            LOGGER.info("checking schemaInitializedVertex...");
            Optional<Vertex> schemaInitializedVertex = graph.traversal().V()
                    .or(
                            __.has(SCHEMA_INITIALIZED, true),
                            __.has(SCHEMA_INITIALIZED, false)
                    )
                    .tryNext();

            if (schemaInitializedVertex.isPresent()) {
                //Set schema-initialized vertex to false if such vertex is present in db
                setSchemaInitializedToFalse(graph, schemaInitializedVertex);
            } else {
                // Creating a new vertex as the vertex is not yet present in db
                LOGGER.info("checking schemaInitializedVertex...");
                createNewSchemaInitializedVertex(graph);
            }

            GraphAdminDBUtils.logConfigs(graph.configuration());

            LOGGER.debug("-- Loading new schema elements into JanusGraph --");

            boolean dbNotEmpty = (graph.traversal().V().limit(1).hasNext());
            LOGGER.info("DB is not empty. Newly created indexes will also be reindexed.");
            List<String> vertexesToReindex = SchemaGenerator.loadSchemaIntoJanusGraph(graph, null, dbNotEmpty);
            LOGGER.debug("-- committing transaction ");
            graph.tx().commit();

            boolean reindexingEnabled = false; // disable reindexing for now, since it's not working correctly
            if (reindexingEnabled && !vertexesToReindex.isEmpty()) {
                killTransactionsAndInstances(graph);
                LOGGER.info("Number of edge indexes to reindex: " + vertexesToReindex.size());
                SchemaGenerator.reindexEdgeIndexes(graph, vertexesToReindex);
            } else {
                if (vertexesToReindex.isEmpty()) {
                    LOGGER.info("Nothing to reindex.");
                }
            }

            // Setting property schema-initialized to true
            LOGGER.info("-- Updating vertex with property schema-initialized to true ");
            graph.traversal().V().has(SCHEMA_INITIALIZED , false).property(SCHEMA_INITIALIZED , true).next();
            LOGGER.debug("-- committing transaction ");
            graph.tx().commit();
            applyLockConsistency(graph);
            loadRootModels(graph);
            graph.close();
            LOGGER.info("Closed the graph");

        } catch (Exception ex) {
            ErrorLogHelper.logError("AAI_4000", ex.getMessage());
            System.exit(1);
        }
    }

    /**
     * Enforces ConsistencyModifier.LOCK on all vertex composite indexes.
     * This ensures index-level consistency after schema creation.
     */
    private static void applyLockConsistency(JanusGraph graph) {
        // Read from environment variable injected by Helm
        boolean lockEnabled = Boolean.parseBoolean(
                Optional.ofNullable(System.getenv("AAI_INDEX_LOCK_ENABLED"))
                        .orElse("false")
        );
        LOGGER.info("=== Enforcing ConsistencyModifier.LOCK on all composite indexes === LOCK ENABLED: {}",lockEnabled);

        JanusGraphManagement mgmt = graph.openManagement();
        try {
            Iterator<JanusGraphIndex> indexes = mgmt.getGraphIndexes(Vertex.class).iterator();
            while (indexes.hasNext()) {
                JanusGraphIndex index = indexes.next();

                if (index.isCompositeIndex()) {
                    try {

                        if (lockEnabled) {
                            mgmt.setConsistency(index, org.janusgraph.core.schema.ConsistencyModifier.LOCK);
                            LOGGER.info("Successfully set LOCK for index: {}", index.name());
                        }else{
                            mgmt.setConsistency(index, org.janusgraph.core.schema.ConsistencyModifier.DEFAULT);
                            LOGGER.info("Successfully set DEFAULT for index: {}", index.name());
                        }
                    } catch (Exception e) {
                        LOGGER.warn("Failed to set LOCK for index {}: {}", index.name(), e.getMessage());
                    }
                } else {
                    LOGGER.debug("Skipping non-composite index: {}", index.name());
                }
            }

            mgmt.commit();
            LOGGER.info("Committed LOCK consistency for all applicable indexes.");
        } catch (Exception e) {
            LOGGER.error("Error while applying ConsistencyModifier.LOCK: {}", e.getMessage(), e);
            mgmt.rollback();
        }
    }

    public static Map<String, Map<String, String>> loadModelProperties(String filePath) {
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

    public static void loadRootModels(JanusGraph graph) {
        if (checkDBContainsModelData(graph)) {
            LOGGER.info("Model data already present in DB, skipping root model loading.");
            return;
        }

        long startTime = System.currentTimeMillis();
        String filePath = AAIConstants.AAI_HOME_ETC_APP_PROPERTIES + "artifact-generator.properties";
        LOGGER.info("Loading root models from filePath: {}", filePath);

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

                String modelInvariantId = ids.get("model-invariant-id");
                String modelVersionId = ids.get("model-version-id");
                String modelType = ids.getOrDefault("model-type", "widget");

                if (modelInvariantId == null || modelVersionId == null) {
                    LOGGER.warn("Skipping {} — missing model IDs", modelName);
                    continue;
                }

                // Create or fetch model vertex
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

                // Create or fetch model-version vertex
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

                // Create hierarchical edges if missing
                // service-design-and-creation → model
                if (!g.V(rootModel).out("models").has("model-invariant-id", modelInvariantId).hasNext()) {
                    g.addE("models").from(rootModel).to(modelVertex).next();
                    LOGGER.info("Linked rootModel → model {}", modelInvariantId);
                }

                // model → model-ver
                if (!g.V(modelVertex).out("model-vers").has("model-version-id", modelVersionId).hasNext()) {
                    g.addE("model-vers").from(modelVertex).to(modelVerVertex).next();
                    LOGGER.info("Linked model → model-ver {}", modelVersionId);
                }
                counter++;
                totalInserted++;

                //Commit periodically for large datasets
                if (counter >= BATCH_SIZE) {
                    tx.commit();
                    LOGGER.info("Committed batch of {} models ({} total so far)", counter, totalInserted);
                }
            }

            tx.commit();
            LOGGER.info("Final commit — total models inserted/updated: {}", totalInserted);

        } catch (Exception e) {
            LOGGER.error("Error inserting/updating model data", e);
            if (tx != null && tx.isOpen()) tx.rollback();
        } finally {
            if (tx != null && tx.isOpen()) tx.close();
        }

        long elapsed = System.currentTimeMillis() - startTime;
        LOGGER.info("Model data loading completed in {} ms, total models inserted: {}", elapsed, totalInserted);
    }

    private static boolean checkDBContainsModelData(JanusGraph graph) {
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

    private static void setSchemaInitializedToFalse(JanusGraph graph, Optional<Vertex> schemaInitializedVertex) {
        Vertex vertex = schemaInitializedVertex.get();
        Object schemaInitializedValueObj = vertex.property(SCHEMA_INITIALIZED).value();
        Boolean schemaInitializedValue = schemaInitializedValueObj instanceof Boolean b ? b : Boolean.FALSE;

        //Setting schema-initialized vertex to False
        if (Boolean.TRUE.equals(schemaInitializedValue)) {
            // Update the property from true to false
            LOGGER.debug("-- Vertex with property 'schema-initialized' present in db and is true. Updating it to false");
            graph.traversal().V()
                    .has(SCHEMA_INITIALIZED, true)
                    .property(SCHEMA_INITIALIZED, false)
                    .next();
        } else {
            // Property already false, no action needed
            LOGGER.debug("-- Vertex with property 'schema-initialized' present in db and is false. Keeping it false. Do Nothing");
        }
    }

    private static void createNewSchemaInitializedVertex(JanusGraph graph) throws Exception {
        LOGGER.debug("-- Adding a new vertex with property schema-initialized as false");
        JanusGraphManagement mgmt = graph.openManagement();
        try {
            // Creating an index
            createSchemaInitializedIndex(graph, mgmt);
        } catch (Exception e) {
            mgmt.rollback();
            LOGGER.error("Problems creating an index for schema-initialized vertex " + e.getMessage());
            throw e;
        }
        try {
            Vertex newVertex = graph.addVertex(SCHEMA_INITIALIZED , false);
            LOGGER.info("Created a new vertex with property '{}' set to '{}'", SCHEMA_INITIALIZED ,
                    newVertex.property(SCHEMA_INITIALIZED ).value());
        } catch (Exception e) {
            LOGGER.error("Error creating a new vertex: {}", e.getMessage(), e);
            throw e;
        }
    }

    private static void createSchemaInitializedIndex(JanusGraph graph, JanusGraphManagement mgmt) throws InterruptedException {
        // creating a composite index
        boolean indexExists = mgmt.containsGraphIndex(SCHEMA_INITIALIZED);
        if(indexExists) {
            LOGGER.debug(SCHEMA_INITIALIZED + " index already exists. Skipping creation.");
            return;
        }
        LOGGER.debug("-- Building an index on property schema-initialized");
        PropertyKey schemaInitialized = mgmt.makePropertyKey(SCHEMA_INITIALIZED).dataType(Boolean.class).make();
        mgmt.buildIndex(SCHEMA_INITIALIZED, Vertex.class)
                .addKey(schemaInitialized)
                .buildCompositeIndex();
        mgmt.commit();

        // Wait for the index to become available
        ManagementSystem.awaitGraphIndexStatus(graph, SCHEMA_INITIALIZED).call();
    }

    /**
     * Radical approach to avoiding index update failures.
     * Indexes can get stuck in INSTALLED state, when there are stale transactions
     * or JanusGraph instances.
     * This is because a state change needs to be acknowledged by all instances
     * before transitioning.
     *
     * @param graph
     * @return
     */
    private static void killTransactionsAndInstances(JanusGraph graph) {
        graph.tx().rollback();
        final StandardJanusGraph janusGraph = (StandardJanusGraph) graph;
        janusGraph.getOpenTransactions().stream().forEach(transaction -> {
            LOGGER.debug("Closing open transaction [{}] before schema generation", transaction.toString());
            transaction.rollback();
        });

        final JanusGraphManagement graphMgtForClosing = graph.openManagement();

        Set<String> instances = graphMgtForClosing.getOpenInstances();
        LOGGER.info("Number of open instances: {}", instances.size());
        LOGGER.info("Currently open instances: [{}]", instances);
        instances.stream()
                .filter(instance -> !instance.contains("graphadmin")) // Potentially comment this out, should there be
                // issues with the schema creation job
                .filter(instance -> !instance.contains("(current)"))
                .forEach(instance -> {
                    LOGGER.debug("Closing open JanusGraph instance [{}] before reindexing procedure", instance);
                    graphMgtForClosing.forceCloseInstance(instance);
                });
        graphMgtForClosing.commit();
    }

}
