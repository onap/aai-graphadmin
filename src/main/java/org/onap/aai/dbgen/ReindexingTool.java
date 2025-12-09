/**
 * ============LICENSE_START=======================================================
 * org.onap.aai
 * ================================================================================
 * Copyright © 2025 Deutsche Telekom. All rights reserved.
 * ================================================================================
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 * <p>
 * http://www.apache.org/licenses/LICENSE-2.0
 * <p>
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 * ============LICENSE_END=========================================================
 */
package org.onap.aai.dbgen;

import org.apache.commons.configuration2.PropertiesConfiguration;
import org.apache.commons.configuration2.ex.ConfigurationException;
import org.apache.tinkerpop.gremlin.structure.Vertex;
import org.janusgraph.core.JanusGraph;
import org.janusgraph.core.JanusGraphFactory;
import org.janusgraph.core.schema.JanusGraphIndex;
import org.janusgraph.core.schema.JanusGraphManagement;
import org.janusgraph.core.schema.SchemaAction;
import org.janusgraph.core.schema.SchemaStatus;
import org.janusgraph.graphdb.database.management.ManagementSystem;
import org.onap.aai.dbmap.AAIGraphConfig;
import org.onap.aai.edges.EdgeIngestor;
import org.onap.aai.exceptions.AAIException;
import org.onap.aai.logging.ErrorLogHelper;
import org.onap.aai.serialization.engines.TransactionalGraphEngine;
import org.onap.aai.setup.SchemaVersions;
import org.onap.aai.util.AAIConstants;
import org.onap.aai.util.AAISystemExitUtil;
import org.onap.aai.util.ExceptionTranslator;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.slf4j.MDC;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.context.annotation.AnnotationConfigApplicationContext;

import java.io.FileNotFoundException;
import java.time.temporal.ChronoUnit;
import java.util.HashSet;
import java.util.Set;

public class ReindexingTool {

    protected TransactionalGraphEngine engine;
    private String indexNameParam = null;
    @Autowired
    protected SchemaVersions schemaVersions;
    @Autowired
    protected EdgeIngestor edgeIngestor;
    private static final String REALTIME_DB = "realtime";

    private static Logger logger = LoggerFactory.getLogger(ReindexingTool.class);

    /**
     * The main method.
     *
     * @param args the arguments
     */
    public static void main(String[] args) throws AAIException, InterruptedException {

        System.setProperty("aai.service.name", ReindexingTool.class.getSimpleName());
        MDC.put("logFilenameAppender", ReindexingTool.class.getSimpleName());

        AnnotationConfigApplicationContext ctx = new AnnotationConfigApplicationContext();
        try {
            ctx.scan(
                    "org.onap.aai"
            );
            ctx.refresh();
        } catch (Exception e) {
            AAIException aai = ExceptionTranslator.schemaServiceExceptionTranslator(e);
            logger.error("Problems running ReindexingTool: {} ", aai.getMessage());
            ErrorLogHelper.logError(aai.getCode(), e.getMessage() + ", resolve and retry");
            throw aai;
        }
        ReindexingTool reindexingTool = new ReindexingTool();

        reindexingTool.execute(args);
        AAISystemExitUtil.systemExitCloseAAIGraph(0);
    }

    private boolean shouldExitVm = true;

    public void exit(int statusCode) {
        if (this.shouldExitVm) {
            System.exit(1);
        }
    }

    public void execute(String[] args) throws InterruptedException {
        for (int i = 0; i < args.length; i++) {
            if (args[i].equalsIgnoreCase("-indexNames")) {
                i++;
                if (i >= args.length) {
                    logger.error(" No value passed with -indexName option.  ");
                    throw new RuntimeException(" No value passed with -indexName option.  ");
//                    exit(0);
                }
                indexNameParam = args[i];
                System.out.println(">>>> indexNameParam:"+indexNameParam);
                if (null == indexNameParam || indexNameParam.isEmpty()) {
                    logger.error("IndexName is empty");
                    throw new RuntimeException("IndexName is empty");
                }
            } else if (args[i].equalsIgnoreCase("-fullReindex")) {
                fullReindex();
            }
        }
        if (null == indexNameParam || indexNameParam.isEmpty()) {
            logger.error("IndexName is empty");
            throw new RuntimeException("IndexName is empty");
        }else if (indexNameParam.contains(",")) {
            String[] indexes = indexNameParam.split(",");
            for (String indexName : indexes) {
                reindexByName(indexName);
            }
        } else {
            reindexByName(indexNameParam);
        }
    }

    public Set<String> getListOfIndexes(){
        final String rtConfig = AAIConstants.REALTIME_DB_CONFIG;
        final String serviceName = System.getProperty("aai.service.name", ReindexingTool.class.getSimpleName());
        Set<String> indexSet = new HashSet<>();
        try {
            PropertiesConfiguration graphConfig = new AAIGraphConfig.Builder(rtConfig)
                    .forService(serviceName)
                    .withGraphType(REALTIME_DB)
                    .buildConfiguration();
            try (JanusGraph janusGraph = JanusGraphFactory.open(graphConfig)) {
                JanusGraphManagement mgmt = janusGraph.openManagement();

                for (JanusGraphIndex index : mgmt.getGraphIndexes(Vertex.class)) {
                    indexSet.add(index.name());
                }
            }
        } catch (ConfigurationException | FileNotFoundException e) {
            logger.error("Failed to load graph configuration: {}", e.getMessage(), e);
        } catch (Exception e) {
            logger.error("Unexpected error while fetching indexes : {}", e.getMessage(), e);
        }
        return indexSet;
    }

    private void fullReindex() throws InterruptedException {
        final String rtConfig = AAIConstants.REALTIME_DB_CONFIG;
        final String serviceName = System.getProperty("aai.service.name", ReindexingTool.class.getSimpleName());

        try {
            PropertiesConfiguration graphConfig = new AAIGraphConfig.Builder(rtConfig)
                    .forService(serviceName)
                    .withGraphType(REALTIME_DB)
                    .buildConfiguration();

            try (JanusGraph janusGraph = JanusGraphFactory.open(graphConfig)) {
                JanusGraphManagement mgmt = janusGraph.openManagement();

                for (JanusGraphIndex index : mgmt.getGraphIndexes(Vertex.class)) {
                    mgmt.updateIndex(index, SchemaAction.REINDEX);
                    mgmt.commit();
                    try {
                        // Wait for the index to reach REGISTERED before enabling
                        ManagementSystem.awaitGraphIndexStatus(janusGraph, indexNameParam)
                                .status(SchemaStatus.REGISTERED)
                                .timeout(10, ChronoUnit.MINUTES)
                                .call();

                        logger.info("Index is now in REGISTERED state: {}", indexNameParam);
                    } catch (Exception e) {
                        logger.error("Error while waiting for index '{}' to register: {}", indexNameParam, e.getMessage(), e);
                        throw e;
                    }
                }
            }
        } catch (ConfigurationException | FileNotFoundException e) {
            logger.error("Failed to load graph configuration: {}", e.getMessage(), e);
        } catch (Exception e) {
            logger.error("Unexpected error while reindexing '{}': {}", indexNameParam, e.getMessage(), e);
        }
    }

    public void reindexByName(String indexNameParam) throws InterruptedException {
        final String rtConfig = AAIConstants.REALTIME_DB_CONFIG;
        final String serviceName = System.getProperty("aai.service.name", ReindexingTool.class.getSimpleName());

        try {
            PropertiesConfiguration graphConfig = new AAIGraphConfig.Builder(rtConfig)
                    .forService(serviceName)
                    .withGraphType(REALTIME_DB)
                    .buildConfiguration();

            try (JanusGraph janusGraph = JanusGraphFactory.open(graphConfig)) {
                JanusGraphManagement mgmt = janusGraph.openManagement();
                JanusGraphIndex index = mgmt.getGraphIndex(indexNameParam);
                if (index == null) {
                    logger.warn("Index not found: " + indexNameParam);
                    mgmt.rollback();
                    return;
                }
                logger.info("Reindexing index: " + index.name());
                mgmt.updateIndex(index, SchemaAction.REINDEX);
                mgmt.commit();

                try {
                    // Wait for the index to reach REGISTERED before enabling
                    ManagementSystem.awaitGraphIndexStatus(janusGraph, indexNameParam)
                            .status(SchemaStatus.REGISTERED)
                            .timeout(10, ChronoUnit.MINUTES)
                            .call();

                    logger.info("Index is now in REGISTERED state: {}", indexNameParam);
                } catch (Exception e) {
                    logger.error("Error while waiting for index '{}' to register: {}", indexNameParam, e.getMessage(), e);
                    throw e;
                }

            }
        } catch (ConfigurationException | FileNotFoundException e) {
            logger.error("Failed to load graph configuration: {}", e.getMessage(), e);
        } catch (Exception e) {
            logger.error("Unexpected error while reindexing '{}': {}", indexNameParam, e.getMessage(), e);
        }
    }


}
