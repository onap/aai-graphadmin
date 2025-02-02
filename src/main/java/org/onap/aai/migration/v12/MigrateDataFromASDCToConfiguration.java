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
package org.onap.aai.migration.v12;

import org.apache.tinkerpop.gremlin.process.traversal.dsl.graph.GraphTraversal;
import org.apache.tinkerpop.gremlin.structure.Vertex;
import org.onap.aai.db.props.AAIProperties;
import org.onap.aai.edges.EdgeIngestor;
import org.onap.aai.introspection.LoaderFactory;
import org.onap.aai.migration.MigrationDangerRating;
import org.onap.aai.migration.MigrationPriority;
import org.onap.aai.migration.Migrator;
import org.onap.aai.migration.Status;
import org.onap.aai.serialization.db.EdgeSerializer;
import org.onap.aai.serialization.engines.TransactionalGraphEngine;
import org.onap.aai.setup.SchemaVersions;
import org.onap.aai.util.AAIConstants;

import java.io.*;
import java.util.Optional;

@MigrationPriority(20)
@MigrationDangerRating(2)
//@Enabled
public class MigrateDataFromASDCToConfiguration extends Migrator {
    private final String PARENT_NODE_TYPE = "generic-vnf";
    private boolean success = true;
    private String entitlementPoolUuid = "";
    private String VNT = "";


    public MigrateDataFromASDCToConfiguration(TransactionalGraphEngine engine, LoaderFactory loaderFactory, EdgeIngestor edgeIngestor, EdgeSerializer edgeSerializer, SchemaVersions schemaVersions) {
        super(engine, loaderFactory, edgeIngestor, edgeSerializer, schemaVersions);
    }


    @Override
    public void run() {

        String homeDir = System.getProperty("AJSC_HOME");
        String configDir = System.getProperty("BUNDLECONFIG_DIR");

        String csvFile = homeDir + AAIConstants.AAI_FILESEP  + configDir
                + AAIConstants.AAI_FILESEP + "migration-input-files"
                + AAIConstants.AAI_FILESEP + "VNT-migration-data" +
                  AAIConstants.AAI_FILESEP + "VNT-migration-input.csv";

        logger.info("Reading Csv file: " + csvFile);
        BufferedReader br = null;
        String line = "";
        String cvsSplitBy = "\t";
        try {

            br = new BufferedReader(new FileReader(new File(csvFile)));
            while ((line = br.readLine()) != null) {
                line = line.replaceAll("\"", "");
                String[] temp = line.split(cvsSplitBy);
                if ("entitlement-pool-uuid".equals(temp[0]) || "vendor-allowed-max-bandwidth (VNT)".equals(temp[1])) {
                    continue;
                }
                entitlementPoolUuid = temp[0];
                VNT = temp[1];
                GraphTraversal<Vertex, Vertex> f = this.engine.asAdmin().getTraversalSource().V().has(AAIProperties.NODE_TYPE, "entitlement").has("group-uuid", entitlementPoolUuid)
                        .out("org.onap.relationships.inventory.BelongsTo").has(AAIProperties.NODE_TYPE, "generic-vnf")
                        .has("vnf-type", "HN").in("org.onap.relationships.inventory.ComposedOf").has(AAIProperties.NODE_TYPE, "service-instance").out("org.onap.relationships.inventory.Uses").has(AAIProperties.NODE_TYPE, "configuration");
                
                modify(f);
            }

        } catch (FileNotFoundException e) {
            success = false;
            logger.error("Found Exception" , e);
        } catch (IOException e) {
            success = false;
            logger.error("Found Exception" , e);
        } catch (Exception a) {
            success= false;
            logger.error("Found Exception" , a);
        } finally {
            try {
                if(br !=null) br.close();
            } catch (IOException e) {
                success = false;
                logger.error("Found Exception" , e);
            }
        }

    }

    public void modify(GraphTraversal<Vertex, Vertex> g) {
        int count = 0;
        while (g.hasNext()) {
            Vertex v = g.next();
            logger.info("Found node type " + v.property("aai-node-type").value().toString() + " with configuration id:  " + v.property("configuration-id").value().toString());
            v.property("vendor-allowed-max-bandwidth", VNT);
            logger.info("VNT val after migration: " + v.property("vendor-allowed-max-bandwidth").value().toString());
            count++;
        }

        logger.info("modified " + count + " configuration nodes related to Entitlement UUID: " +entitlementPoolUuid);

    }

    @Override
    public Status getStatus() {
        if (success) {
            return Status.SUCCESS;
        } else {
            return Status.FAILURE;
        }
    }

    @Override
    public Optional<String[]> getAffectedNodeTypes() {
        return Optional.of(new String[]{PARENT_NODE_TYPE});
    }

    @Override
    public String getMigrationName() {
        return "MigrateDataFromASDCToConfiguration";
    }


}
