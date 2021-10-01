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
import java.io.BufferedReader;
import java.io.FileNotFoundException;
import java.io.FileReader;
import java.io.IOException;
import java.util.*;

import org.apache.tinkerpop.gremlin.process.traversal.dsl.graph.GraphTraversal;
import org.apache.tinkerpop.gremlin.process.traversal.dsl.graph.GraphTraversalSource;
import org.apache.tinkerpop.gremlin.structure.Vertex;
import org.onap.aai.migration.MigrationDangerRating;
import org.onap.aai.migration.MigrationPriority;
import org.onap.aai.db.props.AAIProperties;
import org.onap.aai.migration.Migrator;
import org.onap.aai.migration.Status;
import org.onap.aai.edges.enums.EdgeType;
import org.onap.aai.serialization.engines.TransactionalGraphEngine;
import org.onap.aai.edges.EdgeIngestor;
import org.onap.aai.serialization.db.EdgeSerializer;
import org.onap.aai.introspection.LoaderFactory;
import org.onap.aai.setup.SchemaVersions;


@MigrationPriority(11)
@MigrationDangerRating(0)
public class ALTSLicenseEntitlementMigration extends Migrator{

    private final String LICENSE_NODE_TYPE = "license";
    private final String ENTITLEMENT_NODE_TYPE = "entitlement";
    private boolean success = true;
    private final GraphTraversalSource g;
    private int headerLength;


    public ALTSLicenseEntitlementMigration(TransactionalGraphEngine engine, LoaderFactory loaderFactory, EdgeIngestor edgeIngestor, EdgeSerializer edgeSerializer, SchemaVersions schemaVersions) {
        super(engine, loaderFactory, edgeIngestor, edgeSerializer, schemaVersions);
        this.g = this.engine.asAdmin().getTraversalSource();
    }

    @Override
    public void run() {
        logger.info("---------- Update ALTS Entitlements and Licenses resource-uuid in generic-vnf  ----------");
        String homeDir = System.getProperty("AJSC_HOME");
        String configDir = System.getProperty("BUNDLECONFIG_DIR");
        if (homeDir == null) {
            logger.info("ERROR: Could not find sys prop AJSC_HOME");
            success = false;
            return;
        }
        if (configDir == null) {
            success = false;
            return;
        }
        int fileLineCounter = 0;
        String fileName = homeDir + "/" + configDir + "/" + "migration-input-files/ALTS-migration-data/ALTS-migration-input.csv";
        Map<String, Set<String>> history = new HashMap<>();
        logger.info(fileName);
        logger.info("---------- Processing VNFs from file  ----------");
        try (BufferedReader br = new BufferedReader(new FileReader(fileName))) {
            String vnfLine;
            while ((vnfLine = br.readLine()) != null) {
                vnfLine = vnfLine.replace("\n", "").replace("\r", "");
                logger.info("\n");
                if (!vnfLine.isEmpty()) {
                    if (fileLineCounter != 0) {
                        String[] fields = vnfLine.split(",", -1);
                        if (fields.length != this.headerLength) {
                            logger.info("ERROR: Vnf line should contain " + this.headerLength + " columns, contains " + fields.length + " instead.");
                            success = false;
                            continue;
                        }
                        String newResourceUuid = fields[0].trim();
                        String groupUuid = fields[1].trim();
                        String vnfId = fields[19].trim();
                        logger.info("---------- Processing Line " + vnfLine + "----------");
                        logger.info("newResourceUuid = " + newResourceUuid + " vnfId = " + vnfId + " group uuid = " + groupUuid);
                        if (history.containsKey(vnfId)){
                            if (history.get(vnfId).contains(groupUuid)){
                                logger.info("ERROR: duplicate groupUuid in vnf - skipping");
                                fileLineCounter++;
                                continue;
                            }
                            else{
                                history.get(vnfId).add(groupUuid);
                            }
                        }
                        else {
                            Set<String> newSet = new HashSet<>();
                            newSet.add(groupUuid);
                            history.put(vnfId, newSet);
                        }
                        List<Vertex> entitlements = g.V().has(AAIProperties.NODE_TYPE, "entitlement").has("group-uuid", groupUuid)
                                .where(this.engine.getQueryBuilder().createEdgeTraversal(EdgeType.TREE, "entitlement", "generic-vnf").getVerticesByProperty("vnf-id", vnfId)
                                        .<GraphTraversal<?, ?>>getQuery()).toList();

                        List<Vertex> licenses = g.V().has(AAIProperties.NODE_TYPE, "license").has("group-uuid", groupUuid)
                                .where(this.engine.getQueryBuilder().createEdgeTraversal(EdgeType.TREE, "license", "generic-vnf").getVerticesByProperty("vnf-id", vnfId)
                                        .<GraphTraversal<?, ?>>getQuery()).toList();

                        this.ChangeResourceUuid(entitlements, newResourceUuid, "entitlements", vnfId, groupUuid);
                        this.ChangeResourceUuid(licenses, newResourceUuid, "license", vnfId, groupUuid);

                    } else {
                        this.headerLength = vnfLine.split(",", -1).length;
                        logger.info("headerLength: " + headerLength);
                        if (this.headerLength < 22){
                            logger.info("ERROR: Input file should have 22 columns");
                            this.success = false;
                            return;
                        }
                    }
                }
                fileLineCounter++;
            }
        } catch (FileNotFoundException e) {
            logger.info("ERROR: Could not find file " + fileName, e);
            success = false;
        } catch (IOException e) {
            logger.info("ERROR: Issue reading file " + fileName, e);
            success = false;
        } catch (Exception e) {
            logger.info("encountered exception", e);
            success = false;
        }
    }

    private void ChangeResourceUuid(List<Vertex> vertices, String newResourceUuid, String nodeType, String vnfId, String groupUuid){
        if (vertices.size() > 1) {
           logger.info("\t More than 1 " + nodeType + "found, skipping");
           return;
        }
        else if (vertices.size() == 1) {
            try {
                logger.info(String.format("Updating %s with groupUuid %s from generic-vnf with vnfId %s with newResourceUuid %s", nodeType, groupUuid, vnfId, newResourceUuid));
                Vertex v = vertices.get(0);
                String resourceUuid = v.<String>property("resource-uuid").value();
                logger.info("\tOriginal resource-uuid: " + resourceUuid);
                v.property("resource-uuid", newResourceUuid);

                String aaiUri = v.<String>property(AAIProperties.AAI_URI).value();
                if (aaiUri != null) {
                    logger.info("\tOriginal aaiUri: " + aaiUri);
                    aaiUri = aaiUri.replaceFirst("[^/]*"+resourceUuid + "$", newResourceUuid);
                    v.property(AAIProperties.AAI_URI, aaiUri);
                    logger.info("\tNew aaiUri: " + v.value(AAIProperties.AAI_URI).toString());
                }
                
                this.touchVertexProperties(v, false);
                logger.info("\tNew resource-uuid: " + newResourceUuid);
            }
            catch (Exception e){
                logger.info("\t ERROR: caught exception: " + e.getMessage());
            }
        }
        else {
            logger.info("\t No " + nodeType + " found with group-uuid "+ groupUuid + " for generic-vnf " +vnfId);
            return;
        }
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
        return Optional.of(new String[]{LICENSE_NODE_TYPE, ENTITLEMENT_NODE_TYPE});
    }

    @Override
    public String getMigrationName() {
        return "ALTSLicenseEntitlementMigration";
    }

}
