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
import org.apache.tinkerpop.gremlin.process.traversal.dsl.graph.__;
import org.apache.tinkerpop.gremlin.structure.Vertex;
import org.onap.aai.edges.EdgeIngestor;
import org.onap.aai.exceptions.AAIException;
import org.onap.aai.introspection.LoaderFactory;
import org.onap.aai.migration.*;
import org.onap.aai.edges.enums.EdgeType;
import org.onap.aai.serialization.db.EdgeSerializer;
import org.onap.aai.serialization.engines.TransactionalGraphEngine;
import org.onap.aai.setup.SchemaVersions;

import java.util.*;

@MigrationPriority(1)
@MigrationDangerRating(1)
//@Enabled
public class SDWANSpeedChangeMigration extends Migrator {

    private final String PARENT_NODE_TYPE = "alloted-resource";
    private boolean success = true;

    Vertex allottedRsrcVertex;

    Map<String, String> bandwidthMap = new HashMap<>();
    Set<String> bandWidthSet = new HashSet<>();

    GraphTraversal<Vertex, Vertex> allottedRsrcTraversal;
    GraphTraversal<Vertex, Vertex> tunnelXConnectTraversal;
    GraphTraversal<Vertex, Vertex> pinterfaceTraversal;
    GraphTraversal<Vertex, Vertex> plinkTraversal;

    public SDWANSpeedChangeMigration(TransactionalGraphEngine engine, LoaderFactory loaderFactory, EdgeIngestor edgeIngestor, EdgeSerializer edgeSerializer, SchemaVersions schemaVersions) {
        super(engine, loaderFactory, edgeIngestor, edgeSerializer, schemaVersions);
        bandWidthSet.add("bandwidth-up-wan1");
        bandWidthSet.add("bandwidth-down-wan1");
        bandWidthSet.add("bandwidth-up-wan2");
        bandWidthSet.add("bandwidth-down-wan2");
    }


    @Override
    public void run() {

        logger.info("Started the migration "+ getMigrationName());

        try {

            allottedRsrcTraversal = this.engine.asAdmin().getTraversalSource().V()
                    .has("aai-node-type", "service-subscription")
                    .has("service-type", "DHV")
                    .in("org.onap.relationships.inventory.BelongsTo")
                    .has("aai-node-type", "service-instance")
                    .out("org.onap.relationships.inventory.Uses")
                    .has("aai-node-type", "allotted-resource")
                    .where(
                            this.engine.getQueryBuilder()
                                    .createEdgeTraversal(EdgeType.TREE, "allotted-resource", "service-instance")
                                    .createEdgeTraversal(EdgeType.TREE, "service-instance", "service-subscription")
                                    .<GraphTraversal<Vertex, Vertex>>getQuery()
                                    .has("service-type", "VVIG")
                    );

            if(!(allottedRsrcTraversal.hasNext())){

                logger.info("unable to find allotted resource to DHV as cousin and child of VVIG");
            }

            while (allottedRsrcTraversal.hasNext()) {
                bandwidthMap.clear();

                allottedRsrcVertex = allottedRsrcTraversal.next();
                String allottedResourceId = allottedRsrcVertex.property("id").value().toString();
                logger.info("Found an allotted resource with id " + allottedResourceId);

                tunnelXConnectTraversal = this.engine.asAdmin().getTraversalSource()
                        .V(allottedRsrcVertex)
                        .in("org.onap.relationships.inventory.BelongsTo")
                        .has("aai-node-type", "tunnel-xconnect");

                if (tunnelXConnectTraversal != null && tunnelXConnectTraversal.hasNext()) {
                    Vertex xConnect = tunnelXConnectTraversal.next();
                    String tunnelId = xConnect.property("id").value().toString();
                    logger.info("Found an tunnelxconnect object with id " + tunnelId);
                    extractBandwidthProps(xConnect);
                    modifyPlink(allottedRsrcVertex);
                } else {
                    logger.info("Unable to find the tunnel connect for the current allotted resource traversal");
                }

            }
        } catch (AAIException e) {
            success = false;
        }

        logger.info("Successfully finished the " + getMigrationName());
    }

    public void extractBandwidthProps(Vertex vertex) {
        logger.info("Trying to extract bandwith props");
        bandWidthSet.stream().forEach((key) -> {
            if (vertex.property(key).isPresent()) {
                bandwidthMap.put(key, vertex.property(key).value().toString());
            }
        });
        logger.info("Extracted bandwith props for tunnelXConnect " +vertex.value("id"));
    }

    public void modifyPlink(Vertex v) {

        try {
            pinterfaceTraversal = this.engine.asAdmin().getTraversalSource().V(v)
                    .in("org.onap.relationships.inventory.Uses").has("aai-node-type", "service-instance")
                    .where(
                            __.out("org.onap.relationships.inventory.BelongsTo")
                                    .has("aai-node-type", "service-subscription")
                                    .has("service-type", "DHV")
                    )
                    .out("org.onap.relationships.inventory.ComposedOf").has("aai-node-type", "generic-vnf")
                    .out("tosca.relationships.HostedOn").has("aai-node-type", "vserver")
                    .out("tosca.relationships.HostedOn").has("aai-node-type", "pserver")
                    .in("tosca.relationships.network.BindsTo").has("aai-node-type", "p-interface");
        } catch (Exception e) {
            logger.info("error trying to find p interfaces");
        }


        while (pinterfaceTraversal.hasNext()) {

            Vertex pInterfaceVertex = pinterfaceTraversal.next();

            String pinterfaceName = pInterfaceVertex.property("interface-name").value().toString();
            logger.info("p-interface "+ pinterfaceName + " found from traversal from allotted-resource " +v.value("id"));
            String[] parts = pinterfaceName.split("/");

            if (parts[parts.length - 1].equals("10")) {

                logger.info("Found the pinterface with the interface name ending with /10");

                try {
                    plinkTraversal = this.engine.asAdmin().getTraversalSource()
                            .V(pInterfaceVertex)
                            .out("tosca.relationships.network.LinksTo")
                            .has("aai-node-type", "physical-link");
                } catch (Exception e) {
                    logger.info("error trying to find the p Link for /10");
                }
                if (plinkTraversal != null && plinkTraversal.hasNext()) {
                    Vertex pLink = plinkTraversal.next();


                    if ( bandwidthMap.containsKey("bandwidth-up-wan1")
                            && bandwidthMap.containsKey("bandwidth-down-wan1")
                            && !(("").equals(bandwidthMap.get("bandwidth-up-wan1").replaceAll("[^0-9]", "").trim()))
                            && !(("").equals(bandwidthMap.get("bandwidth-down-wan1").replaceAll("[^0-9]", "").trim())))
                    {

                        pLink.property("service-provider-bandwidth-up-value", Integer.valueOf(bandwidthMap.get("bandwidth-up-wan1").replaceAll("[^0-9]", "").trim()));
                        pLink.property("service-provider-bandwidth-up-units", "Mbps");
                        pLink.property("service-provider-bandwidth-down-value", Integer.valueOf(bandwidthMap.get("bandwidth-down-wan1").replaceAll("[^0-9]", "").trim()));
                        pLink.property("service-provider-bandwidth-down-units", "Mbps");
                        logger.info("Successfully modified the plink with link name ", pLink.property("link-name").value().toString());
                        this.touchVertexProperties(pLink, false);
                    } else {
                        logger.info("missing up and down vals for the plink with link name ", pLink.property("link-name").value().toString());
                    }


                } else {
                    logger.info("missing plink for p interface" + pinterfaceName);
                }

            }

            if (parts[parts.length - 1].equals("11")) {

                logger.info("Found the pinterface with the interface name ending with /11");
                try {
                    plinkTraversal = this.engine.asAdmin()
                            .getTraversalSource()
                            .V(pInterfaceVertex)
                            .out("tosca.relationships.network.LinksTo")
                            .has("aai-node-type", "physical-link");
                } catch (Exception e) {
                    logger.info("error trying to find the p Link for /11");
                }

                if (plinkTraversal != null && plinkTraversal.hasNext()) {
                    Vertex pLink = plinkTraversal.next();


                    if ( bandwidthMap.containsKey("bandwidth-up-wan2")
                            && bandwidthMap.containsKey("bandwidth-down-wan2")
                            && !(("").equals(bandwidthMap.get("bandwidth-up-wan2").replaceAll("[^0-9]", "").trim()))
                            && !(("").equals(bandwidthMap.get("bandwidth-down-wan2").replaceAll("[^0-9]", "").trim())))
                    {
                        pLink.property("service-provider-bandwidth-up-value", Integer.valueOf(bandwidthMap.get("bandwidth-up-wan2").replaceAll("[^0-9]", "").trim()));
                        pLink.property("service-provider-bandwidth-up-units", "Mbps");
                        pLink.property("service-provider-bandwidth-down-value", Integer.valueOf(bandwidthMap.get("bandwidth-down-wan2").replaceAll("[^0-9]", "").trim()));
                        pLink.property("service-provider-bandwidth-down-units", "Mbps");
                        logger.info("Successfully modified the plink with link name ", pLink.property("link-name").value().toString());
                        this.touchVertexProperties(pLink, false);
                    } else {
                       logger.error("missing up and down vals for the plink with link name ", pLink.property("link-name").value().toString());
                    }

                } else {
                    logger.info("missing plink for p interface" + pinterfaceName);
                }
            }
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

        return Optional.of(new String[]{PARENT_NODE_TYPE});
    }

    @Override
    public String getMigrationName() {
        return "SDWANSpeedChangeMigration";
    }


}
