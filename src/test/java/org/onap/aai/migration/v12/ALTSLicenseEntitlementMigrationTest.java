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

import static org.junit.jupiter.api.Assertions.*;
import static org.mockito.Mockito.spy;
import static org.mockito.Mockito.when;

import java.io.UnsupportedEncodingException;
import org.apache.tinkerpop.gremlin.process.traversal.dsl.graph.GraphTraversalSource;
import org.apache.tinkerpop.gremlin.structure.Vertex;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.onap.aai.AAISetup;
import org.onap.aai.db.props.AAIProperties;
import org.onap.aai.dbmap.DBConnectionType;
import org.onap.aai.introspection.Loader;
import org.onap.aai.introspection.LoaderFactory;
import org.onap.aai.introspection.ModelType;
import org.onap.aai.setup.SchemaVersions;
import org.onap.aai.setup.SchemaVersion;
import org.onap.aai.serialization.engines.QueryStyle;
import org.onap.aai.serialization.engines.JanusGraphDBEngine;
import org.onap.aai.serialization.engines.TransactionalGraphEngine;
import org.janusgraph.core.JanusGraphFactory;
import org.janusgraph.core.JanusGraph;
import org.janusgraph.core.JanusGraphTransaction;

public class ALTSLicenseEntitlementMigrationTest extends AAISetup {
    private final static ModelType introspectorFactoryType = ModelType.MOXY;
    private final static QueryStyle queryStyle = QueryStyle.TRAVERSAL;

    private Loader loader;
    private TransactionalGraphEngine dbEngine;
    private JanusGraph graph;
    private ALTSLicenseEntitlementMigration migration;
    private GraphTraversalSource g;
    private JanusGraphTransaction tx;

    @BeforeEach
    public void setUp() throws Exception {
        graph = JanusGraphFactory.build().set("storage.backend","inmemory").open();
        tx = graph.newTransaction();
        g = tx.traversal();
        loader = loaderFactory.createLoaderForVersion(introspectorFactoryType, schemaVersions.getDefaultVersion());
        System.setProperty("AJSC_HOME", ".");
        System.setProperty("BUNDLECONFIG_DIR", "src/test/resources");
        dbEngine = new JanusGraphDBEngine(
                queryStyle,
                loader);

        Vertex vnf = g.addV().property("aai-node-type", "generic-vnf")
                .property("vnf-id", "123456789")
                .property("vnf-name", "test-vnf-name")
                .property("equipment-role", "UCPE")
                .next();

        Vertex entitlement = g.addV().property("aai-node-type", "entitlement")
                .property("group-uuid", "guuid-entitlement")
                .property("resource-uuid", "ruuid-entitlement")
                .property("aai-uri", "/network/generic-vnfs/generic-vnf/123456789/entitlements/entitlement/ruuideuuid/ruuid-entitlement")
                .next();

        Vertex license = g.addV().property("aai-node-type", "license")
                .property("group-uuid", "guuid-license")
                .property("resource-uuid", "ruuid-license")
                .property("aai-uri", "/network/generic-vnfs/generic-vnf/123456789/licenses/license/ruuideuuid/ruuid-license")
                .next();

        Vertex vnf2 = g.addV().property("aai-node-type", "generic-vnf")
                .property("vnf-id", "23456789")
                .property("vnf-name", "test-vnf-name")
                .property("equipment-role", "UCPE")
                .next();
        Vertex duplicateEntitlement = g.addV().property("aai-node-type", "entitlement")
                .property("group-uuid", "guuid")
                .property("resource-uuid", "ruuid-entitlement")
                .property("aai-uri", "/network/generic-vnfs/generic-vnf/123456789/entitlements/entitlement/ruuideuuid/ruuid-entitlement")
                .next();

        Vertex duplicateLicense = g.addV().property("aai-node-type", "license")
                .property("group-uuid", "guuid")
                .property("resource-uuid", "ruuid-license")
                .property("aai-uri", "/network/generic-vnfs/generic-vnf/123456789/licenses/license/ruuideuuid/ruuid-license")
                .next();



        edgeSerializer.addTreeEdge(g, vnf, license);
        edgeSerializer.addTreeEdge(g, vnf, entitlement);
        edgeSerializer.addTreeEdge(g, vnf2, duplicateEntitlement);
        edgeSerializer.addTreeEdge(g, vnf2, duplicateLicense);

        TransactionalGraphEngine spy = spy(dbEngine);
        TransactionalGraphEngine.Admin adminSpy = spy(dbEngine.asAdmin());
        GraphTraversalSource traversal = g;
        when(spy.asAdmin()).thenReturn(adminSpy);
        when(adminSpy.getTraversalSource()).thenReturn(traversal);
        migration = new ALTSLicenseEntitlementMigration(spy, loaderFactory, edgeIngestor, edgeSerializer, schemaVersions);
        migration.run();
    }

    @AfterEach
    public void cleanUp() {
        tx.rollback();
        graph.close();
    }

    @Test
    public void testEntitlementsUpdated() throws UnsupportedEncodingException {
        assertEquals((Long)1L,
                g.V().has("aai-node-type", "generic-vnf").has("vnf-id", "123456789").in("org.onap.relationships.inventory.BelongsTo").has("aai-node-type", "entitlement").count().next(),
                "Found 1 entitlement");
        assertEquals(true,
                g.V().has("aai-node-type", "generic-vnf").has("vnf-id", "123456789").in("org.onap.relationships.inventory.BelongsTo").has("aai-node-type", "entitlement").has("resource-uuid", "new-ruuid-entitlement").hasNext(),
                "Entitlement's resource-uuid is updated ");
        assertEquals(true,
                g.V().has("aai-node-type", "generic-vnf").has("vnf-id", "123456789").in("org.onap.relationships.inventory.BelongsTo")
                .has("aai-node-type", "entitlement").has("resource-uuid", "new-ruuid-entitlement").has("last-mod-source-of-truth", "ALTSLicenseEntitlementMigration").hasNext(),
                "Entitlement's resource-uuid is updated by migration ");
    }
    @Test
    public void testLicensesUpdated() throws UnsupportedEncodingException {
        assertEquals((Long)1L,
                g.V().has("aai-node-type", "generic-vnf").has("vnf-id", "123456789").in("org.onap.relationships.inventory.BelongsTo").has("aai-node-type", "license").count().next(),
                "Found 1 License");
        assertEquals(true,
                g.V().has("aai-node-type", "generic-vnf").has("vnf-id", "123456789").in("org.onap.relationships.inventory.BelongsTo").has("aai-node-type", "license").has("resource-uuid", "new-ruuid-license").hasNext(),
                "License's resource-uuid is updated ");
    }

    @Test
    public void verifyUri() {
        assertEquals("/network/generic-vnfs/generic-vnf/123456789/entitlements/entitlement/ruuideuuid/new-ruuid-entitlement",
                g.V().has("aai-node-type", "generic-vnf").has("vnf-id", "123456789").in("org.onap.relationships.inventory.BelongsTo").has("aai-node-type", "entitlement").has("resource-uuid", "new-ruuid-entitlement").next().property(AAIProperties.AAI_URI).value(),
                "Uri should be updated");
        assertEquals("/network/generic-vnfs/generic-vnf/123456789/licenses/license/ruuideuuid/new-ruuid-license",
                g.V().has("aai-node-type", "generic-vnf").has("vnf-id", "123456789").in("org.onap.relationships.inventory.BelongsTo").has("aai-node-type", "license").has("resource-uuid", "new-ruuid-license").next().property(AAIProperties.AAI_URI).value(),
                "Uri should be updated");
    }

    @Test
    public void duplicateGroupUuid() {
        Long count = g.V().has("aai-node-type", "generic-vnf").has("vnf-id", "23456789").in("org.onap.relationships.inventory.BelongsTo").has("aai-node-type", "license").has("resource-uuid", "new-ruuid-license2").count().next() +
                g.V().has("aai-node-type", "generic-vnf").has("vnf-id", "23456789").in("org.onap.relationships.inventory.BelongsTo").has("aai-node-type", "entitlement").has("resource-uuid", "new-ruuid-entitlement2").count().next();
        assertEquals((Long)1L, count, "Duplicate Entitlement or License Group Uuid should be skipped");


    }
}
