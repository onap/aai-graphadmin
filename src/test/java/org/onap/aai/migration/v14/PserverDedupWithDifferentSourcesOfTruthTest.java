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
package org.onap.aai.migration.v14;

import static org.junit.Assert.*;
import static org.mockito.Mockito.spy;
import static org.mockito.Mockito.when;

import java.util.List;

import org.apache.tinkerpop.gremlin.process.traversal.dsl.graph.GraphTraversalSource;
import org.apache.tinkerpop.gremlin.process.traversal.strategy.verification.ReadOnlyStrategy;
import org.apache.tinkerpop.gremlin.structure.Vertex;
import org.junit.*;
import org.onap.aai.AAISetup;
import org.onap.aai.dbmap.DBConnectionType;
import org.onap.aai.introspection.Loader;
import org.onap.aai.introspection.ModelType;
import org.onap.aai.serialization.engines.QueryStyle;
import org.onap.aai.serialization.engines.JanusGraphDBEngine;
import org.onap.aai.serialization.engines.TransactionalGraphEngine;
import org.janusgraph.core.JanusGraphFactory;
import org.janusgraph.core.JanusGraph;
import org.janusgraph.core.JanusGraphTransaction;

public class  PserverDedupWithDifferentSourcesOfTruthTest extends AAISetup{

    private final static ModelType introspectorFactoryType = ModelType.MOXY;
    private final static QueryStyle queryStyle = QueryStyle.TRAVERSAL;

    Loader loader;
    TransactionalGraphEngine dbEngine;
    JanusGraph graph;
    PserverDedupWithDifferentSourcesOfTruth migration;
    JanusGraphTransaction tx;
    GraphTraversalSource g;

//scn1
    Vertex pIntRo;
    Vertex lInterfaceRo;
    Vertex pserverRCT;
    Vertex complexRO;
//scn2

    Vertex pserverRCTScn2;
    Vertex pIntRoScn2;
    Vertex lInterfaceRoScn2;
    Vertex complexROScn2;
    Vertex lInterfaceRctScn2;
    Vertex pIntRctScn2;
    Vertex complexRctScn2;
    
    //physical link
    Vertex pintPlinkScn1;
    Vertex samePintScn4RO;
    Vertex samePintScn4RCT;
    Vertex pserverRCTPlinkScn4;
    
    //Scn3
    Vertex pserverRCTScn3;
    Vertex complexScn3;
    Vertex pserverROScn3;
    
    //ManyToOne edge scenario
    Vertex pserverRCTScn6;
    Vertex pserverROScn6;
    Vertex zoneScn61;
    Vertex zoneScn62;

    @Before
    public void setUp() throws Exception {
        graph = JanusGraphFactory.build().set("storage.backend","inmemory").open();
        tx = graph.newTransaction();
        g = tx.traversal();
        loader = loaderFactory.createLoaderForVersion(introspectorFactoryType,schemaVersions.getDefaultVersion());
        dbEngine = new JanusGraphDBEngine(
                queryStyle,
                loader);
//Scn1 empty RCT move everything over
        pserverRCT = g.addV().property("aai-node-type", "pserver")
                .property("hostname","pserverRCT")
                .property("source-of-truth","RCT")
                .property("fqdn","tttt.bbbb.cccc.dddd")
                .property("aai-uri","/cloud-infrastructure/pservers/pserver/pserverRCT")
                .property("resource-version","1")
                .next();

        Vertex pserverRO = g.addV().property("aai-node-type", "pserver")
                .property("hostname","tttt.RoHostname")
                .property("source-of-truth","RO")
                .property("aai-uri","/cloud-infrastructure/pservers/pserver/tttt.RoHostname")
                .property("resource-version","2")
                .next();
        pIntRo = g.addV()
                .property("aai-node-type", "p-interface")
                .property("interface-name", "pIntRo")
                .property("aai-uri","/cloud-infrastructure/pservers/pserver/tttt.RoHostname/p-interfaces/p-interface/pIntRo")
                .next();
        lInterfaceRo = g.addV().property("aai-node-type", "l-interface")
                .property("interface-name", "lInterfaceRo")
                .property("aai-uri","/cloud-infrastructure/pservers/pserver/tttt.RoHostname/p-interfaces/p-interface/pIntRo/l-interfaces/l-interface/lInterfaceRo")
                .next();
        complexRO = g.addV().property("aai-node-type", "complex")
                .property("physical-location-id","complexRO")
                .property("aai-uri","/cloud-infrastructure/complexes/complex/complexRO")
                .next();
        
        // physical-link tests
        //1. p-int does not exist on RCT, p-int and p-link moves from RO to RCT
        pintPlinkScn1= g.addV()
                .property("aai-node-type", "p-interface")
                .property("interface-name", "pintPlinkScn1")
                .property("aai-uri","/cloud-infrastructure/pservers/pserver/tttt.RoHostname/p-interfaces/p-interface/pintPlinkScn1")
                .next();

        Vertex pLinkScn1 = g.addV().property("aai-node-type", "physical-link")
                .property("link-name", "pLinkScn1")
                .property("service-provider-bandwidth-up-value", 0)
                .property("service-provider-bandwidth-up-units", "empty")
                .property("service-provider-bandwidth-down-value", 0)
                .property("service-provider-bandwidth-down-units", "empty")
                .property("aai-uri","/network/physical-links/physical-link/pLinkScn1")
                .next();
        edgeSerializer.addTreeEdge(g,pserverRO,pintPlinkScn1);
        edgeSerializer.addEdge(g,pintPlinkScn1,pLinkScn1);
        
        //2. p-int matches on RCT, p-int and p-link don't move from RO to RCT
        
        Vertex pserverROSPlinkScn4 = g.addV().property("aai-node-type", "pserver")
                .property("hostname","Scn4.pserverROSPlinkScn4")
                .property("source-of-truth","RO")
                .property("aai-uri","/cloud-infrastructure/pservers/pserver/Scn4.pserverROSPlinkScn4")
                .property("resource-version","4")
                .next();
        
        pserverRCTPlinkScn4 = g.addV().property("aai-node-type", "pserver")
                .property("hostname","pserverRCTPlinkScn4")
                .property("source-of-truth","RCT")
                .property("fqdn","Scn4.pserverRCTPlinkScn4")
                .property("aai-uri","/cloud-infrastructure/pservers/pserver/pserverRCTPlinkScn4")
                .property("resource-version","3")
                .next();
        
        samePintScn4RO= g.addV()
                .property("aai-node-type", "p-interface")
                .property("interface-name", "pintPlinkScn4")
                .property("aai-uri","/cloud-infrastructure/pservers/pserver/Scn4.pserverROSPlinkScn4/p-interfaces/p-interface/pintPlinkScn4")
                .next();

        Vertex plinkScn2 = g.addV().property("aai-node-type", "physical-link")
                .property("link-name", "plinkScn2")
                .property("service-provider-bandwidth-up-value", 0)
                .property("service-provider-bandwidth-up-units", "empty")
                .property("service-provider-bandwidth-down-value", 0)
                .property("service-provider-bandwidth-down-units", "empty")
                .property("aai-uri","/network/physical-links/physical-link/pLinkScn2")
                .next();
        
        samePintScn4RCT= g.addV()
                .property("aai-node-type", "p-interface")
                .property("interface-name", "pintPlinkScn4")
                .property("aai-uri","/cloud-infrastructure/pservers/pserver/pserverRCTPlinkScn4/p-interfaces/p-interface/pintPlinkScn4")
                .next();
        
        edgeSerializer.addTreeEdge(g,pserverROSPlinkScn4,samePintScn4RO);
        edgeSerializer.addEdge(g,samePintScn4RO,plinkScn2);
        edgeSerializer.addTreeEdge(g,pserverRCTPlinkScn4,samePintScn4RCT);
        //End physical-links tests

//Scn2 RCT has children already

        pserverRCTScn2 = g.addV().property("aai-node-type", "pserver")
                .property("hostname","pserverRCTScn2")
                .property("source-of-truth","RCT")
                .property("fqdn","Scn2.pserverRCTScn2")
                .property("aai-uri","/cloud-infrastructure/pservers/pserver/pserverRCTScn2")
                .property("resource-version","3")
                .next();
        pIntRctScn2 = g.addV()
                .property("aai-node-type", "p-interface")
                .property("interface-name", "pIntRctScn2")
                .property("aai-uri","/cloud-infrastructure/pservers/pserver/pserverRCTScn2/p-interfaces/p-interface/pIntRctScn2")
                .next();
        lInterfaceRctScn2 = g.addV().property("aai-node-type", "l-interface")
                .property("interface-name", "lInterfaceRctScn2")
                .property("aai-uri","/cloud-infrastructure/pservers/pserver/pserverRCTScn2/p-interfaces/p-interface/pIntRctScn2/l-interfaces/l-interface/lInterfaceRctScn2")
                .next();
        complexRctScn2 = g.addV().property("aai-node-type", "complex")
                .property("physical-location-id","complexRctScn2")
                 .property("aai-uri","/cloud-infrastructure/complexes/complex/complexRctScn2")
                .next();
        Vertex pserverROScn2 = g.addV().property("aai-node-type", "pserver")
                .property("hostname","Scn2.pserverROScn2")
                .property("source-of-truth","RO")
                .property("aai-uri","/cloud-infrastructure/pservers/pserver/Scn2.pserverROScn2")
                .property("resource-version","4")
                .next();
        pIntRoScn2 = g.addV()
                .property("aai-node-type", "p-interface")
                .property("interface-name", "pIntRoScn2")
                .property("aai-uri","/cloud-infrastructure/pservers/pserver/Scn2.pserverROScn2/p-interfaces/p-interface/pIntRoScn2")
                .next();
        lInterfaceRoScn2 = g.addV().property("aai-node-type", "l-interface")
                .property("interface-name", "lInterfaceRoScn2")
                .property("aai-uri","/cloud-infrastructure/pservers/pserver/Scn2.pserverROScn2/p-interfaces/p-interface/pIntRoScn2/l-interfaces/l-interface/lInterfaceRoScn2")
                .next();
        complexROScn2 = g.addV().property("aai-node-type", "complex")
                .property("physical-location-id","complexROScn2")
                 .property("aai-uri","/cloud-infrastructure/complexes/complex/complexROScn2")
                .next();


        //Scn1
        edgeSerializer.addTreeEdge(g,pserverRO,pIntRo);
        edgeSerializer.addTreeEdge(g,pIntRo,lInterfaceRo);
        edgeSerializer.addEdge(g,pserverRO,complexRO);
        
        

        //Scn2
        edgeSerializer.addTreeEdge(g,pserverRCTScn2,pIntRctScn2);
        edgeSerializer.addTreeEdge(g,pIntRctScn2,lInterfaceRctScn2);
        edgeSerializer.addEdge(g,pserverRCTScn2,complexRctScn2);
        edgeSerializer.addTreeEdge(g,pserverROScn2,pIntRoScn2);
        edgeSerializer.addTreeEdge(g,pIntRoScn2,lInterfaceRoScn2);
        edgeSerializer.addEdge(g,pserverROScn2,complexROScn2);
        
        //Scn3
        pserverRCTScn3 = g.addV().property("aai-node-type", "pserver")
                .property("hostname","pserverRCTScn3")
                .property("source-of-truth","RCT")
                .property("fqdn","Scn3.pserverRCTScn3")
                .property("aai-uri","/cloud-infrastructure/pservers/pserver/pserverRCTScn3")
                .property("resource-version","3")
                .next();
        
        complexScn3 = g.addV().property("aai-node-type", "complex")
                .property("physical-location-id","complexScn3")
                .property("aai-uri","/cloud-infrastructure/complexes/complex/complexScn3")
                .next();
        
        pserverROScn3 = g.addV().property("aai-node-type", "pserver")
                .property("hostname","Scn3.pserverROScn3")
                .property("source-of-truth","RO")
                .property("fqdn","Scn2.pserverRCTScn2")
                .property("aai-uri","/cloud-infrastructure/pservers/pserver/Scn3.pserverROScn3")
                .property("resource-version","4")
                .next();
        
        edgeSerializer.addEdge(g, pserverRCTScn3, complexScn3);
        edgeSerializer.addEdge(g, pserverROScn3, complexScn3);
        
        //Verify manytoOne edge scenario
        pserverRCTScn6 = g.addV().property("aai-node-type", "pserver")
                .property("hostname","pserverRCTScn6")
                .property("source-of-truth","RCT")
                .property("fqdn","Scn6.pserverRCTScn6")
                .property("aai-uri","/cloud-infrastructure/pservers/pserver/pserverRCTScn6")
                .property("resource-version","1")
                .next();
        
        zoneScn61 = g.addV().property("aai-node-type", "zone")
        		.property("zone-id", "zone-61")
        		.property("aai-uri","/network/zones/zone/zone-61")
        		.next();
        
        pserverROScn6 = g.addV().property("aai-node-type", "pserver")
                .property("hostname","Scn6.pserverROScn6")
                .property("source-of-truth","RO")
                .property("fqdn","Scn6.pserverRCTScn6")
                .property("aai-uri","/cloud-infrastructure/pservers/pserver/Scn6.pserverROScn6")
                .property("resource-version","4")
                .next();
        
        zoneScn62 = g.addV().property("aai-node-type", "zone")
        		.property("zone-id", "zone-62")
        		.property("aai-uri","/network/zones/zone/zone-62")
        		.next();
        
        edgeSerializer.addEdge(g,  pserverRCTScn6, zoneScn61);
        edgeSerializer.addEdge(g,  pserverROScn6, zoneScn62);
        
        //Verify manyToMany edge scenario
        Vertex gvnf1 = g.addV().property("aai-node-type", "generic-vnf")
        		.property("vnf-id", "vnf-1")
        		.property("aai-uri","/cloud-infrastructure/pservers/pserver/Scn6.pserverROScn6")
        		.next();
        
        Vertex gvnf2 = g.addV().property("aai-node-type", "generic-vnf")
        		.property("vnf-id", "vnf-2")
        		.property("aai-uri","/network/generic-vnfs/generic-vnf/vnf-2")
        		.next();
        
        edgeSerializer.addEdge(g,  pserverRCTScn6, gvnf1);
        edgeSerializer.addEdge(g,  pserverROScn6, gvnf2);
        
        // Verify empty string scenario
        Vertex pserver1EmptyFirstToken = g.addV().property("aai-node-type", "pserver")
                .property("hostname",".pserver1EmptyFirstToken")
                .property("source-of-truth","RO")
                .property("fqdn","sameScn1.rrrr.tttt.yyyy")
                .property("aai-uri","/cloud-infrastructure/pservers/pserver/.pserver1EmptyFirstToken")
                .property("resource-version","1")
                .next();
        
        Vertex pserver1EmptyFirstTokenFqdn = g.addV().property("aai-node-type", "pserver")
                .property("hostname","pserver1EmptyFirstTokenFqdn")
                .property("source-of-truth","RCT")
                .property("fqdn",".rrrr.tttt.yyyy")
                .property("aai-uri","/cloud-infrastructure/pservers/pserver/pserver1EmptyFirstTokenFqdn")
                .property("resource-version","1")
                .next();

      //lag-interfaces
        
        Vertex roP1 = g.addV().property("aai-node-type", "pserver")
                .property("hostname","pserver.ro")
                .property("source-of-truth","RO")
                .property("fqdn","pserver.rrrr.tttt.yyyy")
                .property("aai-uri","/cloud-infrastructure/pservers/pserver/pserver.ro")
                .property("resource-version","1")
                .next();
        
        Vertex rctP1 = g.addV().property("aai-node-type", "pserver")
                .property("hostname","rctP1")
                .property("source-of-truth","RCT")
                .property("fqdn","pserver.rrrr.tttt.yyyy")
                .property("aai-uri","/cloud-infrastructure/pservers/pserver/rctP1")
                .property("resource-version","3")
                .next();
        
        //lagint11 does not have a match on rctP1. So, expect this to move to rctP1. Add test
          Vertex lagint11= g.addV()
                  .property("aai-node-type", "lag-interface")
                  .property("interface-name", "lagint11")
                  .property("aai-uri","/cloud-infrastructure/pservers/pserver/roP1/lag-interfaces/lag-interface/lagint11")
                  .next();
          edgeSerializer.addTreeEdge(g, roP1, lagint11);
          
          //lagint12 matches with lagint31 on rctP3
          Vertex lagint12= g.addV()
                  .property("aai-node-type", "lag-interface")
                  .property("interface-name", "lagint12")
                  .property("aai-uri","/cloud-infrastructure/pservers/pserver/roP1/lag-interfaces/lag-interface/lagint12")
                  .next();
          Vertex lagint31= g.addV()
                  .property("aai-node-type", "lag-interface")
                  .property("interface-name", "lagint12")
                  .property("aai-uri","/cloud-infrastructure/pservers/pserver/rctP3/lag-interfaces/lag-interface/lagint12")
                  .next();
          edgeSerializer.addTreeEdge(g, roP1, lagint12);
          edgeSerializer.addTreeEdge(g, rctP1, lagint31);
          
        TransactionalGraphEngine spy = spy(dbEngine);
        TransactionalGraphEngine.Admin adminSpy = spy(dbEngine.asAdmin());

        GraphTraversalSource traversal = g;
        GraphTraversalSource readOnly = graph.traversal().withStrategies(ReadOnlyStrategy.instance());
        when (spy.tx()).thenReturn(tx);
        when(spy.asAdmin()).thenReturn(adminSpy);
        when(adminSpy.getTraversalSource()).thenReturn(traversal);
        when(adminSpy.getReadOnlyTraversalSource()).thenReturn(readOnly);

        migration = new PserverDedupWithDifferentSourcesOfTruth(spy,loaderFactory, edgeIngestor, edgeSerializer, schemaVersions);
        migration.run();
    }

    @After
    public void cleanUp() {
        tx.tx().rollback();
        graph.close();
    }

    @Test
    public void rctSuccessfulMoveScn1() throws Exception {

        assertEquals("tttt.RoHostname",pserverRCT.property("fqdn").value().toString());
        assertEquals(true, g.V().has("aai-node-type", "pserver").has("hostname","pserverRCT").in("tosca.relationships.network.BindsTo").has("aai-node-type","p-interface").hasNext());
        assertEquals("/cloud-infrastructure/pservers/pserver/pserverRCT/p-interfaces/p-interface/pIntRo", pIntRo.property("aai-uri").value().toString());
        assertEquals("/cloud-infrastructure/pservers/pserver/pserverRCT/p-interfaces/p-interface/pIntRo/l-interfaces/l-interface/lInterfaceRo", lInterfaceRo.property("aai-uri").value().toString());
        assertEquals(true,pserverRCT.property("pserver-id").isPresent());
    }
    
    @Test
    public void rctSuccessfulMovePlink() throws Exception {

        assertEquals("tttt.RoHostname",pserverRCT.property("fqdn").value().toString());
        assertEquals(true, g.V().has("aai-node-type", "pserver").has("hostname","pserverRCT").in("tosca.relationships.network.BindsTo")
        		.has("aai-node-type","p-interface").has("interface-name","pintPlinkScn1").hasNext());
        assertEquals("/cloud-infrastructure/pservers/pserver/pserverRCT/p-interfaces/p-interface/pintPlinkScn1", pintPlinkScn1.property("aai-uri").value().toString());
        assertEquals(true, g.V().has("aai-node-type", "pserver").has("hostname","pserverRCT").in("tosca.relationships.network.BindsTo")
        		.has("aai-node-type","p-interface").has("interface-name","pintPlinkScn1").out("tosca.relationships.network.LinksTo").has("link-name","pLinkScn1").hasNext());
    }
    
    @Test
    public void rctNoChangeSamePIntScenario() throws Exception {

        assertEquals("Scn4.pserverROSPlinkScn4",pserverRCTPlinkScn4.property("fqdn").value().toString());
        assertEquals(true, g.V().has("aai-node-type", "pserver").has("hostname","pserverRCTPlinkScn4").in("tosca.relationships.network.BindsTo")
        		.has("aai-node-type","p-interface").has("interface-name","pintPlinkScn4").hasNext());
        assertEquals("only 1 p-int is present on RCT pserver", new Long(1L), g.V().has("aai-node-type", "pserver").has("hostname","pserverRCTPlinkScn4").in("tosca.relationships.network.BindsTo")
        		.has("aai-node-type","p-interface").has("interface-name","pintPlinkScn4").count().next());
        assertEquals("/cloud-infrastructure/pservers/pserver/pserverRCTPlinkScn4/p-interfaces/p-interface/pintPlinkScn4", samePintScn4RCT.property("aai-uri").value().toString());
        //plink is not  moved from RO to RCT when p-int matches
        assertEquals(false, g.V().has("aai-node-type", "pserver").has("hostname","pserverRCT").in("tosca.relationships.network.BindsTo")
        		.has("aai-node-type","p-interface").has("interface-name","pintPlinkScn4").out("tosca.relationships.network.LinksTo").hasNext());
        assertEquals(false, g.V().has("aai-node-type", "pserver").has("hostname","pserverROPlinkScn4").hasNext());
        //Verify that no orphan nodes are present in the graph
        assertEquals(false, g.V().has("aai-node-type","p-interface").has("interface-name","pintPlinkScn4").out("tosca.relationships.network.LinksTo").has("link-name","pLinkScn2").hasNext());
        assertEquals(false, g.V().has("aai-node-type","physical-link").has("link-name","pLinkScn2").hasNext());
    }

    @Test
    public void rctSuccessfulMoveScn2() throws Exception {

        assertEquals("Scn2.pserverROScn2",pserverRCTScn2.property("fqdn").value().toString());
        assertEquals(true, g.V().has("aai-node-type", "pserver").has("hostname","pserverRCTScn2").in("tosca.relationships.network.BindsTo").has("aai-node-type","p-interface").hasNext());
        assertEquals("/cloud-infrastructure/pservers/pserver/pserverRCTScn2/p-interfaces/p-interface/pIntRoScn2", pIntRoScn2.property("aai-uri").value().toString());
        assertEquals("/cloud-infrastructure/pservers/pserver/pserverRCTScn2/p-interfaces/p-interface/pIntRoScn2/l-interfaces/l-interface/lInterfaceRoScn2", lInterfaceRoScn2.property("aai-uri").value().toString());
        assertEquals(true,pserverRCTScn2.property("pserver-id").isPresent());
        assertEquals(true, g.V().has("aai-node-type", "pserver").has("hostname","pserverRCTScn2").out("org.onap.relationships.inventory.LocatedIn")
                .has("aai-node-type","complex").has("physical-location-id","complexRctScn2").hasNext());
    }
    
    @Test
    public void checkRCTPserverHasRelnToOnly1Complex() throws Exception {

               assertEquals("Edge to only 1 complex exists", new Long(1L), g.V().has("aai-node-type", "pserver").has("hostname","pserverRCTScn3").out("org.onap.relationships.inventory.LocatedIn")
                .has("aai-node-type","complex").count().next());
    }
    
    @Test
    public void checkRCTPserverHasRelnToOnly1Zone() throws Exception {

               assertEquals("Edge to only 1 Zone exists", new Long(1L), g.V().has("aai-node-type", "pserver").has("hostname","pserverRCTScn6").out("org.onap.relationships.inventory.LocatedIn")
                .has("aai-node-type","zone").count().next());
               assertEquals(true, g.V().has("aai-node-type", "zone").has("zone-id","zone-62").hasNext());
               //Verify no edge exists from zone62 to RO pserver
               assertEquals(false, g.V().has("aai-node-type", "zone").has("zone-id","zone-62").in().has("aai-node-type", "pserver").hasNext());
               
    }
    
    @Test
    public void checkRCTPserverHasRelnTo2GenericVnfs() throws Exception {

               assertEquals("Edge to 2 generic-vnfs exists", new Long(2L), g.V().has("aai-node-type", "pserver").has("hostname","pserverRCTScn6").in("tosca.relationships.HostedOn")
                .has("aai-node-type","generic-vnf").count().next());
               assertEquals(true, g.V().has("aai-node-type", "generic-vnf").has("vnf-id","vnf-2").out().has("aai-node-type", "pserver").has("hostname", "pserverRCTScn6").hasNext());
               //Verify no edge exists from zone62 to RO pserver
               assertEquals(false, g.V().has("aai-node-type", "generic-vnf").has("vnf-id","vnf-2").out().has("aai-node-type", "pserver").has("hostname", "pserverROScn6").hasNext());
    }
    
    @Test
    public void ignoreEmptyStringFirstTokenFqdn() throws Exception {
//    	List<Vertex> pserverList = g.V().has("aai-node-type", "pserver").has("hostname").toList();
//    	pserverList.forEach(v ->System.out.println(v.property("hostname").value().toString()));
    	assertEquals(true, g.V().has("aai-node-type", "pserver").has("hostname","pserver1EmptyFirstTokenFqdn").hasNext());
    	assertEquals(true, g.V().has("aai-node-type", "pserver").has("hostname",".pserver1EmptyFirstToken").hasNext());
    	
    }
    
    @Test
    public void testLagInterfaces() throws Exception {
    	//1. lagint11 from roP1 moves to rctP1
    	assertEquals(true, g.V().has("aai-node-type", "pserver").has("hostname","rctP1").in("tosca.relationships.network.BindsTo")
        		.has("aai-node-type","lag-interface").has("interface-name","lagint11").hasNext());
    	assertEquals(false, g.V().has("aai-node-type", "lag-interface").has("aai-uri","/cloud-infrastructure/pservers/pserver/pserver.ro/lag-interfaces/lag-interface/lagint11").hasNext());
    	assertEquals(true, g.V().has("aai-node-type", "lag-interface").has("aai-uri","/cloud-infrastructure/pservers/pserver/rctP1/lag-interfaces/lag-interface/lagint11").hasNext());

    	
    	//2. lagint12 int-name matches with lagint31. So, verify that lag-int does not move from rctP1 to rctP3
    	assertEquals("rctP1 has only 1 lag-interface with name lagint12", new Long(1L), g.V().has("aai-node-type", "pserver").has("hostname","rctP1").in("tosca.relationships.network.BindsTo")
        		.has("aai-node-type","lag-interface").has("interface-name","lagint12").count().next());
    	
    }

}
