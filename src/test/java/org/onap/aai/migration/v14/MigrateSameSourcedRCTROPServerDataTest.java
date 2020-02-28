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
import java.util.UUID;

import org.apache.tinkerpop.gremlin.process.traversal.dsl.graph.GraphTraversalSource;
import org.apache.tinkerpop.gremlin.process.traversal.strategy.verification.ReadOnlyStrategy;
import org.apache.tinkerpop.gremlin.structure.Vertex;
import org.junit.*;
import org.onap.aai.AAISetup;
import org.onap.aai.dbmap.DBConnectionType;
import org.onap.aai.introspection.Loader;
import org.onap.aai.introspection.ModelType;
import org.onap.aai.migration.v14.MigrateSameSourcedRCTROPserverData;
import org.onap.aai.serialization.engines.QueryStyle;
import org.onap.aai.serialization.engines.JanusGraphDBEngine;
import org.onap.aai.serialization.engines.TransactionalGraphEngine;
import org.janusgraph.core.JanusGraphFactory;
import org.janusgraph.core.JanusGraph;
import org.janusgraph.core.JanusGraphTransaction;

public class MigrateSameSourcedRCTROPServerDataTest extends AAISetup{

    private final static ModelType introspectorFactoryType = ModelType.MOXY;
    private final static QueryStyle queryStyle = QueryStyle.TRAVERSAL;

     Loader loader;
     TransactionalGraphEngine dbEngine;
     JanusGraph graph;
     MigrateSameSourcedRCTROPserverData migration;
     JanusGraphTransaction tx;
     GraphTraversalSource g;
     Vertex pintOld;
     Vertex lInterfaceold;
     Vertex pintOldRo;
     Vertex lInterfaceoldRo;
     Vertex pintOldRo1;
     Vertex pintNewRo1;
     Vertex plinkROonOldRo1;

     Vertex pintOldScn3;
     Vertex pintNewScn3;
     Vertex pLinkOldScn3;
     Vertex  pLinkNewScn3;

     Vertex  pintOldScn2;
     Vertex  pintNewScn2;
     Vertex  pLinkOldScn2;
     Vertex  pintOld2Scn2;
     Vertex sriovPfOld;
      Vertex sriovVfOld;

     Vertex lInterfaceold2;
     Vertex pintOld2;

     Vertex pLinkMoveScn2;
     Vertex pLinkMoveScn1;
     
     Vertex pint1ROOld;
     Vertex pint2ROOld;
     Vertex pint2RONew;
     Vertex pint3ROOld;
     Vertex pint3RONew;
     Vertex pint1ROOldPlink;
     
   //ManyToOne edge scenario
     Vertex pserverRCTScn6;
     Vertex pserverRCT1Scn6;
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
//rct
        Vertex pserverOld = g.addV().property("aai-node-type", "pserver")
                .property("hostname","pserverOld")
                .property("source-of-truth","RCT")
                .property("fqdn","tttt.bbbb.cccc.dddd")
                .property("aai-uri","/cloud-infrastructure/pservers/pserver/pserverOld")
                .property("resource-version","1")
                .next();
        Vertex pserverNew = g.addV().property("aai-node-type", "pserver")
                .property("hostname","pserverNew")
                .property("source-of-truth","RCT")
                .property("fqdn","tttt.cccc.cccc.dddd")
                .property("aai-uri","/cloud-infrastructure/pservers/pserver/pserverNew")
                .property("resource-version","2")
                .next();
        pintOld = g.addV()
                .property("aai-node-type", "p-interface")
                .property("interface-name", "pintOld")
                .property("aai-uri","/cloud-infrastructure/pservers/pserver/pserverOld/p-interfaces/p-interface/pintOld")
                .next();

        lInterfaceold = g.addV().property("aai-node-type", "l-interface")
                .property("interface-name", "linterfaceold")
                .property("aai-uri","/cloud-infrastructure/pservers/pserver/pserverOld/p-interfaces/p-interface/pintOld/l-interfaces/l-interface/linterfaceold")
                .next();

        pintOld2 = g.addV()
                .property("aai-node-type", "p-interface")
                .property("interface-name", "pintOld2")
                .property("aai-uri","/cloud-infrastructure/pservers/pserver/pserverOld/p-interfaces/p-interface/pintOld2")
                .next();
        lInterfaceold2 = g.addV().property("aai-node-type", "l-interface")
                .property("interface-name", "linterfaceold2")
                .property("aai-uri","/cloud-infrastructure/pservers/pserver/pserverOld/p-interfaces/p-interface/pintOld2/l-interfaces/l-interface/linterfaceold2")
                .next();

        sriovPfOld = g.addV().property("aai-node-type", "sriov-pf")
                .property("pf-pci-id","sriovPfOld")
                .property("source-of-truth","RCT")
                .property("aai-uri","/cloud-infrastructure/pservers/pserver/pserverOld/p-interfaces/p-interface/pintOld2/sriov-pfs/sriov-pf/sriovPfOld")
                .next();

        sriovVfOld = g.addV().property("aai-node-type", "sriov-vf")
                .property("pci-id","sriovVfOld")
                .property("source-of-truth","RCT")
                .property("aai-uri","/cloud-infrastructure/pservers/pserver/pserverOld/p-interfaces/p-interface/pintOld2/l-interfaces/l-interface/linterfaceold2/sriov-vfs/sriov-vf/sriovVfOld")
                .next();
        
        

        Vertex vserver3 = g.addV().property("aai-node-type", "vserver")
                .property("vserver-id", "vserver1")
                .property("aai-uri","/cloud-infrastructure/pservers/pserver/vserver3")
                .next();
        Vertex lInterface3 = g.addV().property("aai-node-type", "l-interface")
                .property("interface-name", "linterfaceold3")
                .property("aai-uri","/cloud-infrastructure/pservers/pserver/vserver3")
                .next();
        Vertex sriovVfOld3 = g.addV().property("aai-node-type", "sriov-vf")
                .property("pci-id","sriovVfOld3")
                .property("source-of-truth","RCT")
//                .property("aai-uri","/cloud-infrastructure/pservers/pserver/vserver3")
                .next();
        Vertex complexOld = g.addV().property("aai-node-type", "complex")
        		.property("physical-location-id", "complexOld")
        		.property("aai-uri","/cloud-infrastructure/complex/complexOld")
                .next();


//ro
        Vertex pserverRoOld = g.addV().property("aai-node-type", "pserver")
                .property("hostname","pserverRo.OldOne.aaaa.bbbbb")
                .property("source-of-truth","RO")
                .property("fqdn","aaaa.bbbb.cccc.dddd")
                .property("aai-uri","/cloud-infrastructure/pservers/pserver/pserverRo.OldOne.aaaa.bbbbb")
                .property("resource-version","1")
                .next();
        Vertex pserverRoNew = g.addV().property("aai-node-type", "pserver")
                .property("hostname","pserverRo.NewOne.aaaa.ccccccccccc")
                .property("source-of-truth","RO")
                .property("fqdn","aaaa.cccc.cccc.dddd")
                .property("aai-uri","/cloud-infrastructure/pservers/pserver/pserverRo.NewOne.aaaa.ccccccccccc")
                .property("resource-version","2")
                .next();
        
        Vertex pserverRo3 = g.addV().property("aai-node-type", "pserver")
                .property("hostname","pserverRo3")
                .property("source-of-truth","RO")
                .property("fqdn","aaaa.cccc.cccc.dddd")
                .property("aai-uri","/cloud-infrastructure/pservers/pserver/pserverRo3")
                .property("resource-version","2")
                .next();
        
        Vertex pserverRo4 =  g.addV().property("aai-node-type", "pserver")
                .property("hostname","pserverRoComplexTest.aaa")
                .property("source-of-truth","RO")
                .property("fqdn","aaaa.cccc.cccc.dddd")
                .property("aai-uri","/cloud-infrastructure/pservers/pserver/pserverRoComplexTest.aaa")
                .property("resource-version","2")
                .next();
        
        Vertex pserverRo5 =  g.addV().property("aai-node-type", "pserver")
                .property("hostname","pserverRoComplexTest.aaaaa")
                .property("source-of-truth","RO")
                .property("fqdn","aaaa.cccc.cccc.eeee")
                .property("aai-uri","/cloud-infrastructure/pservers/pserver/pserverRoComplexTest.aaaaa")
                .property("resource-version","2")
                .next();
        
        pintOldRo = g.addV()
                .property("aai-node-type", "p-interface")
                .property("interface-name", "pintOldRo")
                .property("aai-uri","/cloud-infrastructure/pservers/pserver/pserverRo.OldOne.aaaa.bbbbb/p-interfaces/p-interface/pintOldRo")
                .next();
        lInterfaceoldRo = g.addV().property("aai-node-type", "l-interface")
                .property("interface-name", "linterfaceoldRo")
                .property("aai-uri","/cloud-infrastructure/pservers/pserver/pserverRo.OldOne.aaaa.bbbbb/p-interfaces/p-interface/pintOld/l-interfaces/l-interface/linterfaceold")
                .next();
        Vertex complexOldRO = g.addV().property("aai-node-type", "complex")
        		.property("physical-location-id", "complexOldRO")
        		.property("aai-uri","/cloud-infrastructure/complexes/complex/vserver3")
                .next();

        pintOldRo1 = g.addV()
                .property("aai-node-type", "p-interface")
                .property("interface-name", "pintRo1")
                .property("aai-uri","/cloud-infrastructure/pservers/pserver/pserverRo.OldOne.aaaa.bbbbb/p-interfaces/p-interface/pintRo1")
                .next();
        
        pintNewRo1 = g.addV()
                .property("aai-node-type", "p-interface")
                .property("interface-name", "pintRo1")
                .property("aai-uri","/cloud-infrastructure/pservers/pserver/pserverRo.NewOne.aaaa.ccccccccccc/p-interfaces/p-interface/pintRo1")
                .next();
        
        plinkROonOldRo1 = g.addV()
        		.property("aai-node-type", "physical-link")
        		.property("link-name", "plinkROonOldRo1")
        		.next();
        
        Vertex pintNew31 = g.addV()
                .property("aai-node-type", "p-interface")
                .property("interface-name", "pintRo1")
                .property("aai-uri","/cloud-infrastructure/pservers/pserver/pserverRo3/p-interfaces/p-interface/pintRo1")
                .next();

        Vertex pintOld41 = g.addV()
                .property("aai-node-type", "p-interface")
                .property("interface-name", "pintOld41")
                .property("aai-uri","/cloud-infrastructure/pservers/pserver/pserverRoComplexTest.aaa/p-interfaces/p-interface/pintOld41")
                .next();
        
        Vertex sriovpfOldRo1= g.addV()
                 .property("aai-node-type", "sriov-pf")
                 .property("pf-pci-id", "sriovpfOldRo1")
                 .property("aai-uri","/cloud-infrastructure/pservers/pserver/pserverRo.OldOne.aaaa.bbbbb/p-interfaces/p-interface/pintRo1/sriov-pfs/sriov-pf/sriovpfOldRo1")
                 .next();

        //Scenario 3 same p interface name, new interface has a seprate plink

        Vertex pserverOldScn3 = g.addV().property("aai-node-type", "pserver")
                .property("hostname","pserverOldScn3")
                .property("source-of-truth","RCT")
                .property("fqdn","eeee.rrrr.tttt.yyyy")
                .property("aai-uri","/cloud-infrastructure/pservers/pserver/pserverOldScn3")
                .property("resource-version","1")
                .next();
        Vertex pserverNewScn3= g.addV().property("aai-node-type", "pserver")
                .property("hostname","pserverNewScn3")
                .property("source-of-truth","RCT")
                .property("fqdn","eeee.rrrr.tttt.yyyy")
                .property("aai-uri","/cloud-infrastructure/pservers/pserver/pserverNewScn3")
                .property("resource-version","2")
                .next();

        pintOldScn3= g.addV()
                .property("aai-node-type", "p-interface")
                .property("interface-name", "pintOldScn3")
                .property("aai-uri","/cloud-infrastructure/pservers/pserver/pserverOldScn3/p-interfaces/p-interface/pintOldScn3")
                .next();
        pintNewScn3= g.addV()
                .property("aai-node-type", "p-interface")
                .property("interface-name", "pintNewScn3")
                .property("aai-uri","/cloud-infrastructure/pservers/pserver/pserverNewScn3/p-interfaces/p-interface/pintNewScn3")
                .next();

        pLinkOldScn3 = g.addV().property("aai-node-type", "physical-link")
                .property("link-name", "pLinkOldScn3")
                .property("service-provider-bandwidth-up-value", 0)
                .property("service-provider-bandwidth-up-units", "empty")
                .property("service-provider-bandwidth-down-value", 0)
                .property("service-provider-bandwidth-down-units", "empty")
                .property("aai-uri","/cloud-infrastructure/pservers/pserver/vserver3")
                .next();
        pLinkNewScn3 = g.addV().property("aai-node-type", "physical-link")
                .property("link-name", "pLinkNewScn3")
                .property("service-provider-bandwidth-up-value", 0)
                .property("service-provider-bandwidth-up-units", "empty")
                .property("service-provider-bandwidth-down-value", 0)
                .property("service-provider-bandwidth-down-units", "empty")
                .property("aai-uri","/cloud-infrastructure/pservers/pserver/vserver3")
                .next();





//        Scenario 2 missing plink in new pserver same pinterface name

        Vertex pserverOldScn2 = g.addV().property("aai-node-type", "pserver")
                .property("hostname","pserverOldScn2")
                .property("source-of-truth","RCT")
                .property("fqdn","vvvv.rrrr.tttt.yyyy")
                .property("aai-uri","/cloud-infrastructure/pservers/pserver/pserverOldScn2")
                .property("resource-version","1")
                .next();
        Vertex pserverNewScn2= g.addV().property("aai-node-type", "pserver")
                .property("hostname","pserverNewScn2")
                .property("source-of-truth","RCT")
                .property("fqdn","vvvv.rrrr.tttt.yyyy")
                .property("aai-uri","/cloud-infrastructure/pservers/pserver/pserverNewScn2")
                .property("resource-version","2")
                .next();

        pintOldScn2= g.addV()
                .property("aai-node-type", "p-interface")
                .property("interface-name", "pintScn2")
                .property("aai-uri","/cloud-infrastructure/pservers/pserver/pserverOldScn2/p-interfaces/p-interface/pintScn2")
                .next();
        pintOld2Scn2= g.addV()
                .property("aai-node-type", "p-interface")
                .property("interface-name", "pintOld2Scn2")
                .property("aai-uri","/cloud-infrastructure/pservers/pserver/pserverOldScn2/p-interfaces/p-interface/pintOld2Scn2")
                .next();

        pintNewScn2= g.addV()
                .property("aai-node-type", "p-interface")
                .property("interface-name", "pintScn2")
                .property("aai-uri","/cloud-infrastructure/pservers/pserver/pserverNewScn2/p-interfaces/p-interface/pintScn2")
                .next();

        pLinkOldScn2 = g.addV().property("aai-node-type", "physical-link")
                .property("link-name", "pLinkOldScn2")
                .property("service-provider-bandwidth-up-value", 0)
                .property("service-provider-bandwidth-up-units", "empty")
                .property("service-provider-bandwidth-down-value", 0)
                .property("service-provider-bandwidth-down-units", "empty")
                .property("aai-uri","/cloud-infrastructure/pservers/pserver/vserver3")
                .next();

//   Scnario 2 RCT Pinter face match moving plink and updating the name
        Vertex pserver1Scn2 = g.addV().property("aai-node-type", "pserver")
                .property("hostname","pserver1Scn2")
                .property("source-of-truth","RCT")
                .property("fqdn","same.rrrr.tttt.yyyy")
                .property("aai-uri","/cloud-infrastructure/pservers/pserver/pserver1Scn2")
                .property("resource-version","1")
                .next();
        Vertex pserver2Scn2= g.addV().property("aai-node-type", "pserver")
                .property("hostname","pserver2Scn2")
                .property("source-of-truth","RCT")
                .property("fqdn","same.rrrr.tttt.yyyy")
                .property("aai-uri","/cloud-infrastructure/pservers/pserver/pserver2Scn2")
                .property("resource-version","2")
                .next();
        Vertex pserver3Scn2= g.addV().property("aai-node-type", "pserver")
                .property("hostname","pserver3Scn2")
                .property("source-of-truth","RCT")
                .property("fqdn","jkkdahfkjashf.rrrr.tttt.yyyy")
                .property("aai-uri","/cloud-infrastructure/pservers/pserver/pserver3Scn2")
                .property("resource-version","2")
                .next();


        Vertex pint1Scn2= g.addV()
                .property("aai-node-type", "p-interface")
                .property("interface-name", "pint1Scn2")
                .property("aai-uri","/cloud-infrastructure/pservers/pserver/pserver1Scn2/p-interfaces/p-interface/pint1Scn2")
                .next();
        Vertex pint2NewScn2= g.addV()
                .property("aai-node-type", "p-interface")
                .property("interface-name", "pint1Scn2")
                .property("aai-uri","/cloud-infrastructure/pservers/pserver/pserver2Scn2/p-interfaces/p-interface/pint1Scn2")
                .next();
        Vertex pint3Scn2= g.addV()
                .property("aai-node-type", "p-interface")
                .property("interface-name", "pint3Scn2")
                .property("aai-uri","/cloud-infrastructure/pservers/pserver/pserver3Scn2/p-interfaces/p-interface/pint3Scn2")
                .next();

         pLinkMoveScn2= g.addV().property("aai-node-type", "physical-link")
                .property("link-name", "pserver1Scn2:pint1Scn2|pserver3Scn2:pint3Scn2")
                .property("aai-uri","/cloud-infrastructure/plink/pLinkMoveScn2")
                .next();


// Scenario 1 RCT  plink name change move everything from old pserver to new pserver, new pserver has no plink
        Vertex pserver1Scn1 = g.addV().property("aai-node-type", "pserver")
                .property("hostname","pserver1Scn1")
                .property("source-of-truth","RCT")
                .property("fqdn","sameScn1.rrrr.tttt.yyyy")
                .property("aai-uri","/cloud-infrastructure/pservers/pserver/pserver1Scn1")
                .property("resource-version","1")
                .next();
        Vertex pserver2Scn1= g.addV().property("aai-node-type", "pserver")
                .property("hostname","pserver2Scn1")
                .property("source-of-truth","RCT")
                .property("fqdn","sameScn1.rrrr.tttt.yyyy")
                .property("aai-uri","/cloud-infrastructure/pservers/pserver/pserver2Scn1")
                .property("resource-version","2")
                .next();
        Vertex pserver3Scn1= g.addV().property("aai-node-type", "pserver")
                .property("hostname","pserver3Scn1")
                .property("source-of-truth","RCT")
                .property("fqdn","jkkdahfkjashf.rrrr.tttt.yyyy")
                .property("aai-uri","/cloud-infrastructure/pservers/pserver/pserver3Scn1")
                .property("resource-version","2")
                .next();


        Vertex pint1Scn1= g.addV()
                .property("aai-node-type", "p-interface")
                .property("interface-name", "pint1Scn1")
                .property("aai-uri","/cloud-infrastructure/pservers/pserver/pserver1Scn1/p-interfaces/p-interface/pint1Scn1")
                .next();

        Vertex pint3Scn1= g.addV()
                .property("aai-node-type", "p-interface")
                .property("interface-name", "pint3Scn1")
                .property("aai-uri","/cloud-infrastructure/pservers/pserver/pserver3Scn1/p-interfaces/p-interface/pint3Scn1")
                .next();


        pLinkMoveScn1= g.addV().property("aai-node-type", "physical-link")
                .property("link-name", "pserver1Scn1:pint1Scn1|pserver3Scn1:pint3Scn1")
                .property("aai-uri","/cloud-infrastructure/plink/pLinkMoveScn1")
                .next();


        //Scnario 2 RCT Pinter face match moving plink and updating the name
        edgeSerializer.addTreeEdge(g,pserver1Scn2,pint1Scn2);

        edgeSerializer.addTreeEdge(g,pserver2Scn2,pint2NewScn2);

        edgeSerializer.addTreeEdge(g,pserver3Scn2,pint3Scn2);

        edgeSerializer.addEdge(g,pint1Scn2,pLinkMoveScn2);
        edgeSerializer.addEdge(g,pint3Scn2,pLinkMoveScn2);


        // Scenario 1 RCT  plink name change move everything from old pserver to new pserver, new pserver has no plink
        edgeSerializer.addTreeEdge(g, pserver1Scn1,pint1Scn1);
        edgeSerializer.addTreeEdge(g, pserver3Scn1,pint3Scn1);

        edgeSerializer.addEdge(g,pint1Scn1,pLinkMoveScn1);
        edgeSerializer.addEdge(g,pint3Scn1,pLinkMoveScn1);


//RCT
        edgeSerializer.addTreeEdge(g, pserverOld,pintOld);
        edgeSerializer.addTreeEdge(g, pintOld,lInterfaceold);
//        rules.addTreeEdge(g, pintOld,sriovPfOld);
//        rules.addTreeEdge(g, lInterfaceold,sriovVfOld);

        edgeSerializer.addTreeEdge(g, pserverOld,pintOld2);
        edgeSerializer.addTreeEdge(g, pintOld2,lInterfaceold2);
        edgeSerializer.addTreeEdge(g, pintOld2,sriovPfOld);
        edgeSerializer.addTreeEdge(g, sriovVfOld,lInterfaceold2);

        edgeSerializer.addTreeEdge(g,vserver3,lInterface3);
        edgeSerializer.addTreeEdge(g,lInterface3,sriovVfOld3);

        edgeSerializer.addEdge(g,sriovPfOld,sriovVfOld3);
        edgeSerializer.addEdge(g,pserverOld,complexOld);



//ro
        edgeSerializer.addTreeEdge(g,pserverRoOld,pintOldRo);
        edgeSerializer.addTreeEdge(g,pintOldRo,lInterfaceoldRo);
        edgeSerializer.addEdge(g,pserverRoOld,complexOldRO);
        edgeSerializer.addTreeEdge(g,pserverRoOld,  pintOldRo1);
        edgeSerializer.addTreeEdge(g,pserverRoNew,  pintNewRo1);
        edgeSerializer.addEdge(g, pintOldRo1, plinkROonOldRo1);
        
        edgeSerializer.addTreeEdge(g, pserverRo3, pintNew31);
        edgeSerializer.addEdge(g, pintNew31, plinkROonOldRo1);
        
        edgeSerializer.addTreeEdge(g, pserverRo4, pintOld41);
        
        edgeSerializer.addTreeEdge(g, pintOldRo1, sriovpfOldRo1);
        
        
        // physical-link tests
        //1. p-int does not exist on longer hostname RO, p-int and p-link moves from shorter to longer
        pint1ROOld= g.addV()
                .property("aai-node-type", "p-interface")
                .property("interface-name", "pint1ROOld")
                .property("aai-uri","/cloud-infrastructure/pservers/pserver/pserverRo.OldOne.aaaa.bbbbb/p-interfaces/p-interface/pint1ROOld")
                .next();

        Vertex pLink1ROOld = g.addV().property("aai-node-type", "physical-link")
                .property("link-name", "pLink1ROOld")
                .property("service-provider-bandwidth-up-value", 0)
                .property("service-provider-bandwidth-up-units", "empty")
                .property("service-provider-bandwidth-down-value", 0)
                .property("service-provider-bandwidth-down-units", "empty")
                .next();
        edgeSerializer.addTreeEdge(g,pserverRoOld,pint1ROOld);
        edgeSerializer.addEdge(g,pint1ROOld,pLink1ROOld);
        
        //2. p-int matches on shorter and longer hostname ROP pservers, p-link does not exist on longer RO. p-link moves from shorter to longer hostname
        pint2ROOld= g.addV()
                .property("aai-node-type", "p-interface")
                .property("interface-name", "pint2RO")
                .property("aai-uri","/cloud-infrastructure/pservers/pserver/pserverRo.OldOne.aaaa.bbbbb/p-interfaces/p-interface/pint2RO")
                .next();

        Vertex pLink2ROOld = g.addV().property("aai-node-type", "physical-link")
                .property("link-name", "pLink2ROOld")
                .property("service-provider-bandwidth-up-value", 0)
                .property("service-provider-bandwidth-up-units", "empty")
                .property("service-provider-bandwidth-down-value", 0)
                .property("service-provider-bandwidth-down-units", "empty")
                .next();
        
        pint2RONew= g.addV()
                .property("aai-node-type", "p-interface")
                .property("interface-name", "pint2RO")
                .property("aai-uri","/cloud-infrastructure/pservers/pserver/pserverRo.NewOne.aaaa.ccccccccccc/p-interfaces/p-interface/pint2RO")
                .next();

        edgeSerializer.addTreeEdge(g,pserverRoOld,pint2ROOld);
        edgeSerializer.addEdge(g,pint2ROOld,pLink2ROOld);
        edgeSerializer.addTreeEdge(g,pserverRoNew,pint2RONew);
        
        //3. p-int matches on shorter and longer hostname ROP pservers, p-link exists on both, no change in plink or p-int on longer
        pint3ROOld= g.addV()
                .property("aai-node-type", "p-interface")
                .property("interface-name", "pint3RO")
                .property("aai-uri","/cloud-infrastructure/pservers/pserver/pserverRo.OldOne.aaaa.bbbbb/p-interfaces/p-interface/pint3RO")
                .next();

        Vertex pLink3ROOld = g.addV().property("aai-node-type", "physical-link")
                .property("link-name", "pLink3ROOld")
                .property("service-provider-bandwidth-up-value", 0)
                .property("service-provider-bandwidth-up-units", "empty")
                .property("service-provider-bandwidth-down-value", 0)
                .property("service-provider-bandwidth-down-units", "empty")
                .next();
        
        pint3RONew= g.addV()
                .property("aai-node-type", "p-interface")
                .property("interface-name", "pint3RO")
                .property("aai-uri","/cloud-infrastructure/pservers/pserver/pserverRo.NewOne.aaaa.ccccccccccc/p-interfaces/p-interface/pint3RO")
                .next();
        
        Vertex pLink3RONew = g.addV().property("aai-node-type", "physical-link")
                .property("link-name", "pLink3RONew")
                .property("service-provider-bandwidth-up-value", 0)
                .property("service-provider-bandwidth-up-units", "empty")
                .property("service-provider-bandwidth-down-value", 0)
                .property("service-provider-bandwidth-down-units", "empty")
                .next();

        edgeSerializer.addTreeEdge(g,pserverRoOld,pint3ROOld);
        edgeSerializer.addEdge(g,pint3ROOld,pLink3ROOld);
        edgeSerializer.addTreeEdge(g,pserverRoNew,pint3RONew);
        edgeSerializer.addEdge(g,pint3RONew,pLink3RONew);
        //End physical-links tests


//sc3
        edgeSerializer.addTreeEdge(g,pserverOldScn3,pintOldScn3);
        edgeSerializer.addTreeEdge(g,pserverNewScn3,pintNewScn3);
        edgeSerializer.addEdge(g,pintNewScn3,pLinkNewScn3);
        edgeSerializer.addEdge(g,pintOldScn3,pLinkOldScn3);

//sc2
        edgeSerializer.addTreeEdge(g,pserverOldScn2,pintOldScn2);
        edgeSerializer.addTreeEdge(g,pserverOldScn2,pintOld2Scn2);
        edgeSerializer.addTreeEdge(g,pserverNewScn2,pintNewScn2);
        edgeSerializer.addEdge(g,pintOldScn2,pLinkOldScn2);

//RCT fqdn not set new tests
    
        Vertex rctP1 = g.addV().property("aai-node-type", "pserver")
                .property("hostname","rctP1")
                .property("source-of-truth","RCT")
                .property("fqdn","sameFqdnScn1.rrrr.tttt.yyyy")
                .property("aai-uri","/cloud-infrastructure/pservers/pserver/rctP1")
                .property("resource-version","1")
                .next();
        
        // Don't throw null pointer with fqdn not set
        Vertex rctP2 = g.addV().property("aai-node-type", "pserver")
                .property("hostname","rctP2")
                .property("source-of-truth","RCT")
//                .property("fqdn","sameScn1.rrrr.tttt.yyyy")
                .property("aai-uri","/cloud-infrastructure/pservers/pserver/rctP2")
                .property("resource-version","2")
                .next();
        
        Vertex rctP3 = g.addV().property("aai-node-type", "pserver")
                .property("hostname","rctP3")
                .property("source-of-truth","RCT")
                .property("fqdn","sameFqdnScn1.rrrr.tttt.yyyy")
                .property("aai-uri","/cloud-infrastructure/pservers/pserver/rctP3")
                .property("resource-version","3")
                .next();

        Vertex rctP4 = g.addV().property("aai-node-type", "pserver")
                .property("hostname","rctP4")
                .property("source-of-truth","RCT")
                .property("fqdn","")
                .property("aai-uri","/cloud-infrastructure/pservers/pserver/rctP4")
                .property("resource-version","4")
                .next();
        
        Vertex rctP5 = g.addV().property("aai-node-type", "pserver")
                .property("hostname","rctP5")
                .property("source-of-truth","RCT")
                .property("fqdn","")
                .property("aai-uri","/cloud-infrastructure/pservers/pserver/rctP5")
                .property("resource-version","5")
                .next();
        
        //pint11 does not have a match on rctP3. So, expect this to move to rctP3. Add test
        Vertex pint11= g.addV()
                .property("aai-node-type", "p-interface")
                .property("interface-name", "pint11")
                .property("aai-uri","/cloud-infrastructure/pservers/pserver/rctP1/p-interfaces/p-interface/pint11")
                .next();
        
        // matching interface-name on pint12 and pint31. Delete pint12. Don't move it to rctP3. Add test
	        //interface-name matches b/w vertices pint12 and pint31
	        Vertex pint12= g.addV()
	                .property("aai-node-type", "p-interface")
	                .property("interface-name", "pint12")
	                .property("aai-uri","/cloud-infrastructure/pservers/pserver/rctP1/p-interfaces/p-interface/pint12")
	                .next();
	        
	        //int-name on pint31 is same as pint12
	        Vertex pint31= g.addV()
	                .property("aai-node-type", "p-interface")
	                .property("interface-name", "pint12")
	                .property("aai-uri","/cloud-infrastructure/pservers/pserver/rctP3/p-interfaces/p-interface/pint12")
	                .next();
        //End matching interface-name on pint12 and pint31. Delete pint12. Don't move it to rctP3.
        
        
        // Plink exists on both matching pints. Delete old pint, old plink, and edge b/w them - add test
	        // Vertex pint23 has physical link connected to pint14
		    Vertex pint14= g.addV()
		                .property("aai-node-type", "p-interface")
		                .property("interface-name", "pint14")
		                .property("aai-uri","/cloud-infrastructure/pservers/pserver/rctP1/p-interfaces/p-interface/pint14")
		                .next();
	        Vertex pint23= g.addV()
	                .property("aai-node-type", "p-interface")
	                .property("interface-name", "pint3RCTFqdn2")
	                .property("aai-uri","/cloud-infrastructure/pservers/pserver/rctP2/p-interfaces/p-interface/pint23")
	                .next();
	        Vertex plink1423 = g.addV()
	                .property("aai-node-type", "physical-link")
	                .property("link-name", "rctP1:pint14|rctP2:pint23")
	                .property("aai-uri","/network/physical-links/physical-link/plink1423")
	                .next();
	        
	        // Vertex pint24 has physical link connected to pint33 (Plink exists on both old and new p-int, no change)
	        // Vertex pint33 has same interface-name as pint14
	        Vertex pint33= g.addV()
	                .property("aai-node-type", "p-interface")
	                .property("interface-name", "pint14")
	                .property("aai-uri","/cloud-infrastructure/pservers/pserver/pint33/p-interfaces/p-interface/pint14")
	                .next();        
	        Vertex pint24= g.addV()
	                .property("aai-node-type", "p-interface")
	                .property("interface-name", "pint24")
	                .property("aai-uri","/cloud-infrastructure/pservers/pserver/rctP2/p-interfaces/p-interface/pint24")
	                .next();
	        Vertex plink2433 = g.addV()
	                .property("aai-node-type", "physical-link")
	                .property("link-name", "rctP2:pint24|rctP3:pint14")
	                .property("aai-uri","/network/physical-links/physical-link/plinkFqdn2443")
	                .next();
	     // End Plink exists on both matching pints. Delete old pint, old plink, and edge b/w them - add test
        
        Vertex pint41= g.addV()
                .property("aai-node-type", "p-interface")
                .property("interface-name", "pserver1RCTFqdn4")
                .property("aai-uri","/cloud-infrastructure/pservers/pserver/pserver1RCTFqdn4/p-interfaces/p-interface/pserver1RCTFqdn4")
                .next();
        
        Vertex pint51= g.addV()
                .property("aai-node-type", "p-interface")
                .property("interface-name", "pserver1RCTFqdn5")
                .property("aai-uri","/cloud-infrastructure/pservers/pserver/pserver1RCTFqdn4/p-interfaces/p-interface/pserver1RCTFqdn5")
                .next();
        
      //Case physical link moves from pint13 on rctP1 to pint32 on rctP3 since latest pserver does not have plink - Add test
	      //Vertex pint13 has plink connected to pint21
	        Vertex pint21= g.addV()
	                .property("aai-node-type", "p-interface")
	                .property("interface-name", "pint21")
	                .property("aai-uri","/cloud-infrastructure/pservers/pserver/rctP2/p-interfaces/p-interface/pint21")
	                .next();
	        Vertex pint13= g.addV()
	                .property("aai-node-type", "p-interface")
	                .property("interface-name", "pint13")
	                .property("aai-uri","/cloud-infrastructure/pservers/pserver/rctP1/p-interfaces/p-interface/pint13")
	                .next();
	        Vertex plink1321 = g.addV()
	                .property("aai-node-type", "physical-link")
	                .property("link-name", "rctP1:pint13|rctP2:pint21")
	                .property("aai-uri","/network/physical-links/physical-link/plink1321")
	                .next();
	        
	      //int-name on pint32 is same pint13
	        Vertex pint32= g.addV()
	                .property("aai-node-type", "p-interface")
	                .property("interface-name", "pint13")
	                .property("aai-uri","/cloud-infrastructure/pservers/pserver/rctP3/p-interfaces/p-interface/pint13")
	                .next();
	        
	        edgeSerializer.addTreeEdge(g,rctP1,pint13);
	        edgeSerializer.addTreeEdge(g,rctP2,pint21);
	        edgeSerializer.addTreeEdge(g, rctP3, pint32);
	        edgeSerializer.addEdge(g, plink1321, pint13);
	        edgeSerializer.addEdge(g, plink1321, pint21);
	        
	    // End Case physical link moves from pint13 on rctP1 to pint32 on rctP3 since latest pserver does not have plink
        
        
        edgeSerializer.addTreeEdge(g,rctP1,pint11);
        edgeSerializer.addTreeEdge(g,rctP1,pint12);
        edgeSerializer.addTreeEdge(g,rctP3,pint31);
        edgeSerializer.addTreeEdge(g,rctP3,pint33);
        edgeSerializer.addTreeEdge(g,rctP4,pint41);
        edgeSerializer.addTreeEdge(g,rctP5,pint51);
        
        edgeSerializer.addTreeEdge(g, rctP2, pint23);
        edgeSerializer.addTreeEdge(g, rctP1, pint14);
        edgeSerializer.addTreeEdge(g, rctP2, pint24);
        
        
        edgeSerializer.addEdge(g, plink1423, pint14);
        edgeSerializer.addEdge(g, plink1423, pint23);
        
        edgeSerializer.addEdge(g, plink2433, pint24);
        edgeSerializer.addEdge(g, plink2433, pint33);

 //lag-interfaces
      //lagint11 does not have a match on rctP3. So, expect this to move to rctP3. Add test
        Vertex lagint11= g.addV()
                .property("aai-node-type", "lag-interface")
                .property("interface-name", "lagint11")
                .property("aai-uri","/cloud-infrastructure/pservers/pserver/rctP1/lag-interfaces/lag-interface/lagint11")
                .next();
        edgeSerializer.addTreeEdge(g, rctP1, lagint11);
        
        //lagint12 matches with lagint31 on rctP3
        Vertex lagint12= g.addV()
                .property("aai-node-type", "lag-interface")
                .property("interface-name", "lagint12")
                .property("aai-uri","/cloud-infrastructure/pservers/pserver/rctP1/lag-interfaces/lag-interface/lagint12")
                .next();
        Vertex lagint31= g.addV()
                .property("aai-node-type", "lag-interface")
                .property("interface-name", "lagint12")
                .property("aai-uri","/cloud-infrastructure/pservers/pserver/rctP3/lag-interfaces/lag-interface/lagint12")
                .next();
        edgeSerializer.addTreeEdge(g, rctP1, lagint12);
        edgeSerializer.addTreeEdge(g, rctP3, lagint31);
        
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
        		.next();
        
        pserverRCT1Scn6 = g.addV().property("aai-node-type", "pserver")
                .property("hostname","Scn6.pserverRCT1Scn6")
                .property("source-of-truth","RCT")
                .property("fqdn","Scn6.pserverRCT1Scn6")
                .property("aai-uri","/cloud-infrastructure/pservers/pserver/pserverRCT1Scn6")
                .property("resource-version","4")
                .next();
        
        zoneScn62 = g.addV().property("aai-node-type", "zone")
        		.property("zone-id", "zone-62")
        		.next();
        
        edgeSerializer.addEdge(g,  pserverRCTScn6, zoneScn61);
        edgeSerializer.addEdge(g,  pserverRCT1Scn6, zoneScn62);
        
        //Verify manyToMany edge scenario
        Vertex gvnf1 = g.addV().property("aai-node-type", "generic-vnf")
        		.property("vnf-id", "vnf-1")
        		.next();
        
        Vertex gvnf2 = g.addV().property("aai-node-type", "generic-vnf")
        		.property("vnf-id", "vnf-2")
        		.next();
        
        edgeSerializer.addEdge(g,  pserverRCTScn6, gvnf1);
        edgeSerializer.addEdge(g,  pserverRCT1Scn6, gvnf2);
        
        
        // Empty string first token test
        Vertex pserver1EmptyFirstToken = g.addV().property("aai-node-type", "pserver")
                .property("hostname",".pserver1EmptyFirstToken")
                .property("source-of-truth","RO")
                .property("fqdn","sameScn1.rrrr.tttt.yyyy")
                .property("aai-uri","/cloud-infrastructure/pservers/pserver/.pserver1EmptyFirstToken")
                .property("resource-version","1")
                .next();
        Vertex pserver2EmptyFirstToken= g.addV().property("aai-node-type", "pserver")
                .property("hostname",".pserver2EmptyFirstToken.1")
                .property("source-of-truth","RO")
                .property("fqdn","sameScn1.rrrr.tttt.yyyy")
                .property("aai-uri","/cloud-infrastructure/pservers/pserver/.pserver2EmptyFirstToken")
                .property("resource-version","2")
                .next();
        
        Vertex pserver1EmptyFirstTokenFqdn = g.addV().property("aai-node-type", "pserver")
                .property("hostname","pserver1EmptyFirstTokenFqdn")
                .property("source-of-truth","RCT")
                .property("fqdn",".rrrr.tttt.yyyy")
                .property("aai-uri","/cloud-infrastructure/pservers/pserver/pserver1EmptyFirstTokenFqdn")
                .property("resource-version","1")
                .next();
        Vertex pserver2EmptyFirstTokenFqdn= g.addV().property("aai-node-type", "pserver")
                .property("hostname","pserver2EmptyFirstTokenFqdn")
                .property("source-of-truth","RCT")
                .property("fqdn",".rrrr.tttt.yyyy")
                .property("aai-uri","/cloud-infrastructure/pservers/pserver/pserver2EmptyFirstTokenFqdn")
                .property("resource-version","2")
                .next();
        
        TransactionalGraphEngine spy = spy(dbEngine);
        TransactionalGraphEngine.Admin adminSpy = spy(dbEngine.asAdmin());

        GraphTraversalSource traversal = g;
        GraphTraversalSource readOnly = tx.traversal(GraphTraversalSource.build().with(ReadOnlyStrategy.instance()));
        when (spy.tx()).thenReturn(tx);
        when(spy.asAdmin()).thenReturn(adminSpy);
        when(adminSpy.getTraversalSource()).thenReturn(traversal);
        when(adminSpy.getReadOnlyTraversalSource()).thenReturn(readOnly);

        migration = new MigrateSameSourcedRCTROPserverData(spy,loaderFactory, edgeIngestor, edgeSerializer, schemaVersions);
        migration.run();
    }

    @After
    public void cleanUp() {
        tx.tx().rollback();
        graph.close();
    }

    @Test
    public void RCT() throws Exception {
        assertEquals(true, g.V().has("aai-node-type", "pserver").has("hostname","pserverNew").in("tosca.relationships.network.BindsTo").has("aai-node-type","p-interface").hasNext());
        assertEquals("/cloud-infrastructure/pservers/pserver/pserverNew/p-interfaces/p-interface/pintOld", pintOld.property("aai-uri").value().toString());
        assertEquals("/cloud-infrastructure/pservers/pserver/pserverNew/p-interfaces/p-interface/pintOld/l-interfaces/l-interface/linterfaceold", lInterfaceold.property("aai-uri").value().toString());
        assertEquals("/cloud-infrastructure/pservers/pserver/pserverNew/p-interfaces/p-interface/pintOld2/l-interfaces/l-interface/linterfaceold2", lInterfaceold2.property("aai-uri").value().toString());
        assertEquals("/cloud-infrastructure/pservers/pserver/pserverNew/p-interfaces/p-interface/pintOld2/sriov-pfs/sriov-pf/sriovPfOld",sriovPfOld.property("aai-uri").value().toString());
        assertEquals(true, g.V().has("aai-node-type", "sriov-pf").has("pf-pci-id","sriovPfOld").in("org.onap.relationships.inventory.Uses").has("aai-node-type","sriov-vf").hasNext());
        assertEquals("/cloud-infrastructure/pservers/pserver/pserverNew/p-interfaces/p-interface/pintOld2/l-interfaces/l-interface/linterfaceold2/sriov-vfs/sriov-vf/sriovVfOld",sriovVfOld.property("aai-uri").value().toString());
        assertEquals(true, g.V().has("aai-node-type", "pserver").has("hostname","pserverNew").out("org.onap.relationships.inventory.LocatedIn").has("aai-node-type","complex").hasNext());

    }
    @Test
    public void RO() throws Exception {
        assertEquals(true, g.V().has("aai-node-type", "pserver").has("hostname","pserverRo.NewOne.aaaa.ccccccccccc").in("tosca.relationships.network.BindsTo").has("aai-node-type","p-interface").has("interface-name","pintOldRo").hasNext());
        assertEquals("/cloud-infrastructure/pservers/pserver/pserverRo.NewOne.aaaa.ccccccccccc/p-interfaces/p-interface/pintOldRo", pintOldRo.property("aai-uri").value().toString());
        assertEquals("/cloud-infrastructure/pservers/pserver/pserverRo.NewOne.aaaa.ccccccccccc/p-interfaces/p-interface/pintOldRo/l-interfaces/l-interface/linterfaceoldRo", lInterfaceoldRo.property("aai-uri").value().toString());
        assertEquals(true, g.V().has("aai-node-type", "pserver").has("hostname","pserverRo.NewOne.aaaa.ccccccccccc").out("org.onap.relationships.inventory.LocatedIn").has("aai-node-type","complex").hasNext());
//        System.out.println("************** SRIOV-PF *************"+g.V().has("aai-node-type","pserver").has("hostname","pserverRo.NewOne.aaaa.ccccccccccc").in("tosca.relationships.network.BindsTo")
//        		.has("aai-node-type","p-interface").has("interface-name","pintRo1").in().has("aai-node-type", "sriov-pf").toList().get(0).property("pf-pci-id").value().toString());
        assertEquals(true, g.V().has("aai-node-type","pserver").has("hostname","pserverRo.NewOne.aaaa.ccccccccccc").in("tosca.relationships.network.BindsTo")
        		.has("aai-node-type","p-interface").has("interface-name","pintRo1").in().has("aai-node-type", "sriov-pf").hasNext());
        //.has("pf-pci-id","sriovpfOldRo1")
        assertEquals("/cloud-infrastructure/pservers/pserver/pserverRo.NewOne.aaaa.ccccccccccc/p-interfaces/p-interface/pintRo1/sriov-pfs/sriov-pf/sriovpfOldRo1",
        		g.V().has("aai-node-type","sriov-pf").has("pf-pci-id","sriovpfOldRo1").toList().get(0).property("aai-uri").value().toString());
        assertNotEquals("/cloud-infrastructure/pservers/pserver/pserverRo.OldOne.aaaa.bbbbb/p-interfaces/p-interface/pintRo1/sriov-pfs/sriov-pf/sriovpfOldRo1",
        		g.V().has("aai-node-type","sriov-pf").has("pf-pci-id","sriovpfOldRo1").toList().get(0).property("aai-uri").value().toString());
    }

    @Test
    public void RCTplinkScenario3() throws Exception {
        assertEquals(false, g.V().has("aai-node-type", "pserver").has("hostname","pserverNewplink").in("tosca.relationships.network.BindsTo")
                .has("aai-node-type","p-interface").out("tosca.relationships.network.LinksTo").has("aai-node-type","physical-link").has("link-name","pLinkOld").hasNext());

    }

    @Test
    public void RCTpLinkMoveScn2NameChange() throws Exception {
        assertEquals("pserver2Scn2:pint1Scn2|pserver3Scn2:pint3Scn2", pLinkMoveScn2.property("link-name").value().toString());

    }

    @Test
    public void RCTpLinkMoveScn1NameChange() throws Exception {
        assertEquals("pserver2Scn1:pint1Scn1|pserver3Scn1:pint3Scn1", pLinkMoveScn1.property("link-name").value().toString());

    }

    @Test
    public void RCTplinkScenario2() throws Exception {
        assertEquals(true, g.V().has("aai-node-type", "pserver").has("hostname","pserverNewScn2").in("tosca.relationships.network.BindsTo")
                .has("aai-node-type","p-interface").out("tosca.relationships.network.LinksTo").has("aai-node-type","physical-link").has("link-name","pLinkOldScn2").hasNext());
        assertEquals("/cloud-infrastructure/pservers/pserver/pserverNewScn2/p-interfaces/p-interface/pintOld2Scn2", pintOld2Scn2.property("aai-uri").value().toString());

    }
    
    @Test
    public void roSuccessfulMovePlinkScn1() throws Exception {
        assertEquals("aaaa.cccc.cccc.dddd",g.V().has("aai-node-type","pserver").has("hostname","pserverRo.NewOne.aaaa.ccccccccccc").next().value("fqdn").toString());
        assertEquals(false, g.V().has("aai-node-type", "pserver").has("pserverRo.OldOne.aaaa.bbbbb").hasNext());
        assertEquals(true, g.V().has("aai-node-type", "pserver").has("hostname","pserverRo.NewOne.aaaa.ccccccccccc").in("tosca.relationships.network.BindsTo").has("aai-node-type","p-interface").has("interface-name","pint1ROOld").hasNext());
        assertEquals("/cloud-infrastructure/pservers/pserver/pserverRo.NewOne.aaaa.ccccccccccc/p-interfaces/p-interface/pint1ROOld", pint1ROOld.property("aai-uri").value().toString());
        assertEquals(true, g.V().has("aai-node-type", "pserver").has("hostname","pserverRo.NewOne.aaaa.ccccccccccc").in("tosca.relationships.network.BindsTo")
        		.has("aai-node-type","p-interface").has("interface-name","pint1ROOld").out("tosca.relationships.network.LinksTo").has("link-name","pLink1ROOld").hasNext());
    }
    
    @Test
    public void roSuccessfulSamePIntScn() throws Exception {
        assertEquals("aaaa.cccc.cccc.dddd",g.V().has("aai-node-type","pserver").has("hostname","pserverRo.NewOne.aaaa.ccccccccccc").next().value("fqdn").toString());
        assertEquals(false, g.V().has("aai-node-type", "pserver").has("pserverRo.OldOne.aaaa.bbbbb").hasNext());
        assertEquals(true, g.V().has("aai-node-type", "pserver").has("hostname","pserverRo.NewOne.aaaa.ccccccccccc").in("tosca.relationships.network.BindsTo").has("aai-node-type","p-interface").has("interface-name","pint2RO").hasNext());
        assertEquals("/cloud-infrastructure/pservers/pserver/pserverRo.NewOne.aaaa.ccccccccccc/p-interfaces/p-interface/pint2RO", pint2RONew.property("aai-uri").value().toString());
        assertEquals(true, g.V().has("aai-node-type", "pserver").has("hostname","pserverRo.NewOne.aaaa.ccccccccccc").in("tosca.relationships.network.BindsTo")
        		.has("aai-node-type","p-interface").has("interface-name","pint2RO").out("tosca.relationships.network.LinksTo").has("link-name","pLink2ROOld").hasNext());
    }
    
    @Test
    public void roSuccessfulSamePIntScnPlinkExistsOnBoth() throws Exception {
        assertEquals("aaaa.cccc.cccc.dddd",g.V().has("aai-node-type","pserver").has("hostname","pserverRo.NewOne.aaaa.ccccccccccc").next().value("fqdn").toString());
        assertEquals(false, g.V().has("aai-node-type", "pserver").has("pserverRo.OldOne.aaaa.bbbbb").hasNext());
        assertEquals(true, g.V().has("aai-node-type", "pserver").has("hostname","pserverRo.NewOne.aaaa.ccccccccccc").in("tosca.relationships.network.BindsTo").has("aai-node-type","p-interface").has("interface-name","pint3RO").hasNext());
        assertEquals("/cloud-infrastructure/pservers/pserver/pserverRo.NewOne.aaaa.ccccccccccc/p-interfaces/p-interface/pint3RO", pint3RONew.property("aai-uri").value().toString());
        assertEquals(true, g.V().has("aai-node-type", "pserver").has("hostname","pserverRo.NewOne.aaaa.ccccccccccc").in("tosca.relationships.network.BindsTo")
        		.has("aai-node-type","p-interface").has("interface-name","pint3RO").out("tosca.relationships.network.LinksTo").has("link-name","pLink3RONew").hasNext());
    }

    @Test
    public void RCThandleNullFqdnSamePints() throws Exception {
    	//1. pint11 from rctP1 moves to rctP3
    	assertEquals(true, g.V().has("aai-node-type", "pserver").has("hostname","rctP3").in("tosca.relationships.network.BindsTo")
        		.has("aai-node-type","p-interface").has("interface-name","pint11").hasNext());
    	assertEquals(false, g.V().has("aai-node-type", "p-interface").has("aai-uri","/cloud-infrastructure/pservers/pserver/rctP1/p-interfaces/p-interface/pint11").hasNext());
    	assertEquals(true, g.V().has("aai-node-type", "p-interface").has("aai-uri","/cloud-infrastructure/pservers/pserver/rctP3/p-interfaces/p-interface/pint11").hasNext());
    	
    	//2. pint12 int-name matches with pint31. So, verify that p-int does not move from rctP1 to rctP3
    	assertEquals("rctP3 has only 1 pint with name pint12", new Long(1L), g.V().has("aai-node-type", "pserver").has("hostname","rctP3").in("tosca.relationships.network.BindsTo")
        		.has("aai-node-type","p-interface").has("interface-name","pint12").count().next());
    	
    	//3. Verify that the p-interface from pserver is not moved to another pserver that has null fqdn
    	assertEquals(false, g.V().has("aai-node-type", "pserver").has("hostname","rctP2").in("tosca.relationships.network.BindsTo")
        		.has("aai-node-type","p-interface").has("interface-name","pint11").hasNext());
        		
    	//4. If the fqdn is "" within 2 RCT pservers, ignore that case. Don't move the p-int from old resource-version to new resource-version pserver
    	assertEquals(false, g.V().has("aai-node-type", "pserver").has("hostname","rctP5").in("tosca.relationships.network.BindsTo")
        		.has("aai-node-type","p-interface").has("interface-name","pint41").hasNext());
    	assertEquals("rctP5 has only 1 p-interface", new Long(1L), g.V().has("aai-node-type", "pserver").has("hostname","rctP5").in("tosca.relationships.network.BindsTo")
        		.has("aai-node-type","p-interface").count().next());		
       
    	//5. plink is moved from pint3 on pserver fqdn1 to pint2 on pserver fqdn3. Both p-ints have the same interface-name
    	assertEquals(true, g.V().has("aai-node-type", "pserver").has("hostname","rctP3").in("tosca.relationships.network.BindsTo")
    			.has("aai-node-type","p-interface").has("interface-name","pint13").out().has("aai-node-type", "physical-link").hasNext());
    	System.out.println("plink on pint13 is "+  g.V().has("aai-node-type", "pserver").has("hostname","rctP3").in("tosca.relationships.network.BindsTo")
		.has("aai-node-type","p-interface").has("interface-name","pint13").out().has("aai-node-type", "physical-link").next().property("link-name").value().toString());
    	
    	assertEquals(true, g.V().has("aai-node-type","physical-link").has("link-name","rctP2:pint21|rctP3:pint13").hasNext());
    	
    	//6. plink is not moved from pint4 on pserver fqdn1 to pint3 on pserver fqdn3. Both p-ints have the same interface-name
    	assertEquals(true, g.V().has("aai-node-type","physical-link").has("link-name","rctP2:pint24|rctP3:pint14").hasNext());
    	assertEquals(false, g.V().has("aai-node-type","physical-link").has("link-name","rctP1:pint14|rctP2:pint23").hasNext());
    	
    }
    
    @Test
    public void testRCTLagInterfaces() throws Exception {
    	//1. lagint11 from rctP1 moves to rctP3
    	assertEquals(true, g.V().has("aai-node-type", "pserver").has("hostname","rctP3").in("tosca.relationships.network.BindsTo")
        		.has("aai-node-type","lag-interface").has("interface-name","lagint11").hasNext());
    	assertEquals(false, g.V().has("aai-node-type", "lag-interface").has("aai-uri","/cloud-infrastructure/pservers/pserver/rctP1/lag-interfaces/lag-interface/lagint11").hasNext());
    	assertEquals(true, g.V().has("aai-node-type", "lag-interface").has("aai-uri","/cloud-infrastructure/pservers/pserver/rctP3/lag-interfaces/lag-interface/lagint11").hasNext());

    	
    	//2. lagint12 int-name matches with lagint31. So, verify that lag-int does not move from rctP1 to rctP3
    	assertEquals("rctP3 has only 1 lag-interface with name lagint12", new Long(1L), g.V().has("aai-node-type", "pserver").has("hostname","rctP3").in("tosca.relationships.network.BindsTo")
        		.has("aai-node-type","lag-interface").has("interface-name","lagint12").count().next());
    	
    }
    
    @Test
    public void checkRCTPserverHasRelnToOnly1Zone() throws Exception {

               assertEquals("Edge to only 1 Zone exists", new Long(1L), g.V().has("aai-node-type", "pserver").has("hostname","Scn6.pserverRCT1Scn6").out("org.onap.relationships.inventory.LocatedIn")
                .has("aai-node-type","zone").count().next());
               assertEquals(true, g.V().has("aai-node-type", "zone").has("zone-id","zone-62").hasNext());
               //Verify no edge exists from zone61 to lower resource-version RCT pserver
               assertEquals(false, g.V().has("aai-node-type", "zone").has("zone-id","zone-61").in().has("aai-node-type", "pserver").hasNext());
    }
    
    @Test
    public void checkRCTPserverHasRelnTo2GenericVnfs() throws Exception {

               assertEquals("Edge to 2 generic-vnfs exists", new Long(2L), g.V().has("aai-node-type", "pserver").has("hostname","Scn6.pserverRCT1Scn6").in("tosca.relationships.HostedOn")
                .has("aai-node-type","generic-vnf").count().next());
               assertEquals(true, g.V().has("aai-node-type", "generic-vnf").has("vnf-id","vnf-1").out().has("aai-node-type", "pserver").has("hostname", "Scn6.pserverRCT1Scn6").hasNext());
               //Verify no edge exists from vnf-1 to lower resource-version pserver
               assertEquals(false, g.V().has("aai-node-type", "generic-vnf").has("vnf-id","vnf-1").out().has("aai-node-type", "pserver").has("hostname", "Scn6.pserverRCTScn6").hasNext());
    }
    
    @Test
    public void roPlinkNewMovesToLongerHostNameROPserver() throws Exception {

    	assertEquals(false, g.V().has("aai-node-type", "pserver").has("hostname","pserverRo.NewOne.aaaa.ccccccccccc").in("tosca.relationships.network.BindsTo").has("aai-node-type","p-interface").has("interface-name","pintOldRo1").hasNext());
    	assertEquals(false, g.V().has("aai-node-type", "pserver").has("hostname","pserverRo.OldOne.aaaa.bbbbb").hasNext());
    	assertEquals(false, g.V().has("id", "pintOldRo1").hasNext());
    	//Verify that the physical link moves to the new pserver
    	assertEquals(true, g.V().has("aai-node-type", "pserver").has("hostname","pserverRo.NewOne.aaaa.ccccccccccc").in("tosca.relationships.network.BindsTo")
    			.has("aai-node-type","p-interface").has("interface-name","pintRo1").out().has("link-name","plinkROonOldRo1").hasNext());
    	//Verify complex does not get attached to pserverRO5
    	assertEquals("Complex is related to only 1 pserver", new Long(1L), g.V().has("physical-location-id", "complexOldRO").in("org.onap.relationships.inventory.LocatedIn").count().next());
    }
    
    @Test
    public void ignoreEmptyStringFirstTokenFqdn() throws Exception {
    	List<Vertex> pserverList = g.V().has("aai-node-type", "pserver").has("hostname").toList();
    	pserverList.forEach(v ->System.out.println(v.property("hostname").value().toString()));
    	assertEquals(true, g.V().has("aai-node-type", "pserver").has("hostname","pserver1EmptyFirstTokenFqdn").hasNext());
    	assertEquals(true, g.V().has("aai-node-type", "pserver").has("hostname",".pserver1EmptyFirstToken").hasNext());
    	
    	
    	System.out.println(UUID.randomUUID().toString());
    	System.out.println(UUID.randomUUID().toString());
    	
    }
    
}
