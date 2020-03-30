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
package org.onap.aai.util;

import org.apache.tinkerpop.gremlin.process.traversal.dsl.graph.GraphTraversalSource;
import org.apache.tinkerpop.gremlin.structure.Vertex;
import org.janusgraph.core.JanusGraph;
import org.janusgraph.core.JanusGraphTransaction;
import org.junit.*;
import org.junit.runners.MethodSorters;
import org.mockito.Mockito;
import org.onap.aai.AAISetup;
import org.onap.aai.db.props.AAIProperties;
import org.onap.aai.dbmap.AAIGraph;
import org.onap.aai.exceptions.AAIException;
import org.onap.aai.migration.EventAction;

import java.io.IOException;
import java.nio.file.Files;
import java.nio.file.Paths;
import java.nio.file.StandardOpenOption;
import java.util.concurrent.atomic.AtomicBoolean;

import static org.junit.Assert.assertEquals;
import static org.mockito.Mockito.*;

@FixMethodOrder(MethodSorters.NAME_ASCENDING)
public class SendDeleteMigrationNotificationsTest extends AAISetup {

	private final static String FILE = "./test.txt";

	private static AtomicBoolean graphCreated = new AtomicBoolean(false);

	private JanusGraph graph;
	private JanusGraphTransaction tx;
	private GraphTraversalSource g;

	private static final String REALTIME_CONFIG = "./src/main/resources/etc/appprops/janusgraph-realtime.properties";

	@Before
	public void setUp() throws Exception {
		System.setProperty("realtime.db.config", REALTIME_CONFIG);
		AAIGraph.getInstance();
		graph = AAIGraph.getInstance().getGraph();
		tx = graph.newTransaction();
		g = tx.traversal();

		createFile();
	}

	public void createFile() throws AAIException, IOException {
		
		/*String str = "pserver#@#/cloud-infrastructure/pservers/pserver/mtunj102sd9#@#{\"hostname\":\"mtunj102sd9\",\"ptnii-equip-name\":\"mtunj102sd9\",\"equip-type\":\"SERVER\",\"equip-vendor\":\"HP\",\"equip-model\":\"DL380p9-nd\",\"fqdn\":\"mtunjrsv102.mtunj.sbcglobal.net\",\"ipv4-oam-address\":\"10.64.220.7\",\"resource-version\":\"1523039038578\",\"purpose\":\"LCPA-3.0\",\"relationship-list\":{\"relationship\":[{\"related-to\":\"complex\",\"relationship-label\":\"org.onap.relationships.inventory.LocatedIn\",\"related-link\":\"/aai/v14/cloud-infrastructure/complexes/complex/MDTWNJ21A5\",\"relationship-data\":[{\"relationship-key\":\"complex.physical-location-id\",\"relationship-value\":\"MDTWNJ21A5\"}]}]}}";
		Files.write(Paths.get(FILE), str.getBytes());
		graphCreated.compareAndSet(false, true);
		*/
	    if(!graphCreated.get()){
			Vertex pserver1 = g.addV()
					.property("aai-node-type", "pserver")
					.property("hostname", SendDeleteMigrationNotifications.class.getSimpleName()+"-pserver-1")
					.property(AAIProperties.RESOURCE_VERSION, "333")
					.next();
			
			Vertex pserver2 = g.addV()
					.property("aai-node-type", "pserver")
					.property("hostname", SendDeleteMigrationNotifications.class.getSimpleName()+"-pserver-2")
					.property(AAIProperties.RESOURCE_VERSION, "334")
					.next();
			
			Vertex pserver3 = g.addV()
					.property("aai-node-type", "pserver")
					.property("hostname", SendDeleteMigrationNotifications.class.getSimpleName()+"-pserver-3")
					.property(AAIProperties.RESOURCE_VERSION, "335")
					.next();
			
			Vertex pserver4 = g.addV()
					.property("aai-node-type", "pserver")
					.property("hostname", SendDeleteMigrationNotifications.class.getSimpleName()+"-pserver-4")
					.property(AAIProperties.RESOURCE_VERSION, "336")
					.next();

			tx.commit();
			
			try{
				Files.createFile(Paths.get(FILE));
			}catch(Exception e) {
				e.printStackTrace();
			}
			String finalStr = "";
			finalStr = "pserver" + "#@#" + "/cloud-infrastructure/pservers/pserver/"+SendDeleteMigrationNotifications.class.getSimpleName()+"-pserver-1" + "#@#" + "{\"hostname\":\""+ SendDeleteMigrationNotifications.class.getSimpleName()+"-pserver-1\",\"resource-version\":\"333\"}" + "\n";
			Files.write(Paths.get(FILE), finalStr.getBytes(),StandardOpenOption.APPEND);
			finalStr = "pserver" + "#@#" + "/cloud-infrastructure/pservers/pserver/"+SendDeleteMigrationNotifications.class.getSimpleName()+"-pserver-2" + "#@#" + "{\"hostname\":\""+ SendDeleteMigrationNotifications.class.getSimpleName()+"-pserver-2\",\"resource-version\":\"334\"}" + "\n";
			Files.write(Paths.get(FILE), finalStr.getBytes(),StandardOpenOption.APPEND);
			finalStr = "pserver" + "#@#" + "/cloud-infrastructure/pservers/pserver/"+SendDeleteMigrationNotifications.class.getSimpleName()+"-pserver-3" + "#@#" + "{\"hostname\":\""+ SendDeleteMigrationNotifications.class.getSimpleName()+"-pserver-3\",\"resource-version\":\"335\"}" + "\n";
			Files.write(Paths.get(FILE), finalStr.getBytes(),StandardOpenOption.APPEND);
			finalStr = "pserver" + "#@#" + "/cloud-infrastructure/pservers/pserver/"+SendDeleteMigrationNotifications.class.getSimpleName()+"-pserver-4" + "#@#" + "{\"hostname\":\""+ SendDeleteMigrationNotifications.class.getSimpleName()+"-pserver-4\",\"resource-version\":\"336\"}" + "\n";
			Files.write(Paths.get(FILE), finalStr.getBytes(),StandardOpenOption.APPEND);
			graphCreated.compareAndSet(false, true);
		}
	}
	@AfterClass
	public static void cleanUp() throws IOException {
		Files.delete(Paths.get(FILE));
	}

	@After
	public void tearDown() throws IOException {
		if (tx.isOpen()) {
			tx.tx().rollback();
		}
	}

	@Test
	public void processEverything() throws Exception {
		SendDeleteMigrationNotifications s  = spy(new SendDeleteMigrationNotifications(
				loaderFactory, schemaVersions, REALTIME_CONFIG, FILE, 0, 0, "test", EventAction.DELETE, "DMAAP-LOAD"));
		doNothing().when(s).trigger();
		doNothing().when(s).cleanup();
		s.process("/aai/");
		assertEquals("1 events are created ", 4, s.notificationHelper.getNotifications().getEvents().size());

	}

	@Test
	public void processEverythingBatched2() throws Exception {
		SendDeleteMigrationNotifications s  = spy(new SendDeleteMigrationNotifications(
				loaderFactory, schemaVersions, REALTIME_CONFIG, FILE, 0, 2, "test", EventAction.DELETE, "DMAAP-LOAD"));
		doNothing().when(s).trigger();
		doNothing().when(s).cleanup();
		s.process("/aai/");
		Mockito.verify(s, times(2)).trigger();

	}

	@Test
	public void processEverythingBatched3() throws Exception {
		SendDeleteMigrationNotifications s  = spy(new SendDeleteMigrationNotifications(
				loaderFactory, schemaVersions,  REALTIME_CONFIG, FILE, 0, 3, "test", EventAction.DELETE, "DMAAP-LOAD"));
		doNothing().when(s).trigger();
		doNothing().when(s).cleanup();
		s.process("/aai/");
		Mockito.verify(s, times(2)).trigger();

	}

}