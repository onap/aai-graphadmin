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
import org.janusgraph.core.JanusGraphFactory;
import org.janusgraph.core.JanusGraphTransaction;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.AfterAll;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.MethodOrderer.MethodName;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.TestMethodOrder;
import org.mockito.Mockito;
import org.onap.aai.AAISetup;
import org.onap.aai.db.props.AAIProperties;
import org.onap.aai.dbmap.AAIGraph;
import org.onap.aai.exceptions.AAIException;
import org.onap.aai.migration.EventAction;

import java.io.IOException;
import java.nio.file.Files;
import java.nio.file.Path;
import java.util.*;
import java.util.concurrent.atomic.AtomicBoolean;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.mockito.Mockito.*;

@TestMethodOrder(MethodName.class)
public class SendMigrationNotificationsTest extends AAISetup {

	private final static String FILE = "./test.txt";

	private static AtomicBoolean graphCreated = new AtomicBoolean(false);

	private JanusGraph graph;
	private JanusGraphTransaction tx;
	private GraphTraversalSource g;

	private static final String REALTIME_CONFIG = "./src/main/resources/etc/appprops/janusgraph-realtime.properties";

	@BeforeEach
	public void setUp() throws Exception {
		System.setProperty("realtime.db.config", REALTIME_CONFIG);
		AAIGraph.getInstance();
		graph = AAIGraph.getInstance().getGraph();
		tx = graph.newTransaction();
		g = tx.traversal();

		createGraph();
	}

	public void createGraph() throws AAIException, IOException {
	    if(!graphCreated.get()){
			Vertex pnf1 = g.addV()
					.property("aai-node-type", "pnf")
					.property("pnf-name", SendMigrationNotifications.class.getSimpleName()+"-pnf-1")
					.property("aai-uri", "/network/pnfs/pnf/" + SendMigrationNotifications.class.getSimpleName()+"-pnf-1")
					.property(AAIProperties.RESOURCE_VERSION, "123")
					.next();

			Vertex pnf2 = g.addV()
					.property("aai-node-type", "pnf")
					.property("pnf-name", SendMigrationNotifications.class.getSimpleName()+"-pnf-2")
					.property("aai-uri", "/network/pnfs/pnf/" + SendMigrationNotifications.class.getSimpleName()+"-pnf-2")
					.property(AAIProperties.RESOURCE_VERSION, "456")
					.next();

			Vertex pnf3 = g.addV()
					.property("aai-node-type", "pnf")
					.property("pnf-name", SendMigrationNotifications.class.getSimpleName()+"-pnf-3")
					.property("aai-uri", "/network/pnfs/pnf/" + SendMigrationNotifications.class.getSimpleName()+"-pnf-3")
					.property(AAIProperties.RESOURCE_VERSION, "111")
					.next();

			Vertex pinterface1 = g.addV()
					.property("aai-node-type", "p-interface")
					.property("interface-name", SendMigrationNotifications.class.getSimpleName()+"-pinterface-1")
					.property("aai-uri", "/network/pnfs/pnf/" + SendMigrationNotifications.class.getSimpleName()+"-pnf-1" + "/p-interfaces/p-interface/" + SendMigrationNotifications.class.getSimpleName()+"-pinterface-1")
					.property(AAIProperties.RESOURCE_VERSION, "789")
					.next();

			Vertex pserver1 = g.addV()
					.property("aai-node-type", "pserver")
					.property("hostname", SendMigrationNotifications.class.getSimpleName()+"-pserver-1")
					.property("aai-uri", "/cloud-infrastructure/pservers/pserver/" + SendMigrationNotifications.class.getSimpleName()+"-pserver-1")
					.property(AAIProperties.RESOURCE_VERSION, "333")
					.next();

			edgeSerializer.addTreeEdge(g, pnf1, pinterface1);

			tx.commit();

			List<String> list = new ArrayList<>();
			list.add(pnf1.id().toString() + "_123"); // valid
			list.add(pnf2.id().toString() + "_345"); // invalid: no longer the current resource version
			list.add(pnf2.id().toString() + "_456"); // valid: same as above but with the correct resource version
			list.add(pinterface1.id().toString() + "_789"); // valid
			list.add(pnf3.id().toString() + "_222"); // invalid: wrong resource version
			list.add("345_345"); // invalid
			list.add(pserver1.id().toString() + "_333"); // valid
			Files.write(Path.of(FILE), (Iterable<String>)list.stream()::iterator);
			graphCreated.compareAndSet(false, true);
		}
	}
	@AfterAll
	public static void cleanUp() throws IOException {
		Files.delete(Path.of(FILE));
	}

	@AfterEach
	public void tearDown() throws IOException {
		if (tx.isOpen()) {
			tx.tx().rollback();
		}
	}

	@Test
	public void processEverything() throws Exception {
		SendMigrationNotifications s  = spy(new SendMigrationNotifications(
				loaderFactory, schemaVersions, REALTIME_CONFIG, FILE, Collections.emptySet(), 0, 0, "test", EventAction.UPDATE, "DMAAP-LOAD"));
		doNothing().when(s).trigger();
		doNothing().when(s).cleanup();
		s.process("/aai/");
		assertEquals(4, s.notificationHelper.getNotifications().getEvents().size(), "4 events are created ");

	}

	@Test
	public void processOnlyPnfs() throws Exception {
		SendMigrationNotifications s  = spy(new SendMigrationNotifications(
				loaderFactory, schemaVersions, REALTIME_CONFIG, FILE, new HashSet<>(Arrays.asList("pnf")), 0, 0, "test", EventAction.UPDATE, "DMAAP-LOAD"));
		doNothing().when(s).trigger();
		doNothing().when(s).cleanup();
		s.process("/aai/");
		assertEquals(2, s.notificationHelper.getNotifications().getEvents().size(), "2 events are created ");

	}

	@Test
	public void processOnlyPnfsAndPservers() throws Exception {
		SendMigrationNotifications s  = spy(new SendMigrationNotifications(
				loaderFactory, schemaVersions, REALTIME_CONFIG, FILE, new HashSet<>(Arrays.asList("pserver","pnf")), 0, 0, "test", EventAction.UPDATE, "DMAAP-LOAD"));
		doNothing().when(s).trigger();
		doNothing().when(s).cleanup();
		s.process("/aai/");
		assertEquals(3, s.notificationHelper.getNotifications().getEvents().size(), "3 events are created ");

	}

	@Test
	public void processEverythingBatched2() throws Exception {
		SendMigrationNotifications s  = spy(new SendMigrationNotifications(
				loaderFactory, schemaVersions, REALTIME_CONFIG, FILE, Collections.emptySet(), 0, 2, "test", EventAction.UPDATE, "DMAAP-LOAD"));
		doNothing().when(s).trigger();
		doNothing().when(s).cleanup();
		s.process("/aai/");
		Mockito.verify(s, times(2)).trigger();

	}

	@Test
	public void processEverythingBatched3() throws Exception {
		SendMigrationNotifications s  = spy(new SendMigrationNotifications(
				loaderFactory, schemaVersions,  REALTIME_CONFIG, FILE, Collections.emptySet(), 0, 3, "test", EventAction.UPDATE, "DMAAP-LOAD"));
		doNothing().when(s).trigger();
		doNothing().when(s).cleanup();
		s.process("/aai/");
		Mockito.verify(s, times(2)).trigger();

	}

}
