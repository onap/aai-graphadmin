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

import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collections;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;
import org.apache.tinkerpop.gremlin.process.traversal.dsl.graph.GraphTraversalSource;
import org.apache.tinkerpop.gremlin.process.traversal.strategy.verification.ReadOnlyStrategy;
import org.janusgraph.core.JanusGraph;
import org.janusgraph.core.JanusGraphFactory;
import org.janusgraph.core.JanusGraphTransaction;
import org.javatuples.Pair;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

import org.onap.aai.AAISetup;
import org.onap.aai.introspection.Loader;
import org.onap.aai.introspection.ModelType;
import org.onap.aai.serialization.engines.JanusGraphDBEngine;
import org.onap.aai.serialization.engines.QueryStyle;
import org.onap.aai.serialization.engines.TransactionalGraphEngine;

public class MigrateINVPhysicalInventoryMethodTest extends AAISetup {

	private final static ModelType introspectorFactoryType = ModelType.MOXY;
	private final static QueryStyle queryStyle = QueryStyle.TRAVERSAL;

	private Loader loader;
	private TransactionalGraphEngine dbEngine;
	private JanusGraph graph;
	private JanusGraphTransaction tx;
	private GraphTraversalSource g;
	private TransactionalGraphEngine spy;

	@BeforeEach
	public void setUp() throws Exception {
		graph = JanusGraphFactory.build().set("storage.backend","inmemory").open();
		tx = graph.newTransaction();
		g = tx.traversal();
		loader = loaderFactory.createLoaderForVersion(introspectorFactoryType, schemaVersions.getDefaultVersion());
		dbEngine = new JanusGraphDBEngine(
				queryStyle,
				loader);

		System.setProperty("BUNDLECONFIG_DIR", "src/test/resources");

		spy = spy(dbEngine);
		TransactionalGraphEngine.Admin adminSpy = spy(dbEngine.asAdmin());

		GraphTraversalSource traversal = g;
		GraphTraversalSource readOnly = graph.traversal().withStrategies(ReadOnlyStrategy.instance());
		when (spy.tx()).thenReturn(tx);
		when(spy.asAdmin()).thenReturn(adminSpy);
		when(adminSpy.getTraversalSource()).thenReturn(traversal);
		when(adminSpy.getReadOnlyTraversalSource()).thenReturn(readOnly);
	}
	
	@AfterEach
	public void cleanUp() {
		tx.tx().rollback();
		graph.close();
	}


	@Test
	public void headerTest() throws Exception {
		MigrateINVPhysicalInventory m = new MigrateINVPhysicalInventory(spy, loaderFactory, edgeIngestor, edgeSerializer, schemaVersions);
		String header = "ptnii-name,fic,equipment-model,equipment-role,equipment-role-additional,ip-addr,subnet-mask,slot-name,card-type,card-port-lock,card-vlan-lock,port-aid,port-type,port-role,port-lock,vlan-lock,reservation-name,collector-interconnect-type,tag-mode,media-type,media-speed-value+media-speed-units,uni-cir-value+uni-cir-units,evc-name";
		List<String> lines = new ArrayList<>();
		lines.add(header);
		assertEquals(header, m.processAndRemoveHeader(lines));
		assertEquals(0, lines.size());
		assertEquals(23, m.headerLength);

	}

	@Test
	public void verifyLineTest() throws Exception {
		MigrateINVPhysicalInventory m = new MigrateINVPhysicalInventory(spy, loaderFactory, edgeIngestor, edgeSerializer, schemaVersions);
		m.headerLength = 23;
		assertFalse(m.verifyLine(Collections.nCopies(5, "foo")));
		assertTrue(m.verifyLine(Collections.nCopies(23, "foo")));
		assertEquals(1, m.skippedRowsCount.intValue());

	}

	@Test
	public void readLineTest() throws Exception {
		MigrateINVPhysicalInventory m = new MigrateINVPhysicalInventory(spy, loaderFactory, edgeIngestor, edgeSerializer, schemaVersions);
		String line = "pnf-name-collector-1,06000D.121,5150,AED,,2001:1890:fcfe:7000:7021:0:1:2,64,,,,,\"1.7	\",SFP_1GE/Ethernet_10/100/1000M,ACCESS,N,N,M0651881_ST,SHARED,DOUBLE,SFP-1GE-LX,1000Mbps,,evc-name-1\n";
		Pair<String, String> pair = m.processLine(Arrays.asList(line.split(",", -1))).get();
		assertEquals("pnf-name-collector-1", pair.getValue0(), "Test 1");
		assertEquals("1.7", pair.getValue1(), "Test 1");

		line = "pnf-name-1,06000D.121,5150,AED,,2001:1890:fcfe:7000:7021:0:1:2,64,,,,,1.2,SFP_1GE/Ethernet_10/100/1000M,ACCESS,N,N,M0651882_ST,SHARED,DOUBLE,SFP-1GE-LX,1000Mbps,,evc-name-3";
		pair = m.processLine(Arrays.asList(line.split(",", -1))).get();
		assertEquals("pnf-name-1", pair.getValue0(), "Test 1");
		assertEquals("1.2", pair.getValue1(), "Test 1");

	}

	@Test
	public void getFileContentsTest() throws Exception {
		MigrateINVPhysicalInventory m = new MigrateINVPhysicalInventory(spy, loaderFactory, edgeIngestor, edgeSerializer, schemaVersions);

		Map<String,Set<String>> expected = new HashMap<>();
		List<String> lines = new ArrayList<>();

		String header = "ptnii-name,fic,equipment-model,equipment-role,equipment-role-additional,ip-addr,subnet-mask,slot-name,card-type,card-port-lock,card-vlan-lock,port-aid,port-type,port-role,port-lock,vlan-lock,reservation-name,collector-interconnect-type,tag-mode,media-type,media-speed-value+media-speed-units,uni-cir-value+uni-cir-units,evc-name";
		lines.add(header);

		lines.add("pnf-name-collector-1,06000D.121,5150,AED,,2001:1890:fcfe:7000:7021:0:1:2,64,,,,,\"1.7	\",SFP_1GE/Ethernet_10/100/1000M,ACCESS,N,N,M0651881_ST,SHARED,DOUBLE,SFP-1GE-LX,1000Mbps,,evc-name-1");
		expected.put("pnf-name-collector-1", new HashSet<>(Arrays.asList("1.7")));

		lines.add("pnf-name-1,06000D.121,5150,AED,,2001:1890:fcfe:7000:7021:0:1:2,64,,,,,1.2,SFP_1GE/Ethernet_10/100/1000M,ACCESS,N,N,M0651882_ST,SHARED,DOUBLE,SFP-1GE-LX,1000Mbps,,evc-name-3");
		lines.add("pnf-name-1,06000D.121,5150,AED,,2001:1890:fcfe:7000:7021:0:1:2,64,,,,,1.2,SFP_1GE/Ethernet_10/100/1000M,ACCESS,N,N,M0651882_ST,SHARED,DOUBLE,SFP-1GE-LX,1000Mbps,,evc-name-3");
		lines.add("pnf-name-1,06000D.121,5150,AED,,2001:1890:fcfe:7000:7021:0:1:2,64,,,,,1.3,SFP_1GE/Ethernet_10/100/1000M,ACCESS,N,N,M0651882_ST,SHARED,DOUBLE,SFP-1GE-LX,1000Mbps,,evc-name-3");
		expected.put("pnf-name-1", new HashSet<>(Arrays.asList("1.2", "1.3")));

		lines.add("foo");

		assertEquals(expected, m.getFileContents(lines));

	}
}
