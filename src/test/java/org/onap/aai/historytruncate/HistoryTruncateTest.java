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
package org.onap.aai.historytruncate;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import org.janusgraph.core.JanusGraph;
import org.janusgraph.core.JanusGraphTransaction;
import org.apache.tinkerpop.gremlin.process.traversal.dsl.graph.GraphTraversalSource;
import org.apache.tinkerpop.gremlin.structure.Edge;
import org.apache.tinkerpop.gremlin.structure.Vertex;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.MethodOrderer.MethodName;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.TestMethodOrder;
import org.onap.aai.AAISetup;
import org.onap.aai.dbmap.AAIGraph;
import org.onap.aai.exceptions.AAIException;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.HashSet;

import static org.hamcrest.CoreMatchers.is;
import static org.hamcrest.MatcherAssert.assertThat;
import static org.junit.jupiter.api.Assertions.*;

@TestMethodOrder(MethodName.class)
public class HistoryTruncateTest extends AAISetup {

	private static final Logger logger = LoggerFactory.getLogger(HistoryTruncateTest.class);

	private HistoryTruncate historyTruncate;
	
	private long todayTs;
	private long todayMinusOneWeekTs;
	private long todayMinusOneMonthTs;
	private long todayMinusTwoMonthsTs;
	private long todayMinus55DaysTs;

	@BeforeEach
	public void setup() {
		historyTruncate = new HistoryTruncate();
		
		String [] argsAr = {};
		HistoryTruncate.main(argsAr);
		JanusGraphTransaction currentTransaction = AAIGraph.getInstance().getGraph().newTransaction();
		boolean success = true;
			
		todayTs = System.currentTimeMillis();
		todayMinusOneWeekTs = todayTs - (7 * 24 * 60 * 60L * 1000);
		todayMinusOneMonthTs = todayTs - (30 * 24 * 60 * 60L * 1000);
		todayMinusTwoMonthsTs = todayTs - (60 * 24 * 60 * 60L * 1000);
		todayMinus55DaysTs = todayTs - (55 * 24 * 60 * 60L * 1000);
		try {
			GraphTraversalSource g = currentTransaction.traversal();
			
			// --------- These two have no end-ts 
			Vertex cloudRegionVertex1 = g.addV()
					.property("aai-node-type", "cloud-region")
					.property("cloud-owner", "test-owner1")
					.property("cloud-region-id", "test-region1")
					.property("source-of-truth", "JUNIT")
					.property("aai-uri", "aai-uriX01")
					.property("aai-last-mod-ts","19191919").next();
			Vertex tenantVertex1 = g.addV()
					.property("aai-node-type", "tenant")
					.property("tenant-id", "test-tenant1")
					.property("aai-uri", "aai-uriX21")
					.property("source-of-truth", "JUNIT").next();			
			
			// ---------- These two have end-ts one week ago
			Vertex cloudRegionVertex2 = g.addV()
					.property("aai-node-type", "cloud-region")
					.property("cloud-owner", "test-owner2")
					.property("cloud-region-id", "test-region2")
					.property("source-of-truth", "JUNIT")
					.property("aai-uri", "aai-uriX02")
					.property("end-ts", todayMinusOneWeekTs)
					.property("aai-last-mod-ts","19191919").next();
			Vertex tenantVertex2 = g.addV()
					.property("aai-node-type", "tenant")
					.property("tenant-id", "test-tenant2")
					.property("aai-uri", "aai-uriX22")
					.property("end-ts", todayMinusOneWeekTs)
					.property("source-of-truth", "JUNIT").next();

			// --------- These 7 have end-ts one month ago
			Vertex cloudRegionVertex3 = g.addV()
					.property("aai-node-type", "cloud-region")
					.property("cloud-owner", "test-owner3")
					.property("cloud-region-id", "test-region3")
					.property("source-of-truth", "JUNIT")
					.property("aai-uri", "aai-uriX03")
					.property("end-ts", todayMinusOneMonthTs)
					.property("aai-last-mod-ts","19191919").next();
			Vertex tenantVertex3 = g.addV()
					.property("aai-node-type", "tenant")
					.property("tenant-id", "test-tenant3")
					.property("aai-uri", "aai-uriX23")
					.property("end-ts", todayMinusOneMonthTs)
					.property("source-of-truth", "JUNIT").next();

			Vertex cloudRegionVertex4 = g.addV()
					.property("aai-node-type", "cloud-region")
					.property("cloud-owner", "test-owner4")
					.property("cloud-region-id", "test-region4")
					.property("source-of-truth", "JUNIT")
					.property("aai-uri", "aai-uriX04")
					.property("end-ts", todayMinusOneMonthTs)
					.property("aai-last-mod-ts","19191919").next();
			Vertex tenantVertex4 = g.addV()
					.property("aai-node-type", "tenant")
					.property("tenant-id", "test-tenant4")
					.property("aai-uri", "aai-uriX24")
					.property("end-ts", todayMinusOneMonthTs)
					.property("source-of-truth", "JUNIT").next();

			Vertex cloudRegionVertex5 = g.addV()
					.property("aai-node-type", "cloud-region")
					.property("cloud-owner", "test-owner5")
					.property("cloud-region-id", "test-region5")
					.property("source-of-truth", "JUNIT")
					.property("aai-uri", "aai-uriX05")
					.property("end-ts", todayMinusOneMonthTs)
					.property("aai-last-mod-ts","19191919").next();
			Vertex tenantVertex5 = g.addV()
					.property("aai-node-type", "tenant")
					.property("tenant-id", "test-tenant5")
					.property("aai-uri", "aai-uriX25")
					.property("end-ts", todayMinusOneMonthTs)
					.property("source-of-truth", "JUNIT").next();

			Vertex cloudRegionVertex6 = g.addV()
					.property("aai-node-type", "cloud-region")
					.property("cloud-owner", "test-owner6")
					.property("cloud-region-id", "test-region6")
					.property("source-of-truth", "JUNIT")
					.property("aai-uri", "aai-uriX06")
					.property("end-ts", todayMinusOneMonthTs)
					.property("aai-last-mod-ts","19191919").next();


		} catch (Exception ex) {
			success = false;
			logger.error("Unable to create the vertexes", ex);
		} finally {
			if (success) {
				currentTransaction.commit();
			} else {
				currentTransaction.rollback();
				fail("Unable to setup the graph");
			}
		}
	}

	
	@Test
	public void testZeroWindow() throws AAIException {		
		JanusGraph jgraph = AAIGraph.getInstance().getGraph();	
		assertThat(historyTruncate.getCandidateEdgeCount(jgraph,0), is(0));
		assertThat(historyTruncate.getCandidateVertexCount(jgraph,0), is(9));
	}
	
	@Test
	public void test5DayWindow() throws AAIException {		
		JanusGraph jgraph = AAIGraph.getInstance().getGraph();	
		assertThat(historyTruncate.getCandidateEdgeCount(jgraph,5), is(0));
		assertThat(historyTruncate.getCandidateVertexCount(jgraph,5), is(9));
	}

	@Test
	public void testTenDayWindow() throws AAIException {		
		JanusGraph jgraph = AAIGraph.getInstance().getGraph();	
		assertThat(historyTruncate.getCandidateEdgeCount(jgraph,10), is(0));
		assertThat(historyTruncate.getCandidateVertexCount(jgraph,10), is(7));
	}

	@Test
	public void test90DayWindow() throws AAIException {		
		JanusGraph jgraph = AAIGraph.getInstance().getGraph();	
		assertThat(historyTruncate.getCandidateEdgeCount(jgraph,40), is(0));
		assertThat(historyTruncate.getCandidateVertexCount(jgraph,40), is(0));
	}

	@Test
	public void testCalcTimeStamp() throws AAIException {				
		long ts1 = historyTruncate.calculateTruncWindowEndTimeStamp(0);
		long ts2 = historyTruncate.calculateTruncWindowEndTimeStamp(10);
		assertTrue( 0L < ts2);
		assertTrue(ts2 < ts1);
	}
	
	
	@Test
	public void testProcessVerts() throws AAIException {	
		JanusGraph jgraph = AAIGraph.getInstance().getGraph();	
		
		// - note - when commitBatchSize is set to "2", then this test makes sure that
		//      batch processing works.
		
		// Create 7 records with end-ts of 2 months ago
		make7NodesWith60DayEndTs();
		assertThat(historyTruncate.getCandidateVertexCount(jgraph,55), is(7));
		
		// process those 7 records -first with only logging
		Boolean doLoggingFlag = true;
		Boolean doTheDeleteFlag = false;
		historyTruncate.processVerts(jgraph, todayMinus55DaysTs, doLoggingFlag, doTheDeleteFlag);
		
		// Nodes should still be there since doDelete was false
		assertThat(historyTruncate.getCandidateVertexCount(jgraph,55), is(7));
		
		// process the 7 records, but do the delete
		doTheDeleteFlag = true;
		historyTruncate.processVerts(jgraph, todayMinus55DaysTs, doLoggingFlag, doTheDeleteFlag);
		
		// Check that they were deleted 
		assertThat(historyTruncate.getCandidateVertexCount(jgraph,55), is(0));
		
	}
	
	@Test
	public void test4BadArgs() throws AAIException {	
		
		// try passing a bad mode
		String [] argsAr = {"-truncateWindowDays", "888","-truncateMode","badMode"};
		assertFalse(historyTruncate.executeCommand(argsAr) );
		
		// try passing a bad window value
		String [] argsAr2 = {"-truncateWindowDays", "88xx8","-truncateMode","LOG_ONLY"};
		assertFalse(historyTruncate.executeCommand(argsAr2) );
		
		// try passing a bad option name
		String [] argsAr3 = {"-trunxxxxxxxxxxxcateWindowDays", "888","-truncateMode","LOG_ONLY"};
		assertFalse(historyTruncate.executeCommand(argsAr3) );
		
		// try passing good things
		String [] argsAr4 = {"-truncateWindowDays", "888","-truncateMode","LOG_ONLY"};
		assertTrue(historyTruncate.executeCommand(argsAr4) );
		
		// try passing no args (should default to LOG_ONLY mode)
		String [] argsAr5 = {};
		assertTrue(historyTruncate.executeCommand(argsAr5) );
		
	}
	
	
	
	public void make7NodesWith60DayEndTs() {
		boolean success = true;
		JanusGraphTransaction transaction2 = AAIGraph.getInstance().getGraph().newTransaction();
		try {
			GraphTraversalSource g2 = transaction2.traversal();
			// --------- These have end-ts two months ago
			Vertex cloudRegionVertex991 = g2.addV()
					.property("aai-node-type", "cloud-region")
					.property("cloud-owner", "test-owner991")
					.property("cloud-region-id", "test-region991")
					.property("source-of-truth", "JUNIT")
					.property("aai-uri", "aai-uriX0991")
					.property("end-ts", todayMinusTwoMonthsTs)
					.property("aai-last-mod-ts","19191919").next();
			Vertex tenantVertex991 = g2.addV()
					.property("aai-node-type", "tenant")
					.property("tenant-id", "test-tenant991")
					.property("aai-uri", "aai-uriX2991")
					.property("end-ts", todayMinusTwoMonthsTs)
					.property("source-of-truth", "JUNIT").next();
			
			Vertex cloudRegionVertex992 = g2.addV()
					.property("aai-node-type", "cloud-region")
					.property("cloud-owner", "test-owner992")
					.property("cloud-region-id", "test-region992")
					.property("source-of-truth", "JUNIT")
					.property("aai-uri", "aai-uriX0992")
					.property("end-ts", todayMinusTwoMonthsTs)
					.property("aai-last-mod-ts","19191919").next();
			Vertex tenantVertex992 = g2.addV()
					.property("aai-node-type", "tenant")
					.property("tenant-id", "test-tenant992")
					.property("aai-uri", "aai-uriX2992")
					.property("end-ts", todayMinusTwoMonthsTs)
					.property("source-of-truth", "JUNIT").next();
			
			Vertex cloudRegionVertex993 = g2.addV()
					.property("aai-node-type", "cloud-region")
					.property("cloud-owner", "test-owner993")
					.property("cloud-region-id", "test-region993")
					.property("source-of-truth", "JUNIT")
					.property("aai-uri", "aai-uriX0993")
					.property("end-ts", todayMinusTwoMonthsTs)
					.property("aai-last-mod-ts","19191919").next();
			Vertex tenantVertex993 = g2.addV()
					.property("aai-node-type", "tenant")
					.property("tenant-id", "test-tenant993")
					.property("aai-uri", "aai-uriX2993")
					.property("end-ts", todayMinusTwoMonthsTs)
					.property("source-of-truth", "JUNIT").next();
			
			Vertex cloudRegionVertex994 = g2.addV()
					.property("aai-node-type", "cloud-region")
					.property("cloud-owner", "test-owner994")
					.property("cloud-region-id", "test-region994")
					.property("source-of-truth", "JUNIT")
					.property("aai-uri", "aai-uriX0994")
					.property("end-ts", todayMinusTwoMonthsTs)
					.property("aai-last-mod-ts","19191919").next();
					

		} catch (Exception ex) {
			success = false;
			logger.error("Unable to create the 7 vertices with end-ts = 60 days. ", ex);
		} finally {
			if (success) {
				transaction2.commit();
			} else {
				transaction2.rollback();
				fail("Unable to setup the vertex with end-ts = 60 ");
			}
		}
	}


	
	@AfterEach
	public void tearDown() {

		JanusGraphTransaction transaction = AAIGraph.getInstance().getGraph().newTransaction();
		boolean success = true;
		try {
			GraphTraversalSource g = transaction.traversal();
			g.V().has("source-of-truth", "JUNIT").toList().forEach(v -> v.remove());

		} catch (Exception ex) {
			success = false;
			logger.error("Unable to remove the vertexes", ex);
		} finally {
			if (success) {
				transaction.commit();
			} else {
				transaction.rollback();
				fail("Unable to teardown the graph");
			}
		}
	}
}