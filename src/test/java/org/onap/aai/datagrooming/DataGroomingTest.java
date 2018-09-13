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

package org.onap.aai.datagrooming;

import com.att.eelf.configuration.EELFLogger;
import com.att.eelf.configuration.EELFManager;
import org.janusgraph.core.JanusGraphTransaction;
import org.apache.tinkerpop.gremlin.process.traversal.dsl.graph.GraphTraversalSource;
import org.apache.tinkerpop.gremlin.structure.Edge;
import org.apache.tinkerpop.gremlin.structure.Vertex;
import org.junit.After;
import org.junit.Before;
import org.junit.FixMethodOrder;
import org.junit.Test;
import org.junit.runners.MethodSorters;
import org.onap.aai.AAISetup;
import org.onap.aai.dbmap.AAIGraph;
import org.onap.aai.exceptions.AAIException;

import java.io.ByteArrayInputStream;
import java.io.InputStream;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;

import static org.hamcrest.CoreMatchers.is;
import static org.hamcrest.MatcherAssert.assertThat;
import static org.junit.Assert.*;

@FixMethodOrder(MethodSorters.NAME_ASCENDING)
public class DataGroomingTest extends AAISetup {

	private static final EELFLogger logger = EELFManager.getInstance().getLogger(DataGroomingTest.class);

	private DataGrooming dataGrooming;

	private Vertex cloudRegionVertex;

	private boolean setUp = false;

	@Before
	public void setup() {
		dataGrooming = new DataGrooming(loaderFactory, schemaVersions);
		// deleteTool.SHOULD_EXIT_VM = false;
		JanusGraphTransaction transaction = AAIGraph.getInstance().getGraph().newTransaction();
		boolean success = true;
		try {
			GraphTraversalSource g = transaction.traversal();
			cloudRegionVertex = g.addV().property("aai-node-type", "cloud-region").property("cloud-owner", "test-owner")
					.property("cloud-region-id", "test-region").property("source-of-truth", "JUNIT").next();

			Vertex cloudRegionVertexDupe = g.addV().property("aai-node-type", "cloud-region")
					.property("cloud-owner", "test-owner").property("cloud-region-id", "test-region")
					.property("source-of-truth", "JUNIT").next();

			Vertex cloudRegionDupe3 = g.addV().property("aai-node-type", "cloud-region")
					.property("cloud-owner", "test-owner").property("cloud-region-id", "test-region")
					.property("source-of-truth", "JUNIT").next();

			Vertex cloudRegionDupe4 = g.addV().property("aai-node-type", "cloud-region")
					.property("cloud-owner", "test-owner").property("cloud-region-id", "test-region")
					.property("source-of-truth", "JUNIT").next();

			Vertex cloudRegionDupe5 = g.addV().property("aai-node-type", "cloud-region")
					.property("cloud-owner", "test-owner").property("cloud-region-id", "test-region")
					.property("source-of-truth", "JUNIT").next();
			
			Vertex cloudRegionVertexBadNode = g.addV().property("aai-node-type", "cloud-region")
					.property("cloud-owner", "test-owner-noregionId").property("source-of-truth", "JUNIT").next();

			
			Vertex cloudRegionVertexBadNode2 = g.addV().property("aai-node-type", "cloud-region")
					.property("cloud-region", "test-owner-noownerId").property("source-of-truth", "JUNIT").next();

			Vertex cloudRegionVertexBadNode3 = g.addV().property("aai-node-type", "cloud-region")
					.property("cloud-region", "test-owner-noownerId2").property("source-of-truth", "JUNIT").next();

			Vertex tenantGhostNodeNoNT = g.addV().property("tenant-id", "test-owner-tenant-id-1")
					.property("source-of-truth", "JUNIT").next();

			Vertex cloudRegionNoNT = g.addV().property("cloud-region", "test-owner-noownerIdnont-1")
					.property("cloud-owner", "test-owner-noregion-nont2").property("source-of-truth", "JUNIT").next();

			Vertex tenantNoNT = g.addV().property("tenant-id", "test-owner-tenant-id-1")
					.property("source-of-truth", "JUNIT").next();

			Vertex tenantNoKey = g.addV().property("aai-node-type", "tenant").property("source-of-truth", "JUNIT")
					.next();

			Vertex cloudRegionNoKey = g.addV().property("aai-node-type", "cloud-region")
					.property("source-of-truth", "JUNIT").next();

			Vertex tenantNoParent = g.addV().property("aai-node-type", "tenant")
					.property("tenant-id", "test-owner-tenant-id").property("source-of-truth", "JUNIT").next();

			Vertex tenantNoParent1 = g.addV().property("aai-node-type", "tenant")
					.property("tenant-id", "test-owner-tenant-id1").property("source-of-truth", "JUNIT").next();

			Vertex tenantNoParentDupe1 = g.addV().property("aai-node-type", "tenant")
					.property("tenant-id", "test-owner-tenant-id1").property("source-of-truth", "JUNIT").next();

			Vertex tenantNoParentDupe2 = g.addV().property("aai-node-type", "tenant")
					.property("tenant-id", "test-owner-tenant-id1").property("source-of-truth", "JUNIT").next();

			Vertex tenantDupe3 = g.addV().property("aai-node-type", "tenant")
					.property("tenant-id", "test-owner-tenant-id1").property("source-of-truth", "JUNIT").next();
			Vertex tenantDupe4 = g.addV().property("aai-node-type", "tenant")
					.property("tenant-id", "test-owner-tenant-id1").property("source-of-truth", "JUNIT").next();

			Vertex tenantNoParent2 = g.addV().property("aai-node-type", "tenant")
					.property("tenant-id", "test-owner-tenant-id2").property("source-of-truth", "JUNIT").next();

			tenantNoParent2.property("aai-uuid", tenantNoParent2.id() + "dummy");

			Vertex tenantVertex = g.addV().property("aai-node-type", "tenant").property("tenant-id", "test-tenant")
					.property("source-of-truth", "JUNIT").next();

			Vertex pserverVertex = g.addV().property("aai-node-type", "pserver").property("hostname", "test-pserver")
					.property("in-maint", false).property("source-of-truth", "JUNIT").next();

			Vertex azNokey = g.addV().property("aai-node-type", "availability-zone")
					.property("source-of-truth", "JUNIT").next();

			cloudRegionVertex.addEdge("BadEdge", tenantGhostNodeNoNT, null);
			edgeSerializer.addTreeEdge(g, cloudRegionVertex, tenantVertex);
			edgeSerializer.addTreeEdge(g, cloudRegionVertex, tenantDupe3);
			edgeSerializer.addTreeEdge(g, cloudRegionVertex, tenantDupe4);
			edgeSerializer.addTreeEdge(g, cloudRegionNoKey, tenantNoKey);
			edgeSerializer.addEdge(g, pserverVertex, azNokey);

			cloudRegionNoNT.addEdge("Base Edge2", tenantNoNT, null);

		} catch (Exception ex) {
			success = false;
			logger.error("Unable to create the vertexes", ex);
		} finally {
			if (success) {
				transaction.commit();
			} else {
				transaction.rollback();
				fail("Unable to setup the graph");
			}
		}
	}

	@Test
	public void testGroomingNonAutoFix() throws AAIException {
		String[] args = {
				"-edgesOnly", "false", "-autoFix ", "false", "-skipHostCheck ", "true", "-dontFixOrphans ", "true"
		};

		dataGrooming.execute(args);
		/*
		 * 2 GhostNodes - CloudRegions 1 OrphaNode - tenant
		 */
		assertThat(dataGrooming.getGhostNodeCount(), is(5));
		assertThat(dataGrooming.getOrphanNodeCount(), is(5));
		assertThat(dataGrooming.getMissingAaiNtNodeCount(), is(1));
		assertThat(dataGrooming.getOneArmedEdgeHashCount(), is(3));
	}

	@Test
	public void testGroomingWithAutoFix() throws AAIException {
		String[] args = {
				"-autoFix ", "true", "-edgesOnly", "false", "-skipHostCheck ", "false", "-dontFixOrphans ", "false",
				"-skipIndexUpdateFix", "true", "-sleepMinutes", "1", "-timeWindowMinutes", "100", "-dupeFixOn", "true"
		};

		dataGrooming.execute(args);
		assertThat(dataGrooming.getDeleteCandidateList().size(), is(19));
		assertThat(dataGrooming.getDeleteCount(), is(18));
	}

	@Test
	public void testGroomingUpdateIndexedProps() throws AAIException {

		JanusGraphTransaction transaction = AAIGraph.getInstance().getGraph().newTransaction();
		GraphTraversalSource g = transaction.traversal();
		Vertex cloudRegionVertex1 = g.addV().property("aai-node-type", "cloud-region")
				.property("cloud-owner", "test-owner-partial").property("cloud-region-id", "test-region")
				.property("source-of-truth", "JUNIT").next();
		dataGrooming.updateIndexedProps(cloudRegionVertex1, "1", "cloud-region", new HashMap<>(), new ArrayList<>());
		transaction.rollback();
		// TODO asset something
	}

	@Test
	public void testGroomingGettersAndSetters() throws AAIException {

		dataGrooming.setGhostNodeHash(new HashMap<>());
		dataGrooming.setOrphanNodeHash(new HashMap<>());
		dataGrooming.setMissingAaiNtNodeHash(new HashMap<>());
		dataGrooming.setOneArmedEdgeHash(new HashMap<>());
		dataGrooming.setDeleteCandidateList(new HashSet<>());
		dataGrooming.setDeleteCount(0);

		assertThat(dataGrooming.getGhostNodeCount(), is(0));
		assertThat(dataGrooming.getOrphanNodeCount(), is(0));
		assertThat(dataGrooming.getMissingAaiNtNodeCount(), is(0));
		assertThat(dataGrooming.getOneArmedEdgeHashCount(), is(0));
		assertThat(dataGrooming.getDeleteCandidateList().size(), is(0));
		assertThat(dataGrooming.getDeleteCount(), is(0));
	}

	@Test
	public void testGroomingNoArgs() throws AAIException {
		String[] args = {

		};
		dataGrooming.execute(args);
		assertThat(dataGrooming.getGhostNodeCount(), is(5));
		assertThat(dataGrooming.getOrphanNodeCount(), is(5));
		assertThat(dataGrooming.getMissingAaiNtNodeCount(), is(1));
		assertThat(dataGrooming.getOneArmedEdgeHashCount(), is(3));
		assertThat(dataGrooming.getDeleteCandidateList().size(), is(0));
		assertThat(dataGrooming.getDeleteCount(), is(0));
	}

	@Test
	public void testGroomingDupeCheck() throws AAIException {
		String[] args = {
		};

		dataGrooming.execute(args);
		assertThat(dataGrooming.getDupeGroups().size(), is(2));
	}

	@Test
	public void testGroomingAutoFixMaxRecords() throws AAIException {

		String[] args = { "-autoFix ", "true", "-maxFix", "0",  "-edgesOnly",
		"true" , "-sleepMinutes", "1"};
		dataGrooming.execute(args);
		assertThat(dataGrooming.getDeleteCandidateList().size(), is(0));

	}

	@After
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