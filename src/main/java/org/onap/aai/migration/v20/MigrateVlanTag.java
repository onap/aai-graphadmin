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
package org.onap.aai.migration.v20;

import java.io.FileNotFoundException;
import java.io.FileReader;
import java.io.IOException;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.Set;
import java.util.concurrent.atomic.AtomicInteger;

import org.apache.tinkerpop.gremlin.process.traversal.dsl.graph.GraphTraversal;
import org.apache.tinkerpop.gremlin.structure.Vertex;
import org.javatuples.Pair;
import org.json.simple.JSONArray;
import org.json.simple.JSONObject;
import org.json.simple.parser.JSONParser;
import org.onap.aai.db.props.AAIProperties;
import org.onap.aai.edges.EdgeIngestor;
import org.onap.aai.introspection.Introspector;
import org.onap.aai.introspection.LoaderFactory;
import org.onap.aai.migration.EdgeSwingMigrator;
import org.onap.aai.migration.Enabled;
import org.onap.aai.migration.MigrationDangerRating;
import org.onap.aai.migration.MigrationPriority;
import org.onap.aai.migration.Status;
import org.onap.aai.serialization.db.EdgeSerializer;
import org.onap.aai.serialization.engines.TransactionalGraphEngine;
import org.onap.aai.setup.SchemaVersions;
import org.onap.aai.util.AAIConstants;

@MigrationPriority(20)
@MigrationDangerRating(2)
@Enabled
public class MigrateVlanTag extends EdgeSwingMigrator {

	protected static final String CLOUD_REGION_NODE_TYPE = "cloud-region";
	protected static final String CLOUD_OWNER = "cloud-owner";
	protected static final String CLOUD_REGION_ID = "cloud-region-id";
	protected static final String VLAN_RANGE = "vlan-range";
	protected static final String VLAN_RANGE_ID = "vlan-range-id";
	protected static final String VLAN_TAG = "vlan-tag";
	protected static final String VLAN_TAG_ID = "vlan-tag-id";
	protected static final String UPGRADE_CYCLE = "upgrade-cycle";

	protected final AtomicInteger skippedRowsCount = new AtomicInteger(0);
	protected final AtomicInteger processedRowsCount = new AtomicInteger(0);
	private static List<String> dmaapMsgList = new ArrayList<String>();
	private static List<Introspector> dmaapDeleteIntrospectorList = new ArrayList<Introspector>();
	private static int vlanRangeCount = 0;
	private static int vlanRangeFailureCount = 0;
	private static int vlanTagCount = 0;
	private static int vlanTagFailureCount = 0;
	
	private boolean success = true;
	protected int headerLength;

	protected final AtomicInteger falloutRowsCount = new AtomicInteger(0);

	public MigrateVlanTag(TransactionalGraphEngine engine, LoaderFactory loaderFactory, EdgeIngestor edgeIngestor,
			EdgeSerializer edgeSerializer, SchemaVersions schemaVersions) {
		super(engine, loaderFactory, edgeIngestor, edgeSerializer, schemaVersions);
	}

	@Override
	public void run() {
		logger.info("---------- Start Updating vlan-tags under cloud-region  ----------");

		String logDir = AAIConstants.AAI_HOME + AAIConstants.AAI_FILESEP + "logs";
		String feedDir = logDir +  AAIConstants.AAI_FILESEP + "data"
				+ AAIConstants.AAI_FILESEP + "migration-input-files" 
				+ AAIConstants.AAI_FILESEP;
		String fileName = feedDir + "dslResults.json";
		logger.info(fileName);
		logger.info("---------- Processing vlan-tags from file  ----------");

		Map cloudOwnerRegionIdWiseVertexMap = new HashMap();
		Map<String, Vertex> vlanTagIdWiseVertexMap = new HashMap<>();

		try {
			List<Vertex> cloudRegionList = this.engine.asAdmin().getTraversalSource().V()
					.has(AAIProperties.NODE_TYPE, CLOUD_REGION_NODE_TYPE).toList();

			List<Vertex> vlantagIdList = this.engine.asAdmin().getTraversalSource().V()
					.has(AAIProperties.NODE_TYPE, VLAN_TAG).toList();

			String cloudOwner, cloudRegionId;
			for (Vertex vertex : cloudRegionList) {
				cloudOwner = getCloudOwnerNodeValue(vertex);
				cloudRegionId = getCloudRegionIdNodeValue(vertex);
				if (!cloudOwner.isEmpty() && !cloudRegionId.isEmpty()) {
					cloudOwnerRegionIdWiseVertexMap.put(cloudOwner + "/" + cloudRegionId, vertex);
				}
			}

			String vlantagId;
			for (Vertex vertex : vlantagIdList) {
				vlantagId = getVlanTagIdNodeValue(vertex);
				if (!vlantagId.isEmpty() && 
						!(vertex.property(AAIProperties.AAI_URI).isPresent() &&
							vertex.property(AAIProperties.AAI_URI).toString().contains(CLOUD_REGION_NODE_TYPE))) {
					vlanTagIdWiseVertexMap.put(vlantagId, vertex);
				}
			}

			JSONParser jsonParser = new JSONParser();
			Object obj = jsonParser
					.parse(new FileReader(fileName));
			JSONObject vlanRangeResultJsonObj = (JSONObject) obj;
			JSONArray vlanRangeResultJsonArray = (JSONArray) vlanRangeResultJsonObj.get("results");

			JSONObject vlanRangeJsonObject, vlanTagsJsonObject, vlanTagJsonObject;
			JSONArray vlanTagJsonArray;
			String vlanTagId;
			boolean isFirstTime;
			Vertex vlanRangeVtx;
			Set<String> matchedVlanTagIdSet = new HashSet<String>();
			for (int i = 0; i < vlanRangeResultJsonArray.size(); i++) {
				isFirstTime = true;
				vlanRangeVtx = null;
				vlanRangeJsonObject = (JSONObject) vlanRangeResultJsonArray.get(i);
				cloudOwner = getCloudOwnerValueFromUrl((String) vlanRangeJsonObject.get("url"));
				cloudRegionId = getCloudRegionIdValueFromUrl((String) vlanRangeJsonObject.get("url"));
				if (cloudOwnerRegionIdWiseVertexMap.containsKey(cloudOwner + "/" + cloudRegionId)) {// ap1 key contains
																									// api2 key
					JSONObject vlanRangeInnerObject = (JSONObject) vlanRangeJsonObject.get("vlan-range");
					String vlanRangeId = (String) vlanRangeInnerObject.get("vlan-range-id");
					vlanTagsJsonObject = (JSONObject) vlanRangeInnerObject.get("vlan-tags");
					vlanTagJsonArray = (JSONArray) vlanTagsJsonObject.get("vlan-tag");

					for (int j = 0; j < vlanTagJsonArray.size(); j++) {
						vlanTagJsonObject = (JSONObject) vlanTagJsonArray.get(j);
						vlanTagId = (String) vlanTagJsonObject.get("vlan-tag-id");
						if (vlanTagIdWiseVertexMap.containsKey(vlanTagId)) {// ap1 key contains api2 key
							matchedVlanTagIdSet.add(vlanTagId);
							if (isFirstTime) {
								isFirstTime = false;
								vlanRangeVtx = createNewVlanRangeFromCloudRegion(vlanRangeInnerObject, cloudOwner,
										cloudRegionId, vlanRangeId);
							}
							Vertex vertex = vlanTagIdWiseVertexMap.get(vlanTagId);
							createNewVlanTagFromVlanRange(vlanTagJsonObject, vlanRangeVtx, cloudOwner, cloudRegionId,
									vlanRangeId, vertex);
							vlanTagIdWiseVertexMap.remove(vlanTagId);
						}
					}
				}
			}
			logger.info("******* Final Summary of Migrate vlan-tag  Migration *********");
			if(!vlanTagIdWiseVertexMap.isEmpty()) {
				logger.info("The following vlan-ids in A&AI graph were not found in the Narad input"
						+ "file and not migrated: ");			
				for(Vertex vlanTagIdWiseVertex : vlanTagIdWiseVertexMap.values()) {
					logger.info(vlanTagIdWiseVertex.property("vlan-tag-id").toString());
				}
			}
			logger.info(MIGRATION_SUMMARY_COUNT+"Total Vlan Ranges Updated Count:" + vlanRangeCount);
			logger.info(MIGRATION_ERROR+"Total Vlan Ranges Not Updated Count:" + vlanRangeFailureCount);
			logger.info(MIGRATION_SUMMARY_COUNT+"Total Vlan Tags Updated Sucessfully Count: " + vlanTagCount);
			logger.info(MIGRATION_ERROR+"Total Vlan Tags Not Updated Count: " + vlanTagFailureCount);
		} catch (FileNotFoundException e) {
			logger.info("ERROR: Could not file file " + fileName, e.getMessage());
			logger.error("ERROR: Could not file file " + fileName, e);
			success = false;
		} catch (IOException e) {
			logger.info("ERROR: Issue reading file " + fileName, e.getMessage());
			logger.error("ERROR: Issue reading file " + fileName, e);
			success = false;
		} catch (Exception e) {
			logger.info("encountered exception", e.getMessage());
			logger.error("encountered exception", e);
			e.printStackTrace();
			success = false;
		}
	}

	private Vertex createNewVlanRangeFromCloudRegion(JSONObject vlanRangeJsonObject, String cloudOwner,
			String cloudRegionId, String vlanRangeId) {

		Vertex vlanRangeVtx = null;
		try {

			GraphTraversal<Vertex, Vertex> cloudRegionVtxList = this.engine.asAdmin().getTraversalSource().V()
					.has(AAIProperties.NODE_TYPE, CLOUD_REGION_NODE_TYPE).has("cloud-owner", cloudOwner)
					.has("cloud-region-id", cloudRegionId);

			if (cloudRegionVtxList != null && cloudRegionVtxList.hasNext()) {
				Vertex cloudRegionVtx = cloudRegionVtxList.next();
				if (cloudRegionVtx != null) {
					Introspector vlanRange = loader.introspectorFromName("vlan-range");
					vlanRangeVtx = serializer.createNewVertex(vlanRange);
					vlanRange.setValue("vlan-range-id", vlanRangeId);// required
					vlanRange.setValue("vlan-id-lower", (Long) vlanRangeJsonObject.get("vlan-id-lower"));// required
					vlanRange.setValue("vlan-id-upper", (Long) vlanRangeJsonObject.get("vlan-id-upper"));// required
					vlanRange.setValue("vlan-type", (String) vlanRangeJsonObject.get("vlan-type"));// required
					this.createTreeEdge(cloudRegionVtx, vlanRangeVtx);
					vlanRangeVtx.property(AAIProperties.AAI_URI,
							"/cloud-infrastructure/" + "cloud-regions/cloud-region/" + cloudOwner + "/" + cloudRegionId
									+ "/vlan-ranges/vlan-range/" + vlanRangeId);
					serializer.serializeSingleVertex(vlanRangeVtx, vlanRange, "migrations");

					logger.info("\t Created new vlan-range " + vlanRangeVtx + " with vlan-range-id = "
							+ (String) vlanRangeJsonObject.get("vlan-range-id"));

					String dmaapMsg = System.nanoTime() + "_" + vlanRangeVtx.id().toString() + "_"
							+ vlanRangeVtx.value("resource-version").toString();
					dmaapMsgList.add(dmaapMsg);
					vlanRangeCount++;
				}
			}

		} catch (Exception e) {
			vlanRangeFailureCount++;
			logger.error("vlan-range failure: ", e);
			logger.info("vlan-range with id : " + vlanRangeId + " failed: ", e.getMessage());
			
		}
		return vlanRangeVtx;

	}

	private Vertex createNewVlanTagFromVlanRange(JSONObject vlanTagJsonObject, Vertex vlanRangeVtx, String cloudOwner,
			String cloudRegionId, String vlanRangeId, Vertex oldVlanTagVtx) {

		Vertex vlanTagVtx = null;
		try {
			Introspector vlanTag = loader.introspectorFromName("vlan-tag");
			vlanTagVtx = serializer.createNewVertex(vlanTag);
			String vlanTagId = (String) vlanTagJsonObject.get("vlan-tag-id");
			vlanTag.setValue("vlan-tag-id", vlanTagId);// required
			vlanTag.setValue("vlan-tag-role", (String) vlanTagJsonObject.get("vlan-tag-role"));// required
			vlanTag.setValue("is-private", (Boolean) vlanTagJsonObject.get("is-private"));// required
			if (vlanTagJsonObject.containsKey("vlan-id-inner"))
				vlanTag.setValue("vlan-id-inner", (Long) vlanTagJsonObject.get("vlan-id-inner"));
			if (vlanTagJsonObject.containsKey("vlan-id-outer"))
				vlanTag.setValue("vlan-id-outer", (Long) vlanTagJsonObject.get("vlan-id-outer"));
			if (vlanTagJsonObject.containsKey("vlan-tag-function"))
				vlanTag.setValue("vlan-tag-function", (String) vlanTagJsonObject.get("vlan-tag-function"));
			if (vlanTagJsonObject.containsKey("config-phase"))
				vlanTag.setValue("config-phase", (String) vlanTagJsonObject.get("config-phase"));
			if (vlanTagJsonObject.containsKey("vlan-tag-type"))
				vlanTag.setValue("vlan-tag-type", (String) vlanTagJsonObject.get("vlan-tag-type"));
			this.createTreeEdge(vlanRangeVtx, vlanTagVtx);
			vlanTagVtx.property(AAIProperties.AAI_URI,
					"/cloud-infrastructure/" + "cloud-regions/cloud-region/" + cloudOwner + "/" + cloudRegionId
							+ "/vlan-ranges/vlan-range/" + vlanRangeId + "/vlan-tags/vlan-tag/" + vlanTagId);
			executeModifyOperation(oldVlanTagVtx, vlanTagVtx);
			Introspector deletedVlanEvent = serializer.getLatestVersionView(oldVlanTagVtx);
			dmaapDeleteIntrospectorList.add(deletedVlanEvent);
			oldVlanTagVtx.remove();
			serializer.serializeSingleVertex(vlanTagVtx, vlanTag, "migrations");

			logger.info("\t Created new vlan-tag " + vlanTagVtx + " with vlan-tag-id = "
					+ (String) vlanTagJsonObject.get("vlan-tag-id"));

			String dmaapMsg = System.nanoTime() + "_" + vlanTagVtx.id().toString() + "_"
					+ vlanRangeVtx.value("resource-version").toString();
			dmaapMsgList.add(dmaapMsg);
			vlanTagCount++;
		} catch (Exception e) {
			vlanTagFailureCount++;
			logger.error("vlan-tag failure: ", e);
			if(vlanTagJsonObject != null && vlanTagJsonObject.get("vlan-tag-id") != null){
				logger.info("vlan-tag with id : " + vlanTagJsonObject.get("vlan-tag-id") + " failed: ", e.getMessage());
			}
			else {
				logger.info("vlan-tag failure: ", e.getMessage());
			}
		}
		return vlanRangeVtx;

	}

	private String getCloudRegionIdNodeValue(Vertex vertex) {
		String propertyValue = "";
		if (vertex != null && vertex.property(CLOUD_REGION_ID).isPresent()) {
			propertyValue = vertex.property(CLOUD_REGION_ID).value().toString();
		}
		return propertyValue;
	}

	private String getCloudOwnerNodeValue(Vertex vertex) {
		String propertyValue = "";
		if (vertex != null && vertex.property(CLOUD_OWNER).isPresent()) {
			propertyValue = vertex.property(CLOUD_OWNER).value().toString();
		}
		return propertyValue;
	}

	private String getVlanTagIdNodeValue(Vertex vertex) {
		String propertyValue = "";
		if (vertex != null && vertex.property(VLAN_TAG_ID).isPresent()) {
			propertyValue = vertex.property(VLAN_TAG_ID).value().toString();
		}
		return propertyValue;
	}

	private String getCloudOwnerValueFromUrl(String url) {
		String arr[] = url.split("/");
		return arr[6];
	}

	private String getCloudRegionIdValueFromUrl(String url) {
		String arr[] = url.split("/");
		return arr[7];
	}

	protected void executeModifyOperation(Vertex fromNode, Vertex toNode) {
		try {
			
			this.swingEdges(fromNode, toNode, "l3-network", "none", "BOTH");
			this.swingEdges(fromNode, toNode, "cp", "none", "BOTH");
		} catch (Exception e) {
			logger.error("error encountered", e);
			success = false;
		}
	}
	
	@Override
	public void commit() {
        engine.commit();
        createDmaapFiles(dmaapMsgList);
        createDmaapFilesForDelete(dmaapDeleteIntrospectorList);
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
		return Optional.of(new String[] { CLOUD_REGION_NODE_TYPE });
	}

	@Override
	public String getMigrationName() {
		return "MigrateVlanTag";
	}

	@Override
	public List<Pair<Vertex, Vertex>> getAffectedNodePairs() {
		return null;
	}

	@Override
	public String getNodeTypeRestriction() {
		return null;
	}

	@Override
	public String getEdgeLabelRestriction() {
		return null;
	}

	@Override
	public String getEdgeDirRestriction() {
		return null;
	}

	@Override
	public void cleanupAsAppropriate(List<Pair<Vertex, Vertex>> nodePairL) {
	}

}