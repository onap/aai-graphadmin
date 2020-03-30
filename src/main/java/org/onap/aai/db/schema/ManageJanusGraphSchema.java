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
package org.onap.aai.db.schema;

import org.apache.tinkerpop.gremlin.structure.Vertex;
import org.janusgraph.core.JanusGraph;
import org.janusgraph.core.PropertyKey;
import org.janusgraph.core.schema.JanusGraphManagement;
import org.janusgraph.core.schema.JanusGraphManagement.IndexBuilder;
import org.onap.aai.edges.EdgeIngestor;
import org.onap.aai.setup.SchemaVersions;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.*;

public class ManageJanusGraphSchema {

	protected Logger logger = LoggerFactory.getLogger(ManageJanusGraphSchema.class.getSimpleName());

	private JanusGraphManagement graphMgmt;
	private JanusGraph graph;
	private List<DBProperty> aaiProperties;
	private List<DBIndex> aaiIndexes;
	private List<EdgeProperty> aaiEdgeProperties;
	private Auditor oxmInfo = null;
	private Auditor graphInfo = null;


	/**
	 * Instantiates a new manage JanusGraph schema.
	 *
	 * @param graph the graph
	 */
	public ManageJanusGraphSchema(final JanusGraph graph, AuditorFactory auditorFactory, SchemaVersions schemaVersions, EdgeIngestor edgeIngestor) {
		this.graph = graph;
		oxmInfo = auditorFactory.getOXMAuditor(schemaVersions.getDefaultVersion(), edgeIngestor);
		graphInfo = auditorFactory.getGraphAuditor(graph);
	}


	/**
	 * Builds the schema.
	 */
	public void buildSchema() {

		this.graphMgmt = graph.openManagement();
		aaiProperties = new ArrayList<>();
		aaiEdgeProperties = new ArrayList<>();
		aaiIndexes = new ArrayList<>();
		aaiProperties.addAll(oxmInfo.getAuditDoc().getProperties());
		aaiIndexes.addAll(oxmInfo.getAuditDoc().getIndexes());
		aaiEdgeProperties.addAll(oxmInfo.getAuditDoc().getEdgeLabels());
		try {
			for (DBProperty prop : aaiProperties) {
				createProperty(graphMgmt, prop);
			}
			createIndexes();
			createEdgeLabels();
		} catch (Exception e) {
			logger.info("exception during schema build, executing rollback", e);
			graphMgmt.rollback();
		}
		graphMgmt.commit();
	}

	/**
	 * Creates the indexes.
	 */
	private void createIndexes() {
		final String IS_NEW = "isNew";
		final String IS_CHANGED = "isChanged";

		for (DBIndex index : aaiIndexes) {
			Set<DBProperty> props = index.getProperties();
			List<PropertyKey> keyList = new ArrayList<>();
			for (DBProperty prop : props) {
				keyList.add(graphMgmt.getPropertyKey(prop.getName()));
			}
			Map<String, Boolean> isNewIsChanged = isIndexNewOrChanged(index, keyList, IS_NEW, IS_CHANGED);
			if (!keyList.isEmpty()) {
				this.createIndex(graphMgmt, index.getName(), keyList, index.isUnique(), isNewIsChanged.get("isNew"), isNewIsChanged.get("isChanged"));
			}
		}
	}

	private Map<String, Boolean> isIndexNewOrChanged(DBIndex index, List<PropertyKey> keyList, final String IS_NEW, final String IS_CHANGED) {
		Map<String, Boolean> result = new HashMap<>();
		result.put(IS_NEW, false);
		result.put(IS_CHANGED, false);
		if (graphMgmt.containsGraphIndex(index.getName())) {
			PropertyKey[] dbKeys = graphMgmt.getGraphIndex(index.getName()).getFieldKeys();
			if (dbKeys.length != keyList.size()) {
				result.put(IS_CHANGED, true);
			} else {
				int i = 0;
				for (PropertyKey key : keyList) {
					if (!dbKeys[i].equals(key)) {
						result.put(IS_CHANGED, true);
						break;
					}
					i++;
				}
			}
		} else {
			result.put(IS_NEW, true);
		}
		return result;
	}

	// Use EdgeRules to make sure edgeLabels are defined in the db.  NOTE: the multiplicty used here is
	// always "MULTI".  This is not the same as our internal "Many2Many", "One2One", "One2Many" or "Many2One"
	// We use the same edge-label for edges between many different types of nodes and our internal
	// multiplicty definitions depends on which two types of nodes are being connected.
	/**
	 * Creates the edge labels.
	 */
	private void createEdgeLabels() {


		for (EdgeProperty prop : aaiEdgeProperties) {

			if (graphMgmt.containsEdgeLabel(prop.getName())) {
				// see what changed
			} else {
				graphMgmt.makeEdgeLabel(prop.getName()).multiplicity(prop.getMultiplicity()).make();
			}

		}


	}

	/**
	 * Creates the property.
	 *
	 * @param mgmt the mgmt
	 * @param prop the prop
	 */
	private void createProperty(JanusGraphManagement mgmt, DBProperty prop) {
		if (mgmt.containsPropertyKey(prop.getName())) {
			PropertyKey key = mgmt.getPropertyKey(prop.getName());
			boolean isChanged = false;
			if (!prop.getCardinality().equals(key.cardinality())) {
				isChanged = true;
			}
			if (!prop.getTypeClass().equals(key.dataType())) {
				isChanged = true;
			}
			if (isChanged) {
				//must modify!
				this.replaceProperty();
			}
		} else {
			//create a new property key
			System.out.println("Key: " + prop.getName() + " not found - adding");
			mgmt.makePropertyKey(prop.getName()).dataType(prop.getTypeClass()).cardinality(prop.getCardinality()).make();
		}
	}

	/**
	 * Replace property.
	 */
	private void replaceProperty() {
		//must modify!
	}

	/**
	 * Creates the index.
	 *
	 * @param mgmt the mgmt
	 * @param indexName the index name
	 * @param keys the keys
	 * @param isUnique the is unique
	 * @param isNew the is new
	 * @param isChanged the is changed
	 */
	private void createIndex(JanusGraphManagement mgmt, String indexName, List<PropertyKey> keys, boolean isUnique, boolean isNew, boolean isChanged) {

		if (isNew) {
			IndexBuilder builder = mgmt.buildIndex(indexName,Vertex.class);
			for (PropertyKey k : keys) {
				builder.addKey(k);
			}
			if (isUnique) {
				builder.unique();
			}
			builder.buildCompositeIndex();
			System.out.println("Built index for " + indexName + " with keys: " + keys);
		}
		if (isChanged) {
		   	//System.out.println("Changing index: " + indexName);
		   	//JanusGraphIndex oldIndex = mgmt.getGraphIndex(indexName);
		   	//mgmt.updateIndex(oldIndex, SchemaAction.DISABLE_INDEX);
		   	//mgmt.commit();
		   	//cannot remove indexes
		   	//graphMgmt.updateIndex(oldIndex, SchemaAction.REMOVE_INDEX);

        }

	}

	/**
	 * Update index.
	 *
	 * @param index the index
	 */
	public void updateIndex(DBIndex index) {

		JanusGraphManagement mgmt = graph.openManagement();
		List<PropertyKey> keys = new ArrayList<>();
		boolean isNew = false;
		boolean isChanged = false;
		for (DBProperty prop : index.getProperties()) {
			createProperty(mgmt, prop);
			keys.add(mgmt.getPropertyKey(prop.getName()));
		}
		if (mgmt.containsGraphIndex(index.getName())) {
			System.out.println("index already exists");
			isChanged = true;
		} else {
			isNew = true;
		}
		this.createIndex(mgmt, index.getName(), keys, index.isUnique(), isNew, isChanged);

		mgmt.commit();
		
	}
	
	
	
	
	
}
