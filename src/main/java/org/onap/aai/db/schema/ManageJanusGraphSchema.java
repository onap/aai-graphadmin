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
import org.janusgraph.core.schema.JanusGraphIndex;
import org.janusgraph.core.schema.JanusGraphManagement;
import org.janusgraph.core.schema.JanusGraphManagement.IndexBuilder;
import org.janusgraph.core.schema.SchemaStatus;
import org.onap.aai.setup.SchemaVersions;
import org.onap.aai.setup.SchemaVersion;

import java.util.ArrayList;
import java.util.List;
import java.util.Set;

public class ManageJanusGraphSchema {

	
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
	public ManageJanusGraphSchema(final JanusGraph graph, AuditorFactory auditorFactory, SchemaVersions schemaVersions) {
		this.graph = graph;
		oxmInfo = auditorFactory.getOXMAuditor(schemaVersions.getDefaultVersion());
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
			createPropertyKeys();
			createIndexes();
			createEdgeLabels();
		} catch (Exception e) {
			e.printStackTrace();
			graphMgmt.rollback();
		}
		graphMgmt.commit();
	}

	/**
	 * Creates the property keys.
	 */
	private void createPropertyKeys() {


		for (DBProperty prop : aaiProperties) {

			if (graphMgmt.containsPropertyKey(prop.getName())) {
				PropertyKey key = graphMgmt.getPropertyKey(prop.getName());
				boolean isChanged = false;
				if (!prop.getCardinality().equals(key.cardinality())) {
					isChanged = true;
				}
				if (!prop.getTypeClass().equals(key.dataType())) {
					isChanged = true;
				}
				if (isChanged) {
					//must modify!
					this.replaceProperty(prop);
				}
			} else {
				//create a new property key
				System.out.println("Key: " + prop.getName() + " not found - adding");
				graphMgmt.makePropertyKey(prop.getName()).dataType(prop.getTypeClass()).cardinality(prop.getCardinality()).make();
			}
		}

	}

	/**
	 * Creates the indexes.
	 */
	private void createIndexes() {

		for (DBIndex index : aaiIndexes) {
			Set<DBProperty> props = index.getProperties();
			boolean isChanged = false;
			boolean isNew = false;
			List<PropertyKey> keyList = new ArrayList<>();
			for (DBProperty prop : props) {
				keyList.add(graphMgmt.getPropertyKey(prop.getName()));
			}
			if (graphMgmt.containsGraphIndex(index.getName())) {
				JanusGraphIndex JanusGraphIndex = graphMgmt.getGraphIndex(index.getName());
				PropertyKey[] dbKeys = JanusGraphIndex.getFieldKeys();
				if (dbKeys.length != keyList.size()) {
					isChanged = true;
				} else {
					int i = 0;
					for (PropertyKey key : keyList) {
						if (!dbKeys[i].equals(key)) {
							isChanged = true;
							break;
						}
						i++;
					}
				}
			} else {
				isNew = true;
			}
			if (keyList.size() > 0) {
				this.createIndex(graphMgmt, index.getName(), keyList, index.isUnique(), isNew, isChanged);
			}
		}
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
				this.replaceProperty(prop);
			}
		} else {
			//create a new property key
			System.out.println("Key: " + prop.getName() + " not found - adding");
			mgmt.makePropertyKey(prop.getName()).dataType(prop.getTypeClass()).cardinality(prop.getCardinality()).make();
		}
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

		/*if (isChanged) {
			System.out.println("Changing index: " + indexName);
			JanusGraphIndex oldIndex = mgmt.getGraphIndex(indexName);
			mgmt.updateIndex(oldIndex, SchemaAction.DISABLE_INDEX);
			mgmt.commit();
			//cannot remove indexes
			//graphMgmt.updateIndex(oldIndex, SchemaAction.REMOVE_INDEX);
		}*/
		if (isNew || isChanged) {

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

				//mgmt.commit();
			}

			//mgmt = graph.asAdmin().getManagementSystem();
			//mgmt.updateIndex(mgmt.getGraphIndex(indexName), SchemaAction.REGISTER_INDEX);
			//mgmt.commit();

			try {
				//waitForCompletion(indexName);
				//JanusGraphIndexRepair.hbaseRepair(AAIConstants.AAI_CONFIG_FILENAME, indexName, "");
			} catch (Exception e) {
				// TODO Auto-generated catch block
				graph.tx().rollback();
				graph.close();
				e.printStackTrace();
			}

			//mgmt = graph.asAdmin().getManagementSystem();
			//mgmt.updateIndex(mgmt.getGraphIndex(indexName), SchemaAction.REINDEX);

			//mgmt.updateIndex(mgmt.getGraphIndex(indexName), SchemaAction.ENABLE_INDEX);

			//mgmt.commit();

		}
	}

	/**
	 * Wait for completion.
	 *
	 * @param name the name
	 * @throws InterruptedException the interrupted exception
	 */
	private void waitForCompletion(String name) throws InterruptedException {

		boolean registered = false;
		long before = System.currentTimeMillis();
		while (!registered) {
		    Thread.sleep(500L);
		    JanusGraphManagement mgmt = graph.openManagement();
		    JanusGraphIndex idx  = mgmt.getGraphIndex(name);
		    registered = true;
		    for (PropertyKey k : idx.getFieldKeys()) {
		        SchemaStatus s = idx.getIndexStatus(k);
		        registered &= s.equals(SchemaStatus.REGISTERED);
		    }
		    mgmt.rollback();
		}
		System.out.println("Index REGISTERED in " + (System.currentTimeMillis() - before) + " ms");
	}

	/**
	 * Replace property.
	 *
	 * @param key the key
	 */
	private void replaceProperty(DBProperty key) {
		
		
		
		
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
			isNew = false;
			isChanged = true;
		} else {
			isNew = true;
			isChanged = false;
		}
		this.createIndex(mgmt, index.getName(), keys, index.isUnique(), isNew, isChanged);

		mgmt.commit();
		
	}
	
	
	
	
	
}
