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
import org.janusgraph.core.EdgeLabel;
import org.janusgraph.core.JanusGraph;
import org.janusgraph.core.PropertyKey;
import org.janusgraph.core.schema.JanusGraphIndex;
import org.janusgraph.core.schema.JanusGraphManagement;

import java.util.Iterator;
import java.util.LinkedHashSet;

public class AuditJanusGraph extends Auditor {

	private final JanusGraph graph;
	
	/**
	 * Instantiates a new audit JanusGraph.
	 *
	 * @param g the g
	 */
	public AuditJanusGraph (JanusGraph g) {
		this.graph = g;
		buildSchema();
	}
	
	/**
	 * Builds the schema.
	 */
	private void buildSchema() {
		populateProperties();
		populateIndexes();
		populateEdgeLabels();
	}
	
	/**
	 * Populate properties.
	 */
	private void populateProperties() {
		JanusGraphManagement mgmt = graph.openManagement();
		Iterable<PropertyKey> iterable = mgmt.getRelationTypes(PropertyKey.class);
		Iterator<PropertyKey> JanusGraphProperties = iterable.iterator();
		PropertyKey propKey;
		while (JanusGraphProperties.hasNext()) {
			propKey = JanusGraphProperties.next();
			DBProperty prop = new DBProperty();

			prop.setName(propKey.name());
			prop.setCardinality(propKey.cardinality());
			prop.setTypeClass(propKey.dataType());

			this.properties.put(prop.getName(), prop);
		}
	}

	/**
	 * Populate indexes.
	 */
	private void populateIndexes() {
		JanusGraphManagement mgmt = graph.openManagement();
		Iterable<JanusGraphIndex> iterable = mgmt.getGraphIndexes(Vertex.class);
		Iterator<JanusGraphIndex> JanusGraphIndexes = iterable.iterator();
		JanusGraphIndex JanusGraphIndex;
		while (JanusGraphIndexes.hasNext()) {
			JanusGraphIndex = JanusGraphIndexes.next();
			if (JanusGraphIndex.isCompositeIndex()) {
				DBIndex index = new DBIndex();
				LinkedHashSet<DBProperty> dbProperties = new LinkedHashSet<>();
				index.setName(JanusGraphIndex.name());
				index.setUnique(JanusGraphIndex.isUnique());
				PropertyKey[] keys = JanusGraphIndex.getFieldKeys();
				for (PropertyKey key : keys) {
					dbProperties.add(this.properties.get(key.name()));
				}
				index.setProperties(dbProperties);
				index.setStatus(JanusGraphIndex.getIndexStatus(keys[0]));
				this.indexes.put(index.getName(), index);
			}
		}
	}

	/**
	 * Populate edge labels.
	 */
	private void populateEdgeLabels() {
		JanusGraphManagement mgmt = graph.openManagement();
		Iterable<EdgeLabel> iterable = mgmt.getRelationTypes(EdgeLabel.class);
		Iterator<EdgeLabel> JanusGraphEdgeLabels = iterable.iterator();
		EdgeLabel edgeLabel;
		while (JanusGraphEdgeLabels.hasNext()) {
			edgeLabel = JanusGraphEdgeLabels.next();
			EdgeProperty edgeProperty = new EdgeProperty();
			
			edgeProperty.setName(edgeLabel.name());
			edgeProperty.setMultiplicity(edgeLabel.multiplicity());
			
			this.edgeLabels.put(edgeProperty.getName(), edgeProperty);
		}	
	}
	
}
