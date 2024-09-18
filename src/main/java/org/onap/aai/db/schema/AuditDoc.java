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

import java.util.List;

import com.fasterxml.jackson.annotation.JsonProperty;

public class AuditDoc {

	private List<DBProperty> properties;
	private List<DBIndex> indexes;
	private List<EdgeProperty> edgeLabels;

	/**
	 * Gets the properties.
	 *
	 * @return the properties
	 */
	public List<DBProperty> getProperties() {
		return properties;
	}

	/**
	 * Sets the properties.
	 *
	 * @param properties the new properties
	 */
	public void setProperties(List<DBProperty> properties) {
		this.properties = properties;
	}

	/**
	 * Gets the indexes.
	 *
	 * @return the indexes
	 */
	public List<DBIndex> getIndexes() {
		return indexes;
	}

	/**
	 * Sets the indexes.
	 *
	 * @param indexes the new indexes
	 */
	public void setIndexes(List<DBIndex> indexes) {
		this.indexes = indexes;
	}

	/**
	 * Gets the edge labels.
	 *
	 * @return the edge labels
	 */
	@JsonProperty("edge-labels")
	public List<EdgeProperty> getEdgeLabels() {
		return edgeLabels;
	}

	/**
	 * Sets the edge labels.
	 *
	 * @param edgeLabels the new edge labels
	 */
	public void setEdgeLabels(List<EdgeProperty> edgeLabels) {
		this.edgeLabels = edgeLabels;
	}
}
