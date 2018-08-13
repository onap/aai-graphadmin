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

import java.util.*;

public abstract class Auditor {

	protected Map<String, DBProperty> properties = new HashMap<>();
	protected Map<String, DBIndex> indexes = new HashMap<>();
	protected Map<String, EdgeProperty> edgeLabels = new HashMap<>();
	
	/**
	 * Gets the audit doc.
	 *
	 * @return the audit doc
	 */
	public AuditDoc getAuditDoc() {
		AuditDoc doc = new AuditDoc();
		List<DBProperty> propertyList = new ArrayList<>();
		List<DBIndex> indexList = new ArrayList<>();
		List<EdgeProperty> edgeLabelList = new ArrayList<>();
		propertyList.addAll(this.properties.values());
		indexList.addAll(this.indexes.values());
		edgeLabelList.addAll(this.edgeLabels.values());
		Collections.sort(propertyList, new CompareByName());
		Collections.sort(indexList, new CompareByName());
		Collections.sort(edgeLabelList, new CompareByName());
		
		doc.setProperties(propertyList);
		doc.setIndexes(indexList);
		doc.setEdgeLabels(edgeLabelList);
		
		return doc;
	}
}
