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

import org.janusgraph.core.schema.SchemaStatus;

import java.util.LinkedHashSet;
import java.util.Set;

public class DBIndex implements Named {

	private String name = null;
	private boolean unique = false;
	private LinkedHashSet<DBProperty> properties = new LinkedHashSet<>();
	private SchemaStatus status = null;

	/**
	 * Gets the name
	 */
	public String getName() {
		return name;
	}

	/**
	 * Sets the name.
	 *
	 * @param name the new name
	 */
	public void setName(String name) {
		this.name = name;
	}

	/**
	 * Checks if is unique.
	 *
	 * @return true, if is unique
	 */
	public boolean isUnique() {
		return unique;
	}

	/**
	 * Sets the unique.
	 *
	 * @param unique the new unique
	 */
	public void setUnique(boolean unique) {
		this.unique = unique;
	}

	/**
	 * Gets the properties.
	 *
	 * @return the properties
	 */
	public Set<DBProperty> getProperties() {
		return properties;
	}
	
	/**
	 * Sets the properties.
	 *
	 * @param properties the new properties
	 */
	public void setProperties(LinkedHashSet<DBProperty> properties) {
		this.properties = properties;
	}
	
	/**
	 * Gets the status.
	 *
	 * @return the status
	 */
	public SchemaStatus getStatus() {
		return status;
	}
	
	/**
	 * Sets the status.
	 *
	 * @param status the new status
	 */
	public void setStatus(SchemaStatus status) {
		this.status = status;
	}
	
}
