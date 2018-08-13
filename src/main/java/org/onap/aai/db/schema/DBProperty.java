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

import org.janusgraph.core.Cardinality;

public class DBProperty implements Named {

	
	private String name = null;
	private Cardinality cardinality = null;
	private Class<?> typeClass = null;
	
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
	 * Gets the cardinality.
	 *
	 * @return the cardinality
	 */
	public Cardinality getCardinality() {
		return cardinality;
	}
	
	/**
	 * Sets the cardinality.
	 *
	 * @param cardinality the new cardinality
	 */
	public void setCardinality(Cardinality cardinality) {
		this.cardinality = cardinality;
	}
	
	/**
	 * Gets the type class.
	 *
	 * @return the type class
	 */
	public Class<?> getTypeClass() {
		return typeClass;
	}
	
	/**
	 * Sets the type class.
	 *
	 * @param type the new type class
	 */
	public void setTypeClass(Class<?> type) {
		this.typeClass = type;
	}
	
}
