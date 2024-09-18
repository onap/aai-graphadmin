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

import org.janusgraph.core.Multiplicity;

import com.fasterxml.jackson.annotation.JsonProperty;
import com.fasterxml.jackson.annotation.JsonPropertyOrder;

@JsonPropertyOrder({ "label", "multiplicity" })
public class EdgeProperty implements Named {

	private String name = null;
	private Multiplicity multiplicity = null;

    /**
     * Gets the name
     */
  @JsonProperty("label")
	public String getName() {
		return name;
	}

	/**
	 * Sets the name.
	 *
	 * @param name the new name
	 */
  @JsonProperty("label")
	public void setName(String name) {
		this.name = name;
	}

	/**
	 * Gets the multiplicity.
	 *
	 * @return the multiplicity
	 */
	public Multiplicity getMultiplicity() {
		return multiplicity;
	}

	/**
	 * Sets the multiplicity.
	 *
	 * @param multiplicity the new multiplicity
	 */
	public void setMultiplicity(Multiplicity multiplicity) {
		this.multiplicity = multiplicity;
	}

}
