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
package org.onap.aai.util;

import java.util.Iterator;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class GraphAdminDBUtils {

	private static Logger logger = LoggerFactory.getLogger(GraphAdminDBUtils.class);

	private GraphAdminDBUtils() {

	}

	public static void logConfigs(org.apache.commons.configuration.Configuration configuration) {

		if (configuration != null && configuration.getKeys() != null) {
			Iterator<String> keys = configuration.getKeys();
			keys.forEachRemaining(
					key -> logger.info("Key is " + key + "Value is  " + configuration.getProperty(key).toString()));
		}

	}
}
