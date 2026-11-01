/**
 * ============LICENSE_START=======================================================
 * org.onap.aai
 * ================================================================================
 * Copyright © 2017-2018 AT&T Intellectual Property. All rights reserved.
 * Modifications Copyright (C) 2026 Deutsche Telekom AG.
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
package org.onap.aai.interceptors;

import org.onap.aai.util.FormatDate;

import java.util.LinkedHashMap;
import java.util.List;
import java.util.Locale;
import java.util.Map;
import java.util.Set;
import java.util.UUID;

import jakarta.ws.rs.core.MultivaluedMap;

public abstract class  AAIContainerFilter {

	private static final Set<String> SENSITIVE_HEADERS =
			Set.of("authorization", "proxy-authorization", "cookie", "set-cookie");
	private static final List<String> MASKED_VALUE = List.of("***");
    
	protected String genDate() {
		FormatDate fd = new FormatDate("YYMMdd-HH:mm:ss:SSS");
		return fd.getDateTime();
	}
	
	protected boolean isValidUUID(String transId) {
		try {
			UUID.fromString(transId);
		} catch (IllegalArgumentException e) {
			return false;
		}
		return true;
	}

	protected String headersForLog(MultivaluedMap<String, ?> headers) {
		Map<String, Object> loggable = new LinkedHashMap<>();
		headers.forEach((name, values) -> loggable.put(name,
				SENSITIVE_HEADERS.contains(name.toLowerCase(Locale.ROOT)) ? MASKED_VALUE : values));
		return loggable.toString();
	}
}
