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
package org.onap.aai.interceptors.pre;

import org.onap.aai.interceptors.AAIContainerFilter;
import org.onap.aai.interceptors.AAIHeaderProperties;

import jakarta.annotation.Priority;
import jakarta.ws.rs.container.ContainerRequestContext;
import jakarta.ws.rs.container.ContainerRequestFilter;
import jakarta.ws.rs.container.PreMatching;
import jakarta.ws.rs.core.MultivaluedMap;
import jakarta.ws.rs.ext.Provider;
import java.util.Collections;
import java.util.regex.Matcher;

@Provider
@PreMatching
@Priority(AAIRequestFilterPriority.HEADER_MANIPULATION)
public class RequestHeaderManipulation extends AAIContainerFilter implements ContainerRequestFilter {

	@Override
	public void filter(ContainerRequestContext requestContext) {

		String uri = requestContext.getUriInfo().getPath();
		this.addRequestContext(uri, requestContext.getHeaders());

	}
	
	private void addRequestContext(String uri, MultivaluedMap<String, String> requestHeaders) {

		String rc = "";

        Matcher match = VersionInterceptor.EXTRACT_VERSION_PATTERN.matcher(uri);
        if (match.find()) {
            rc = match.group(1);
        }

		if (requestHeaders.containsKey(AAIHeaderProperties.REQUEST_CONTEXT)) {
			requestHeaders.remove(AAIHeaderProperties.REQUEST_CONTEXT);
		}
		requestHeaders.put(AAIHeaderProperties.REQUEST_CONTEXT, Collections.singletonList(rc));
	}

}
