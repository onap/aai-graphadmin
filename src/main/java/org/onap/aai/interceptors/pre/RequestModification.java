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

import javax.annotation.Priority;
import javax.ws.rs.container.ContainerRequestContext;
import javax.ws.rs.container.ContainerRequestFilter;
import javax.ws.rs.container.PreMatching;
import javax.ws.rs.core.MultivaluedMap;
import javax.ws.rs.core.UriBuilder;
import javax.ws.rs.ext.Provider;
import java.io.IOException;
import java.util.Arrays;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.stream.Collectors;

@Provider
@PreMatching
@Priority(AAIRequestFilterPriority.REQUEST_MODIFICATION)
public class RequestModification extends AAIContainerFilter implements ContainerRequestFilter {

	@Override
	public void filter(ContainerRequestContext requestContext) throws IOException {

		this.cleanDME2QueryParams(requestContext);

	}
	
	private void cleanDME2QueryParams(ContainerRequestContext request) {
		UriBuilder builder = request.getUriInfo().getRequestUriBuilder();
		MultivaluedMap<String, String> queries = request.getUriInfo().getQueryParameters();

		String[] blacklist = { "version", "envContext", "routeOffer" };
		Set<String> blacklistSet = Arrays.stream(blacklist).collect(Collectors.toSet());

		boolean remove = true;

		for (String param : blacklistSet) {
			if (!queries.containsKey(param)) {
				remove = false;
				break;
			}
		}

		if (remove) {
			for (Map.Entry<String, List<String>> query : queries.entrySet()) {
				String key = query.getKey();
				if (blacklistSet.contains(key)) {
					builder.replaceQueryParam(key);
				}
			}
		}
		request.setRequestUri(builder.build());
	}

}
