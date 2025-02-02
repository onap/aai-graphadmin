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
package org.onap.aai.interceptors.post;

import org.onap.aai.interceptors.AAIContainerFilter;
import org.onap.aai.interceptors.AAIHeaderProperties;

import jakarta.annotation.Priority;
import jakarta.ws.rs.container.ContainerRequestContext;
import jakarta.ws.rs.container.ContainerResponseContext;
import jakarta.ws.rs.container.ContainerResponseFilter;
import jakarta.ws.rs.core.MediaType;
import java.io.IOException;

@Priority(AAIResponseFilterPriority.HEADER_MANIPULATION)
public class ResponseHeaderManipulation extends AAIContainerFilter implements ContainerResponseFilter {

	private static final String DEFAULT_XML_TYPE = MediaType.APPLICATION_XML;

	@Override
	public void filter(ContainerRequestContext requestContext, ContainerResponseContext responseContext)
			throws IOException {

		updateResponseHeaders(requestContext, responseContext);

	}

	private void updateResponseHeaders(ContainerRequestContext requestContext,
			ContainerResponseContext responseContext) {

		responseContext.getHeaders().add(AAIHeaderProperties.AAI_TX_ID, requestContext.getProperty(AAIHeaderProperties.AAI_TX_ID));

		String responseContentType = responseContext.getHeaderString("Content-Type");

		if(responseContentType == null){
			String acceptType = requestContext.getHeaderString("Accept");

			if(acceptType == null || "*/*".equals(acceptType)){
				responseContext.getHeaders().putSingle("Content-Type", DEFAULT_XML_TYPE);
			} else {
				responseContext.getHeaders().putSingle("Content-Type", acceptType);
			}
		}

	}

}
