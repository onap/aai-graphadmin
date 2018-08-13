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
import org.onap.aai.logging.LoggingContext;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.core.env.Environment;

import javax.annotation.Priority;
import javax.servlet.http.HttpServletRequest;
import javax.ws.rs.container.ContainerRequestContext;
import javax.ws.rs.container.ContainerRequestFilter;
import javax.ws.rs.container.PreMatching;
import javax.ws.rs.core.MultivaluedMap;
import javax.ws.rs.ext.Provider;
import java.io.IOException;

@Provider
@PreMatching
@Priority(AAIRequestFilterPriority.SET_LOGGING_CONTEXT)
public class SetLoggingContext extends AAIContainerFilter implements ContainerRequestFilter {

	@Autowired
	private Environment environment;

	@Autowired
	private HttpServletRequest httpServletRequest;
	
	@Override
	public void filter(ContainerRequestContext requestContext) throws IOException {

		String uri = httpServletRequest.getRequestURI();
		String queryString = httpServletRequest.getQueryString();

		if(queryString != null && !queryString.isEmpty()){
		    uri = uri + "?" + queryString;
		}

		String httpMethod = requestContext.getMethod();

		MultivaluedMap<String, String> headersMap = requestContext.getHeaders();

		String transId = headersMap.getFirst(AAIHeaderProperties.TRANSACTION_ID);
		String fromAppId = headersMap.getFirst(AAIHeaderProperties.FROM_APP_ID);
		
		LoggingContext.init();
		LoggingContext.requestId(transId);
		LoggingContext.partnerName(fromAppId);
		LoggingContext.targetEntity(environment.getProperty("spring.application.name"));
		LoggingContext.component(fromAppId);
		LoggingContext.serviceName(httpMethod + " " + uri);
		LoggingContext.targetServiceName(httpMethod + " " + uri);
		LoggingContext.statusCode(LoggingContext.StatusCode.COMPLETE);
	}
	
}
