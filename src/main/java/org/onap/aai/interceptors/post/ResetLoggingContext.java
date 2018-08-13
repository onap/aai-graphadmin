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

import com.att.eelf.configuration.EELFLogger;
import com.att.eelf.configuration.EELFManager;
import org.onap.aai.interceptors.AAIContainerFilter;
import org.onap.aai.logging.LoggingContext;
import org.onap.aai.logging.LoggingContext.StatusCode;
import org.springframework.beans.factory.annotation.Autowired;

import javax.annotation.Priority;
import javax.servlet.http.HttpServletRequest;
import javax.ws.rs.container.ContainerRequestContext;
import javax.ws.rs.container.ContainerResponseContext;
import javax.ws.rs.container.ContainerResponseFilter;
import javax.ws.rs.core.Response.Status;
import javax.ws.rs.core.Response.StatusType;
import java.io.IOException;

@Priority(AAIResponseFilterPriority.RESET_LOGGING_CONTEXT)
public class ResetLoggingContext extends AAIContainerFilter implements ContainerResponseFilter {

	private static final EELFLogger LOGGER = EELFManager.getInstance().getLogger(ResetLoggingContext.class);

	@Autowired
	private HttpServletRequest httpServletRequest;
	
	@Override
	public void filter(ContainerRequestContext requestContext, ContainerResponseContext responseContext)
			throws IOException {

		this.cleanLoggingContext(responseContext);

	}

	private void cleanLoggingContext(ContainerResponseContext responseContext) {
		//String url = httpServletRequest.getRequestURL().toString();
		boolean success = true;
		String uri = httpServletRequest.getRequestURI();
		String queryString = httpServletRequest.getQueryString();

		if(queryString != null && !queryString.isEmpty()){
		    uri = uri + "?" + queryString;
		}
		// For now, we use the the HTTP status code, 
		// This may change, once the requirements for response codes are defined

		int httpStatusCode = responseContext.getStatus();
		if ( httpStatusCode < 100 || httpStatusCode > 599 ) {
			httpStatusCode = Status.INTERNAL_SERVER_ERROR.getStatusCode();
		}
		LoggingContext.responseCode(Integer.toString(httpStatusCode));
		
		StatusType sType = responseContext.getStatusInfo();
		if ( sType != null ) {
			Status.Family sFamily = sType.getFamily();
			if ( ! ( Status.Family.SUCCESSFUL.equals(sFamily)  ||
				( Status.NOT_FOUND.equals(Status.fromStatusCode(httpStatusCode)) ) ) ) {
				success = false;
			}		
		}
		else {
			if ( (httpStatusCode < 200 || httpStatusCode > 299) && ( ! ( Status.NOT_FOUND.equals(Status.fromStatusCode(httpStatusCode) ) ) ) ) {
				success = false;
			}
		}
		if (success) {
			LoggingContext.statusCode(StatusCode.COMPLETE);
			LOGGER.info(uri + " call succeeded");
		}
		else {
			LoggingContext.statusCode(StatusCode.ERROR);
			LOGGER.error(uri + " call failed with responseCode=" + httpStatusCode);
		}
		LoggingContext.clear();
		

	}

}
