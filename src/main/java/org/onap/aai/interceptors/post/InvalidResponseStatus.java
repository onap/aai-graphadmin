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

import org.onap.aai.exceptions.AAIException;
import org.onap.aai.interceptors.AAIContainerFilter;
import org.onap.aai.logging.ErrorLogHelper;

import jakarta.annotation.Priority;
import jakarta.ws.rs.container.ContainerRequestContext;
import jakarta.ws.rs.container.ContainerResponseContext;
import jakarta.ws.rs.container.ContainerResponseFilter;
import jakarta.ws.rs.core.MediaType;
import java.io.IOException;
import java.util.ArrayList;
import java.util.List;

@Priority(AAIResponseFilterPriority.INVALID_RESPONSE_STATUS)
public class InvalidResponseStatus extends AAIContainerFilter implements ContainerResponseFilter {

	@Override
	public void filter(ContainerRequestContext requestContext, ContainerResponseContext responseContext)
			throws IOException {

		if(responseContext.getStatus() == 405){

		    responseContext.setStatus(400);
			AAIException e = new AAIException("AAI_3012");
			ArrayList<String> templateVars = new ArrayList<>();

			List<MediaType> mediaTypeList = new ArrayList<>();

			String contentType = responseContext.getHeaderString("Content-Type");

			if (contentType == null) {
				mediaTypeList.add(MediaType.APPLICATION_XML_TYPE);
			} else {
				mediaTypeList.add(MediaType.valueOf(contentType));
			}

			String message = ErrorLogHelper.getRESTAPIErrorResponse(mediaTypeList, e, templateVars);

			responseContext.setEntity(message);
		}

	}

}
