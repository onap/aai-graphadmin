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

import org.onap.aai.exceptions.AAIException;
import org.onap.aai.interceptors.AAIContainerFilter;
import org.onap.aai.interceptors.AAIHeaderProperties;
import org.onap.aai.logging.ErrorLogHelper;
import org.onap.logging.filter.base.Constants;
import org.onap.logging.ref.slf4j.ONAPLogConstants;

import javax.annotation.Priority;
import javax.ws.rs.container.ContainerRequestContext;
import javax.ws.rs.container.ContainerRequestFilter;
import javax.ws.rs.container.PreMatching;
import javax.ws.rs.core.MediaType;
import javax.ws.rs.core.MultivaluedMap;
import javax.ws.rs.core.Response;
import javax.ws.rs.ext.Provider;
import java.io.IOException;
import java.util.ArrayList;
import java.util.List;
import java.util.Optional;

@Provider
@PreMatching
@Priority(AAIRequestFilterPriority.HEADER_VALIDATION)
public class HeaderValidation extends AAIContainerFilter implements ContainerRequestFilter {

	@Override
	public void filter(ContainerRequestContext requestContext) throws IOException {

		Optional<Response> oResp;

		MultivaluedMap<String, String> headersMap = requestContext.getHeaders();

		List<MediaType> acceptHeaderValues = requestContext.getAcceptableMediaTypes();
		String fromAppId = getPartnerName(requestContext);
		oResp = this.validateHeaderValuePresence(fromAppId, "AAI_4009", acceptHeaderValues);
		if (oResp.isPresent()) {
			requestContext.abortWith(oResp.get());
			return;
		}
		String transId = getRequestId(requestContext);
		oResp = this.validateHeaderValuePresence(transId, "AAI_4010", acceptHeaderValues);
		if (oResp.isPresent()) {
			requestContext.abortWith(oResp.get());
			return;
		}

	}
	
	private Optional<Response> validateHeaderValuePresence(String value, String errorCode,
			List<MediaType> acceptHeaderValues) {
		Response response = null;
		AAIException aaie;
		if (value == null || value.isEmpty()) {
			aaie = new AAIException(errorCode);
			return Optional.of(Response.status(aaie.getErrorObject().getHTTPResponseCode())
					.entity(ErrorLogHelper.getRESTAPIErrorResponse(acceptHeaderValues, aaie, new ArrayList<>()))
					.build());
		}

		return Optional.ofNullable(response);
	}
	public String getRequestId(ContainerRequestContext requestContext) {
		String requestId = requestContext.getHeaderString(ONAPLogConstants.Headers.REQUEST_ID);
		if (requestId == null || requestId.isEmpty()) {
			requestId = requestContext.getHeaderString(Constants.HttpHeaders.HEADER_REQUEST_ID);
			if (requestId == null || requestId.isEmpty()) {
				requestId = requestContext.getHeaderString(Constants.HttpHeaders.TRANSACTION_ID);
				if (requestId == null || requestId.isEmpty()) {
					requestId = requestContext.getHeaderString(Constants.HttpHeaders.ECOMP_REQUEST_ID);
					if (requestId == null || requestId.isEmpty()) {
						return requestId;
					}
				}
			}
		}
		if (requestContext.getHeaders().get(ONAPLogConstants.Headers.REQUEST_ID) != null) {
			requestContext.getHeaders().get(ONAPLogConstants.Headers.REQUEST_ID).clear();
		}
		if (requestContext.getHeaders().get(Constants.HttpHeaders.TRANSACTION_ID) != null) {
			requestContext.getHeaders().get(Constants.HttpHeaders.TRANSACTION_ID).clear();
		}
		if (requestContext.getHeaders().get(Constants.HttpHeaders.HEADER_REQUEST_ID) != null) {
			requestContext.getHeaders().get(Constants.HttpHeaders.HEADER_REQUEST_ID).clear();
		}
		if (requestContext.getHeaders().get(Constants.HttpHeaders.ECOMP_REQUEST_ID) != null) {
			requestContext.getHeaders().get(Constants.HttpHeaders.ECOMP_REQUEST_ID).clear();
		}
		requestContext.getHeaders().add(Constants.HttpHeaders.TRANSACTION_ID, requestId);

		return requestId;
	}

	public String getPartnerName(ContainerRequestContext requestContext) {
		String partnerName = requestContext.getHeaderString(ONAPLogConstants.Headers.PARTNER_NAME);
		if (partnerName == null || (partnerName.isEmpty())) {
			partnerName = requestContext.getHeaderString(AAIHeaderProperties.FROM_APP_ID);
			if (partnerName == null || (partnerName.isEmpty())) {
				return partnerName;
			}
		}
		if (requestContext.getHeaders().get(ONAPLogConstants.Headers.PARTNER_NAME) != null) {
			requestContext.getHeaders().get(ONAPLogConstants.Headers.PARTNER_NAME).clear();
		}
		if (requestContext.getHeaders().get(AAIHeaderProperties.FROM_APP_ID) != null) {
			requestContext.getHeaders().get(AAIHeaderProperties.FROM_APP_ID).clear();
		}
		requestContext.getHeaders().add(AAIHeaderProperties.FROM_APP_ID, partnerName);
		return partnerName;
	}
}
