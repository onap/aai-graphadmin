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

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import com.google.gson.JsonObject;
import org.glassfish.jersey.message.internal.ReaderWriter;
import org.onap.aai.exceptions.AAIException;
import org.onap.aai.interceptors.AAIContainerFilter;
import org.onap.aai.interceptors.AAIHeaderProperties;
import org.onap.aai.logging.LogFormatTools;
import org.onap.aai.util.AAIConfig;
import org.onap.aai.util.AAIConstants;
import org.onap.aai.util.HbaseSaltPrefixer;
import org.springframework.beans.factory.annotation.Autowired;

import jakarta.annotation.Priority;
import jakarta.servlet.http.HttpServletRequest;
import jakarta.ws.rs.HttpMethod;
import jakarta.ws.rs.container.ContainerRequestContext;
import jakarta.ws.rs.container.ContainerRequestFilter;
import jakarta.ws.rs.container.PreMatching;
import jakarta.ws.rs.core.MediaType;
import jakarta.ws.rs.core.MultivaluedMap;
import jakarta.ws.rs.ext.Provider;
import java.io.ByteArrayInputStream;
import java.io.ByteArrayOutputStream;
import java.io.IOException;
import java.io.InputStream;
import java.security.SecureRandom;
import java.util.Random;
import java.util.UUID;

@Provider
@PreMatching
@Priority(AAIRequestFilterPriority.REQUEST_TRANS_LOGGING)
public class RequestTransactionLogging extends AAIContainerFilter implements ContainerRequestFilter {

	private static final Logger LOGGER = LoggerFactory.getLogger(RequestTransactionLogging.class);

	@Autowired
	private HttpServletRequest httpServletRequest;

	private static final String DEFAULT_CONTENT_TYPE = MediaType.APPLICATION_JSON;
	private static final String DEFAULT_RESPONSE_TYPE = MediaType.APPLICATION_XML;

	private static final String CONTENT_TYPE = "Content-Type";
	private static final String ACCEPT = "Accept";
	private static final String TEXT_PLAIN = "text/plain";

	@Override
	public void filter(ContainerRequestContext requestContext) throws IOException {

		String currentTimeStamp = genDate();
		String fullId = this.getAAITxIdToHeader(currentTimeStamp);
		this.addToRequestContext(requestContext, AAIHeaderProperties.AAI_TX_ID, fullId);
		this.addToRequestContext(requestContext, AAIHeaderProperties.AAI_REQUEST, this.getRequest(requestContext, fullId));
		this.addToRequestContext(requestContext, AAIHeaderProperties.AAI_REQUEST_TS, currentTimeStamp);
		this.addDefaultContentType(requestContext);
	}

	private void addToRequestContext(ContainerRequestContext requestContext, String name, String aaiTxIdToHeader) {
		requestContext.setProperty(name, aaiTxIdToHeader);
	}

	private void addDefaultContentType(ContainerRequestContext requestContext) {

		MultivaluedMap<String, String> headersMap = requestContext.getHeaders();
		String contentType = headersMap.getFirst(CONTENT_TYPE);
		String acceptType  = headersMap.getFirst(ACCEPT);

		if(contentType == null && !requestContext.getMethod().equals(HttpMethod.GET.toString())){
			LOGGER.debug("Content Type header missing in the request, adding one of [{}]", DEFAULT_CONTENT_TYPE);
			requestContext.getHeaders().putSingle(CONTENT_TYPE, DEFAULT_CONTENT_TYPE);
		}

		if(acceptType == null){
			LOGGER.debug("Accept header missing in the request, adding one of [{}]", DEFAULT_RESPONSE_TYPE);
			requestContext.getHeaders().putSingle(ACCEPT, DEFAULT_RESPONSE_TYPE);
		}
	}

	private String getAAITxIdToHeader(String currentTimeStamp) {
		String txId = UUID.randomUUID().toString();
		try {
			Random rand = new SecureRandom();
			int number = rand.nextInt(99999);
			txId = HbaseSaltPrefixer.getInstance().prependSalt(AAIConfig.get(AAIConstants.AAI_NODENAME) + "-"
					+ currentTimeStamp + "-" + number ); //new Random(System.currentTimeMillis()).nextInt(99999)
		} catch (AAIException e) {
		}

		return txId;
	}

	private String getRequest(ContainerRequestContext requestContext, String fullId) {

		JsonObject request = new JsonObject();
		request.addProperty("ID", fullId);
		request.addProperty("Http-Method", requestContext.getMethod());
		request.addProperty(CONTENT_TYPE, httpServletRequest.getContentType());
		request.addProperty("Headers", requestContext.getHeaders().toString());

		ByteArrayOutputStream out = new ByteArrayOutputStream();
		InputStream in = requestContext.getEntityStream();

		try {
			if (in.available() > 0) {
				ReaderWriter.writeTo(in, out);
				byte[] requestEntity = out.toByteArray();
				request.addProperty("Payload", new String(requestEntity, "UTF-8"));
				requestContext.setEntityStream(new ByteArrayInputStream(requestEntity));
			}
		} catch (IOException ex) {
			LOGGER.error("An exception occurred during the transaction logging: " + LogFormatTools.getStackTop(ex));
		}

		return request.toString();
	}

}
