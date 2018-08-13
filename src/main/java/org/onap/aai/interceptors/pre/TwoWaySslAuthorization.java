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

import org.onap.aai.auth.AAIAuthCore;
import org.onap.aai.exceptions.AAIException;
import org.onap.aai.interceptors.AAIContainerFilter;
import org.onap.aai.interceptors.AAIHeaderProperties;
import org.onap.aai.logging.ErrorLogHelper;
import org.onap.aai.restcore.HttpMethod;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.context.annotation.Profile;

import javax.annotation.Priority;
import javax.security.auth.x500.X500Principal;
import javax.servlet.http.HttpServletRequest;
import javax.ws.rs.container.ContainerRequestContext;
import javax.ws.rs.container.ContainerRequestFilter;
import javax.ws.rs.container.PreMatching;
import javax.ws.rs.core.MediaType;
import javax.ws.rs.core.Response;
import javax.ws.rs.ext.Provider;
import java.security.cert.X509Certificate;
import java.util.*;
import java.util.stream.Collectors;

@Provider
@PreMatching
@Priority(AAIRequestFilterPriority.AUTHORIZATION)
@Profile("two-way-ssl")
public class TwoWaySslAuthorization extends AAIContainerFilter implements ContainerRequestFilter {

	@Autowired
	private HttpServletRequest httpServletRequest;

	@Autowired
	private AAIAuthCore aaiAuthCore;

	@Override
	public void filter(ContainerRequestContext requestContext) {

		Optional<Response> oResp;

		String uri = requestContext.getUriInfo().getAbsolutePath().getPath();
		String httpMethod = getHttpMethod(requestContext);

		List<MediaType> acceptHeaderValues = requestContext.getAcceptableMediaTypes();

		Optional<String> authUser = getUser(this.httpServletRequest);

		if (authUser.isPresent()) {
			oResp = this.authorize(uri, httpMethod, acceptHeaderValues, authUser.get(),
					this.getHaProxyUser(this.httpServletRequest), getCertIssuer(this.httpServletRequest));
			if (oResp.isPresent()) {
				requestContext.abortWith(oResp.get());
				return;
			}
		} else {
			AAIException aaie = new AAIException("AAI_9107");
			requestContext
					.abortWith(Response
							.status(aaie.getErrorObject().getHTTPResponseCode()).entity(ErrorLogHelper
									.getRESTAPIErrorResponseWithLogging(acceptHeaderValues, aaie, new ArrayList<>()))
							.build());
		}

	}

	private String getCertIssuer(HttpServletRequest hsr) {
		String issuer =  hsr.getHeader("X-AAI-SSL-Issuer");
		if (issuer != null && !issuer.isEmpty()) {
			// the haproxy header replaces the ', ' with '/' and reverses on the '/' need to undo that.
			List<String> broken = Arrays.asList(issuer.split("/"));
			broken = broken.stream().filter(s -> !s.isEmpty()).collect(Collectors.toList());
			Collections.reverse(broken);
			issuer = String.join(", ", broken);
		} else {
			if (hsr.getAttribute("javax.servlet.request.cipher_suite") != null) {
				X509Certificate[] certChain = (X509Certificate[]) hsr.getAttribute("javax.servlet.request.X509Certificate");
				if (certChain != null && certChain.length > 0) {
					X509Certificate clientCert = certChain[0];
					issuer = clientCert.getIssuerX500Principal().getName();
				}
			}
		}
		return issuer;
	}

	private String getHttpMethod(ContainerRequestContext requestContext) {
		String httpMethod = requestContext.getMethod();
		if ("POST".equalsIgnoreCase(httpMethod)
				&& "PATCH".equals(requestContext.getHeaderString(AAIHeaderProperties.HTTP_METHOD_OVERRIDE))) {
			httpMethod = HttpMethod.MERGE_PATCH.toString();
		}
		if (httpMethod.equalsIgnoreCase(HttpMethod.MERGE_PATCH.toString()) || "patch".equalsIgnoreCase(httpMethod)) {
			httpMethod = HttpMethod.PUT.toString();
		}
		return httpMethod;
	}

	private Optional<String> getUser(HttpServletRequest hsr) {
		String authUser = null;
		if (hsr.getAttribute("javax.servlet.request.cipher_suite") != null) {
			X509Certificate[] certChain = (X509Certificate[]) hsr.getAttribute("javax.servlet.request.X509Certificate");

			/*
			 * If the certificate is null or the certificate chain length is zero Then
			 * retrieve the authorization in the request header Authorization Check that it
			 * is not null and that it starts with Basic and then strip the basic portion to
			 * get the base64 credentials Check if this is contained in the AAIBasicAuth
			 * Singleton class If it is, retrieve the username associated with that
			 * credentials and set to authUser Otherwise, get the principal from certificate
			 * and use that authUser
			 */

			if (certChain == null || certChain.length == 0) {

				String authorization = hsr.getHeader("Authorization");

				if (authorization != null && authorization.startsWith("Basic ")) {
					authUser = authorization.replace("Basic ", "");
				}

			} else {
				X509Certificate clientCert = certChain[0];
				X500Principal subjectDN = clientCert.getSubjectX500Principal();
				authUser = subjectDN.toString().toLowerCase();
			}
		}

		return Optional.ofNullable(authUser);
	}

	private String getHaProxyUser(HttpServletRequest hsr) {
		String haProxyUser;
		if (Objects.isNull(hsr.getHeader("X-AAI-SSL-Client-CN")) 
				|| Objects.isNull(hsr.getHeader("X-AAI-SSL-Client-OU"))
				|| Objects.isNull(hsr.getHeader("X-AAI-SSL-Client-O"))
				|| Objects.isNull(hsr.getHeader("X-AAI-SSL-Client-L"))
				|| Objects.isNull(hsr.getHeader("X-AAI-SSL-Client-ST"))
				|| Objects.isNull(hsr.getHeader("X-AAI-SSL-Client-C"))) {
			haProxyUser = "";
		} else {
			haProxyUser = String.format("CN=%s, OU=%s, O=\"%s\", L=%s, ST=%s, C=%s",
					Objects.toString(hsr.getHeader("X-AAI-SSL-Client-CN"), ""),
					Objects.toString(hsr.getHeader("X-AAI-SSL-Client-OU"), ""),
					Objects.toString(hsr.getHeader("X-AAI-SSL-Client-O"), ""),
					Objects.toString(hsr.getHeader("X-AAI-SSL-Client-L"), ""),
					Objects.toString(hsr.getHeader("X-AAI-SSL-Client-ST"), ""),
					Objects.toString(hsr.getHeader("X-AAI-SSL-Client-C"), "")).toLowerCase();
		}
		return haProxyUser;
	}

	private Optional<Response> authorize(String uri, String httpMethod, List<MediaType> acceptHeaderValues,
			String authUser, String haProxyUser, String issuer) {
		Response response = null;
		try {
			if (!aaiAuthCore.authorize(authUser, uri, httpMethod, haProxyUser, issuer)) {
				throw new AAIException("AAI_9101", "Request on " + httpMethod + " " + uri + " status is not OK");
			}
		} catch (AAIException e) {
			response = Response.status(e.getErrorObject().getHTTPResponseCode())
					.entity(ErrorLogHelper.getRESTAPIErrorResponseWithLogging(acceptHeaderValues, e, new ArrayList<>()))
					.build();
		}
		return Optional.ofNullable(response);
	}

}
