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

import org.onap.aai.Profiles;
import org.onap.aai.exceptions.AAIException;
import org.onap.aai.interceptors.AAIContainerFilter;
import org.onap.aai.logging.ErrorLogHelper;
import org.onap.aai.service.AuthorizationService;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.context.annotation.Profile;

import javax.annotation.Priority;
import javax.ws.rs.container.ContainerRequestContext;
import javax.ws.rs.container.ContainerRequestFilter;
import javax.ws.rs.container.PreMatching;
import javax.ws.rs.core.MediaType;
import javax.ws.rs.core.Response;
import javax.ws.rs.ext.Provider;
import java.io.IOException;
import java.util.ArrayList;
import java.util.List;
import java.util.Optional;

@Provider
@Profile(Profiles.ONE_WAY_SSL)
@PreMatching
@Priority(AAIRequestFilterPriority.AUTHORIZATION)
public class OneWaySslAuthorization extends AAIContainerFilter implements ContainerRequestFilter {

    @Autowired
    private AuthorizationService authorizationService;

    @Override
    public void filter(ContainerRequestContext containerRequestContext) throws IOException
    {

        if(containerRequestContext.getUriInfo().getRequestUri().getPath().matches("^.*/util/echo$")){
            return;
        }

        String basicAuth = containerRequestContext.getHeaderString("Authorization");
        List<MediaType> acceptHeaderValues = containerRequestContext.getAcceptableMediaTypes();

        if(basicAuth == null || !basicAuth.startsWith("Basic ")){
            Optional<Response> responseOptional = errorResponse("AAI_3300", acceptHeaderValues);
            containerRequestContext.abortWith(responseOptional.get());
            return;
        }

        basicAuth = basicAuth.replaceAll("Basic ", "");

        if(!authorizationService.checkIfUserAuthorized(basicAuth)){
            Optional<Response> responseOptional = errorResponse("AAI_3300", acceptHeaderValues);
            containerRequestContext.abortWith(responseOptional.get());
            return;
        }

    }

    private Optional<Response> errorResponse(String errorCode, List<MediaType> acceptHeaderValues) {
        AAIException aaie = new AAIException(errorCode);
        return Optional.of(Response.status(aaie.getErrorObject().getHTTPResponseCode())
                .entity(ErrorLogHelper.getRESTAPIErrorResponse(acceptHeaderValues, aaie, new ArrayList<>()))
                .build());

    }
}
