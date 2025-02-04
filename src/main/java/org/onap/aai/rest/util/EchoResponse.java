/*
 * ============LICENSE_START=======================================================
 * Copyright (C) 2022 Bell Canada
 * Modification Copyright (C) 2022 Deutsche Telekom SA
 * ================================================================================
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 * http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 * ============LICENSE_END=========================================================
 */
package org.onap.aai.rest.util;

import java.util.ArrayList;

import javax.servlet.http.HttpServletRequest;
import javax.ws.rs.GET;
import javax.ws.rs.Path;
import javax.ws.rs.Produces;
import javax.ws.rs.QueryParam;
import javax.ws.rs.core.Context;
import javax.ws.rs.core.HttpHeaders;
import javax.ws.rs.core.MediaType;
import javax.ws.rs.core.Response;
import javax.ws.rs.core.Response.Status;

import org.onap.aai.exceptions.AAIException;
import org.onap.aai.logging.ErrorLogHelper;
import org.onap.aai.restcore.RESTAPI;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.stereotype.Component;

@Component
@Path("/util")
public class EchoResponse extends RESTAPI {
    private static final Logger LOGGER = LoggerFactory.getLogger(EchoResponse.class);

    public static final String echoPath = "/util/echo";
    private final AaiGraphChecker aaiGraphChecker;
    
    private static final String UP_RESPONSE="{\"status\":\"UP\",\"groups\":[\"liveness\",\"readiness\"]}";
   
  
    @Autowired
    public EchoResponse(AaiGraphChecker aaiGraphChecker ) {
        this.aaiGraphChecker = aaiGraphChecker;
    }

    @GET
    @Produces({MediaType.APPLICATION_XML, MediaType.APPLICATION_JSON})
    @Path("/echo")
    public Response echoResult(@Context HttpHeaders headers, @Context HttpServletRequest req,
                               @QueryParam("action") String myAction) {
        Response response;

        String fromAppId;
        String transId;
        try {
            fromAppId = getFromAppId(headers);
            transId = getTransId(headers);
        } catch (AAIException e) {
            ArrayList<String> templateVars = new ArrayList<>();
            templateVars.add("PUT uebProvider");
            templateVars.add("addTopic");
            return Response
                    .status(e.getErrorObject().getHTTPResponseCode())
                    .entity(ErrorLogHelper.getRESTAPIErrorResponse(headers.getAcceptableMediaTypes(), e, templateVars))
                    .build();
        }

        ArrayList<String> templateVars = new ArrayList<>();
        templateVars.add(fromAppId);
        templateVars.add(transId);

        try {
            if (myAction != null) {
                LOGGER.info("Checking Database Connectivity...!");
                if (!aaiGraphChecker.isAaiGraphDbAvailable()) {
                    throw new AAIException("AAI_5105", "Error establishing a database connection");
                }
                return generateSuccessResponse();
            }
            return generateSuccessResponse();

        } catch (AAIException aaiException) {
        	ErrorLogHelper.logException(aaiException);
            return generateFailureResponse(headers, templateVars, aaiException);
        } catch (Exception e) {
            AAIException aaiException = new AAIException("AAI_4000", e);
            ErrorLogHelper.logException(aaiException);
            return generateFailureResponse(headers, templateVars, aaiException);
        }
    }

    private Response generateSuccessResponse() {
    	return Response.status(Status.OK)
    			.entity(UP_RESPONSE)
    			.build();
    }

    private Response generateFailureResponse(HttpHeaders headers, ArrayList<String> templateVariables,
    		AAIException aaiException) {
    	return Response.status(aaiException.getErrorObject().getHTTPResponseCode()).entity(ErrorLogHelper
    			.getRESTAPIErrorResponseWithLogging(headers.getAcceptableMediaTypes(), aaiException, templateVariables))
    			.build();
    }
}
