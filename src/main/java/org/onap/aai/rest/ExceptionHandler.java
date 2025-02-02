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
package org.onap.aai.rest;

import com.fasterxml.jackson.core.JsonParseException;
import com.fasterxml.jackson.databind.JsonMappingException;
import com.sun.istack.SAXParseException2;
import org.onap.aai.exceptions.AAIException;
import org.onap.aai.logging.ErrorLogHelper;

import jakarta.servlet.http.HttpServletRequest;
import jakarta.ws.rs.WebApplicationException;
import jakarta.ws.rs.core.Context;
import jakarta.ws.rs.core.HttpHeaders;
import jakarta.ws.rs.core.MediaType;
import jakarta.ws.rs.core.Response;
import jakarta.ws.rs.ext.ExceptionMapper;
import jakarta.ws.rs.ext.Provider;
import java.util.ArrayList;
import java.util.List;

/**
 * The Class ExceptionHandler.
 */
@Provider
public class ExceptionHandler implements ExceptionMapper<Exception> {

    @Context
    private HttpServletRequest request;
    
    @Context
    private HttpHeaders headers;
    
    /**
	 * @{inheritDoc}
	 */
    @Override
    public Response toResponse(Exception exception) {

    	Response response = null;
    	ArrayList<String> templateVars = new ArrayList<String>();

    	//the general case is that cxf will give us a WebApplicationException
    	//with a linked exception
    	if (exception instanceof WebApplicationException e) {
    		if (e.getCause() != null) {
    			if (e.getCause() instanceof SAXParseException2) {
    				templateVars.add("UnmarshalException");
    				AAIException ex = new AAIException("AAI_4007", exception);
    				response = Response
    						.status(400)
    						.entity(ErrorLogHelper.getRESTAPIErrorResponse(headers.getAcceptableMediaTypes(), ex, templateVars))
    						.build();
    			}
    		}
    	} else if (exception instanceof JsonParseException) {
    		//jackson does it differently so we get the direct JsonParseException
    		templateVars.add("JsonParseException");
    		AAIException ex = new AAIException("AAI_4007", exception);
    		response = Response
    				.status(400)
    				.entity(ErrorLogHelper.getRESTAPIErrorResponse(headers.getAcceptableMediaTypes(), ex, templateVars))
    				.build();
        } else if (exception instanceof JsonMappingException) {
    		//jackson does it differently so we get the direct JsonParseException
    		templateVars.add("JsonMappingException");
    		AAIException ex = new AAIException("AAI_4007", exception);
    		response = Response
    				.status(400)
    				.entity(ErrorLogHelper.getRESTAPIErrorResponse(headers.getAcceptableMediaTypes(), ex, templateVars))
    				.build();
        } 
    	
    	// it didn't get set above, we wrap a general fault here
    	if (response == null) { 
    		
    		Exception actual_e = exception;
    		if (exception instanceof WebApplicationException e) {
    			response = e.getResponse();
    		} else { 
    			templateVars.add(request.getMethod());
    			templateVars.add("unknown");
    			AAIException ex = new AAIException("AAI_4000", actual_e);
    			List<MediaType> mediaTypes = headers.getAcceptableMediaTypes();
    			int setError = 0;

    			for (MediaType mediaType : mediaTypes) { 
    				if (MediaType.APPLICATION_XML_TYPE.isCompatible(mediaType)) {
    					response = Response
    							.status(400)
    							.type(MediaType.APPLICATION_XML_TYPE)
    							.entity(ErrorLogHelper.getRESTAPIErrorResponse(headers.getAcceptableMediaTypes(), ex, templateVars))
    							.build();	
    					setError = 1;
    				} 
    			}
    			if (setError == 0) { 
    				response = Response
    						.status(400)
    						.type(MediaType.APPLICATION_JSON_TYPE)
    						.entity(ErrorLogHelper.getRESTAPIErrorResponse(headers.getAcceptableMediaTypes(), ex, templateVars))
    						.build();	
    			}
    		}
    	}		
    	return response;
    }
}
