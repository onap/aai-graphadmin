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
import org.onap.aai.logging.ErrorLogHelper;
import org.onap.aai.service.RetiredService;
import org.onap.aai.util.AAIConfig;
import org.springframework.beans.factory.annotation.Value;

import jakarta.annotation.Priority;
import jakarta.ws.rs.container.ContainerRequestContext;
import jakarta.ws.rs.container.ContainerRequestFilter;
import jakarta.ws.rs.container.PreMatching;
import jakarta.ws.rs.core.Response;
import jakarta.ws.rs.ext.Provider;
import java.io.IOException;
import java.util.ArrayList;
import java.util.List;
import java.util.regex.Matcher;
import java.util.regex.Pattern;

// Can cache this so if the uri was already cached then it won't run the string
// matching each time but only does it for the first time

@Provider
@PreMatching
@Priority(AAIRequestFilterPriority.RETIRED_SERVICE)
public class RetiredInterceptor extends AAIContainerFilter implements ContainerRequestFilter {

    private static final Pattern VERSION_PATTERN = Pattern.compile("v\\d+|latest");

    private RetiredService retiredService;

    private String basePath;

    public RetiredInterceptor(RetiredService retiredService, @Value("${schema.uri.base.path}") String basePath){
        this.retiredService = retiredService;
        this.basePath = basePath;
        if(!basePath.endsWith("/")){
            this.basePath = basePath + "/";
        }
    }
    @Override
    public void filter(ContainerRequestContext containerRequestContext) throws IOException {

        String requestURI = containerRequestContext.getUriInfo().getAbsolutePath().getPath();

        String version = extractVersionFromPath(requestURI);

        List<Pattern> retiredAllVersionList = retiredService.getRetiredAllVersionList();


        if(checkIfUriRetired(containerRequestContext, retiredAllVersionList, version, requestURI, "")){
            return;
        }

        List<Pattern> retiredVersionList = retiredService.getRetiredPatterns();

        checkIfUriRetired(containerRequestContext, retiredVersionList, version, requestURI);
    }

    public boolean checkIfUriRetired(ContainerRequestContext containerRequestContext,
                                     List<Pattern> retiredPatterns,
                                     String version,
                                     String requestURI,
                                     String message){


        for(Pattern retiredPattern : retiredPatterns){
            if(retiredPattern.matcher(requestURI).matches()){
                AAIException e;

                if(message == null){
                    e = new AAIException("AAI_3007");
                } else {
                    e = new AAIException("AAI_3015");
                }

                ArrayList<String> templateVars = new ArrayList<>();

                if (templateVars.isEmpty()) {
                    templateVars.add("PUT");
                    if(requestURI != null){
                        requestURI = requestURI.replaceAll(basePath, "");
                    }
                    templateVars.add(requestURI);
                    if(message == null){
                        templateVars.add(version);
                        templateVars.add(AAIConfig.get("aai.default.api.version", ""));
                    }
                }

                Response response = Response
                        .status(e.getErrorObject().getHTTPResponseCode())
                        .entity(
                                ErrorLogHelper
                                        .getRESTAPIErrorResponse(
                                                containerRequestContext.getAcceptableMediaTypes(), e, templateVars
                                        )
                        )
                        .build();

                containerRequestContext.abortWith(response);

                return true;
            }
        }

        return false;
    }

    public boolean checkIfUriRetired(ContainerRequestContext containerRequestContext,
                                     List<Pattern> retiredPatterns,
                                     String version,
                                     String requestURI){
        return checkIfUriRetired(containerRequestContext, retiredPatterns, version, requestURI, null);
    }

    protected String extractVersionFromPath(String requestURI) {
        Matcher versionMatcher = VERSION_PATTERN.matcher(requestURI);
        String version = null;

        if(versionMatcher.find()){
            version = versionMatcher.group(0);
        }
        return version;
    }

}
