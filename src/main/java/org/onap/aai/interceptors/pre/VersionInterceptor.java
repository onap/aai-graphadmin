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
import org.onap.aai.setup.SchemaVersion;
import org.onap.aai.setup.SchemaVersions;
import org.springframework.beans.factory.annotation.Autowired;

import javax.annotation.Priority;
import javax.ws.rs.container.ContainerRequestContext;
import javax.ws.rs.container.ContainerRequestFilter;
import javax.ws.rs.container.PreMatching;
import javax.ws.rs.core.Response;
import java.util.ArrayList;
import java.util.Set;
import java.util.regex.Matcher;
import java.util.regex.Pattern;
import java.util.stream.Collectors;

@PreMatching
@Priority(AAIRequestFilterPriority.VERSION)
public class VersionInterceptor extends AAIContainerFilter implements ContainerRequestFilter {

    public static final Pattern EXTRACT_VERSION_PATTERN = Pattern.compile("^(v[1-9][0-9]*).*$");

    private final Set<String> allowedVersions;

    private final SchemaVersions schemaVersions;

    @Autowired
    public VersionInterceptor(SchemaVersions schemaVersions){
        this.schemaVersions = schemaVersions;
        allowedVersions  = schemaVersions.getVersions()
            .stream()
            .map(SchemaVersion::toString)
            .collect(Collectors.toSet());

    }

    @Override
    public void filter(ContainerRequestContext requestContext) {

        String uri = requestContext.getUriInfo().getPath();

        if (uri.startsWith("search") || uri.startsWith("util/echo") || uri.startsWith("tools") || uri.endsWith("audit-sql-db")) {
            return;
		}

        Matcher matcher = EXTRACT_VERSION_PATTERN.matcher(uri);

        String version = null;
        if(matcher.matches()){
            version = matcher.group(1);
        } else {
            requestContext.abortWith(createInvalidVersionResponse("AAI_3017", requestContext, version));
            return;
        }

        if(!allowedVersions.contains(version)){
            requestContext.abortWith(createInvalidVersionResponse("AAI_3016", requestContext, version));
        }
    }

    private Response createInvalidVersionResponse(String errorCode, ContainerRequestContext context, String version) {
        AAIException e = new AAIException(errorCode);
        ArrayList<String> templateVars = new ArrayList<>();

        if (templateVars.isEmpty()) {
            templateVars.add(context.getMethod());
            templateVars.add(context.getUriInfo().getPath());
            templateVars.add(version);
        }

        String entity = ErrorLogHelper.getRESTAPIErrorResponse(context.getAcceptableMediaTypes(), e, templateVars);

        return Response
                .status(e.getErrorObject().getHTTPResponseCode())
                .entity(entity)
                .build();
    }
}
