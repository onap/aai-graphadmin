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

import org.onap.aai.interceptors.AAIContainerFilter;
import org.onap.aai.setup.SchemaVersions;
import org.springframework.beans.factory.annotation.Autowired;

import javax.annotation.Priority;
import javax.ws.rs.container.ContainerRequestContext;
import javax.ws.rs.container.ContainerRequestFilter;
import javax.ws.rs.container.PreMatching;
import java.net.URI;

@PreMatching
@Priority(AAIRequestFilterPriority.LATEST)
public class VersionLatestInterceptor extends AAIContainerFilter implements ContainerRequestFilter {

    private final SchemaVersions schemaVersions;

    @Autowired
    public VersionLatestInterceptor(SchemaVersions schemaVersions){
        this.schemaVersions = schemaVersions;
    }

    @Override
    public void filter(ContainerRequestContext requestContext) {

        String uri = requestContext.getUriInfo().getPath();

		if(uri.startsWith("latest")){
		    String absolutePath = requestContext.getUriInfo().getAbsolutePath().toString();
            String latest = absolutePath.replaceFirst("latest", schemaVersions.getDefaultVersion().toString());
            requestContext.setRequestUri(URI.create(latest));
            return;
        }

    }
}
