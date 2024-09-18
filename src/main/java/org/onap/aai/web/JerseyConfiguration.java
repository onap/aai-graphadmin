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
package org.onap.aai.web;

import java.util.List;
import java.util.Set;
import java.util.logging.Logger;
import java.util.stream.Collectors;
import javax.annotation.Priority;
import javax.ws.rs.container.ContainerRequestFilter;
import javax.ws.rs.container.ContainerResponseFilter;
import org.glassfish.jersey.logging.LoggingFeature;
import org.glassfish.jersey.server.ResourceConfig;
import org.glassfish.jersey.servlet.ServletProperties;
import org.onap.aai.rest.AuditSqlDbConsumer;
import org.onap.aai.rest.QueryConsumer;
import org.onap.aai.rest.util.EchoResponse;
import org.onap.logging.filter.base.AuditLogContainerFilter;
import org.reflections8.Reflections;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.context.annotation.Profile;
import org.springframework.core.env.Environment;
import org.springframework.stereotype.Component;

@Component
public class JerseyConfiguration extends ResourceConfig {

    private static final Logger log = Logger.getLogger(JerseyConfiguration.class.getName());

    private Environment env;

    @Autowired
    public JerseyConfiguration(Environment env) {

        this.env = env;

        register(QueryConsumer.class);
        register(AuditSqlDbConsumer.class);
        register(EchoResponse.class);

        //Filters
        registerFilters(ContainerRequestFilter.class);
        registerFilters(ContainerResponseFilter.class);
        registerFilters(AuditLogContainerFilter.class);

        property(ServletProperties.FILTER_FORWARD_ON_404, true);

        // Following registers the request headers and response headers
        // If the LoggingFilter second argument is set to true, it will print response value as well
        if ("true".equalsIgnoreCase(env.getProperty("aai.request.logging.enabled"))) {
            register(new LoggingFeature(log));
        }
    }

    public <T> void registerFilters(Class<T> type) {

        // Find all the classes within the interceptors package
        // TODO: do not rely on the transitive reflections dependency of janusgraph-core
    	Reflections loggingReflections = new Reflections("org.onap.aai.aailog.filter");
		Reflections reflections = new Reflections("org.onap.aai.interceptors");
		// Filter them based on the clazz that was passed in
        Set<Class<? extends T>> filters = loggingReflections.getSubTypesOf(type);
        filters.addAll(reflections.getSubTypesOf(type));

        // Check to ensure that each of the filter has the @Priority annotation and if not throw exception
        for (Class filterClass : filters) {
            if (filterClass.getAnnotation(Priority.class) == null) {
                throw new RuntimeException("Container filter " + filterClass.getName() + " does not have @Priority annotation");
            }
        }

        // Turn the set back into a list
        List<Class<? extends T>> filtersList = filters
                .stream()
                .filter(f -> {
                    if (f.isAnnotationPresent(Profile.class)
                            && !env.acceptsProfiles(f.getAnnotation(Profile.class).value())) {
                        return false;
                    }
                    return true;
                })
                .collect(Collectors.toList());

        // Sort them by their priority levels value
        filtersList.sort((c1, c2) -> Integer.valueOf(c1.getAnnotation(Priority.class).value()).compareTo(c2.getAnnotation(Priority.class).value()));

        // Then register this to the jersey application
        filtersList.forEach(this::register);
    }
}
