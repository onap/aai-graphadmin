/**
 * ============LICENSE_START=======================================================
 * org.onap.aai
 * ================================================================================
 * Copyright © 2026 Deutsche Telekom. All rights reserved.
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

package org.onap.aai.config;

import static org.junit.jupiter.api.Assertions.assertEquals;

import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.ValueSource;
import org.springframework.boot.actuate.endpoint.Access;
import org.springframework.core.env.StandardEnvironment;
import org.springframework.core.io.ClassPathResource;
import org.springframework.core.io.support.ResourcePropertySource;

public class EndpointAccessPropertyTest {

    // GenTester and the other batch tools run a plain AnnotationConfigApplicationContext, whose
    // environment converts enums case-sensitively, unlike SpringApplication's.
    @ParameterizedTest
    @ValueSource(strings = {"application.properties", "application-test.properties"})
    public void thatEndpointAccessResolvesInPlainSpringContext(String file) throws Exception {
        StandardEnvironment environment = new StandardEnvironment();
        environment.getPropertySources().addFirst(new ResourcePropertySource(new ClassPathResource(file)));

        assertEquals(Access.UNRESTRICTED,
                environment.getProperty("management.endpoints.access.default", Access.class));
    }
}
