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
package org.onap.aai.rest.client;

import org.onap.aai.restclient.RestClient;
import org.springframework.boot.autoconfigure.condition.ConditionalOnProperty;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;
import org.springframework.context.annotation.Profile;


@Configuration
public class ApertureConfiguration {

    @Bean(name = "apertureRestClient")
    @ConditionalOnProperty(name = "aperture.service.client", havingValue = "two-way-ssl", matchIfMissing = true)
    public RestClient apertureRestClientTwoWaySSL() {
        return new ApertureServiceRestClient();
    }

    @Bean(name = "apertureRestClient")
    @ConditionalOnProperty(name = "aperture.service.client", havingValue = "no-auth")
    public RestClient apertureRestClientNoAuth() {
        return new ApertureServiceNoAuthClient();
    }

    @Bean(name = "apertureRestClient")
    @ConditionalOnProperty(name = "aperture.service.client", havingValue = "one-way-ssl")
    public RestClient apertureRestClientOneWaySSL() {
        return new ApertureServiceOneWayClient();
    }
}
