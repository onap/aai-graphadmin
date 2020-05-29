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

import org.onap.aai.restclient.OneWaySSLRestClient;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.http.HttpHeaders;
import org.springframework.http.MediaType;
import org.springframework.http.client.HttpComponentsClientHttpRequestFactory;
import org.springframework.util.MultiValueMap;

import java.util.Collections;
import java.util.Map;
import java.util.UUID;

public class ApertureServiceOneWayClient extends OneWaySSLRestClient {

    @Value("${aperture.service.base.url}")
    private String baseUrl;

    @Value("${aperture.service.ssl.trust-store}")
    private String truststorePath;

    @Value("${aperture.service.ssl.trust-store-password}")
    private String truststorePassword;

    @Value("${aperture.service.timeout-in-milliseconds}")
    private Integer timeout;

    @Override
    public String getBaseUrl() {
        return baseUrl;
    }

    @Override
    protected String getTruststorePath() {
        return truststorePath;
    }

    @Override
    protected char[] getTruststorePassword() {
        return truststorePassword.toCharArray();
    }

    @Override
    protected HttpComponentsClientHttpRequestFactory getHttpRequestFactory() throws Exception {
        HttpComponentsClientHttpRequestFactory requestFactory = super.getHttpRequestFactory();
        requestFactory.setConnectionRequestTimeout(timeout);
        requestFactory.setReadTimeout(timeout);
        requestFactory.setConnectTimeout(timeout);
        return requestFactory;
    }

    @Override
    public MultiValueMap<String, String> getHeaders(Map<String, String> headers) {
        HttpHeaders httpHeaders = new HttpHeaders();

        String defaultAccept = headers.getOrDefault(HttpHeaders.ACCEPT, MediaType.APPLICATION_JSON.toString());
        String defaultContentType =
            headers.getOrDefault(HttpHeaders.CONTENT_TYPE, MediaType.APPLICATION_JSON.toString());

        if (headers.isEmpty()) {
            httpHeaders.setAccept(Collections.singletonList(MediaType.parseMediaType(defaultAccept)));
            httpHeaders.setContentType(MediaType.parseMediaType(defaultContentType));
        }

        httpHeaders.add("X-FromAppId", appName);
        httpHeaders.add("X-TransactionId", UUID.randomUUID().toString());
        httpHeaders.add("X-TransactionId", appName);
        headers.forEach(httpHeaders::add);
        return httpHeaders;
    }

}
