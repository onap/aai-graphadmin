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
package org.onap.aai;

import com.att.eelf.configuration.EELFLogger;
import com.att.eelf.configuration.EELFManager;
import org.apache.http.client.HttpClient;
import org.apache.http.impl.client.HttpClients;
import org.apache.http.ssl.SSLContextBuilder;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.boot.test.context.TestConfiguration;
import org.springframework.boot.web.client.RestTemplateBuilder;
import org.springframework.context.annotation.Bean;
import org.springframework.core.env.Environment;
import org.springframework.http.HttpStatus;
import org.springframework.http.client.ClientHttpResponse;
import org.springframework.http.client.HttpComponentsClientHttpRequestFactory;
import org.springframework.util.ResourceUtils;
import org.springframework.web.client.ResponseErrorHandler;
import org.springframework.web.client.RestTemplate;

import javax.net.ssl.SSLContext;
import java.io.File;
import java.io.FileInputStream;
import java.io.IOException;
import java.io.InputStream;
import java.security.KeyStore;

@TestConfiguration
public class GraphAdminTestConfiguration {

    private static final EELFLogger logger = EELFManager.getInstance().getLogger(GraphAdminTestConfiguration.class);

    @Autowired
    private Environment env;

    /**
     * Create a RestTemplate bean, using the RestTemplateBuilder provided
     * by the auto-configuration.
     */
    @Bean
    RestTemplate restTemplate(RestTemplateBuilder builder) throws Exception {

        char[] trustStorePassword = env.getProperty("server.ssl.trust-store-password").toCharArray();
        char[] keyStorePassword   = env.getProperty("server.ssl.key-store-password").toCharArray();

        String keyStore = env.getProperty("server.ssl.key-store");
        String trustStore = env.getProperty("server.ssl.trust-store");

        SSLContextBuilder sslContextBuilder = SSLContextBuilder.create();

        if(env.acceptsProfiles("two-way-ssl")){
            sslContextBuilder = sslContextBuilder.loadKeyMaterial(loadPfx(keyStore, keyStorePassword), keyStorePassword);
        }

        SSLContext sslContext = sslContextBuilder
                .loadTrustMaterial(ResourceUtils.getFile(trustStore), trustStorePassword)
                .build();

        HttpClient client = HttpClients.custom()
                .setSSLContext(sslContext)
                .setSSLHostnameVerifier((s, sslSession) -> true)
                .build();

        RestTemplate restTemplate =  builder
                .requestFactory(new HttpComponentsClientHttpRequestFactory(client))
                .build();

        restTemplate.setErrorHandler(new ResponseErrorHandler() {
            @Override
            public boolean hasError(ClientHttpResponse clientHttpResponse) throws IOException {
                if (clientHttpResponse.getStatusCode() != HttpStatus.OK) {

                    logger.debug("Status code: " + clientHttpResponse.getStatusCode());

                    if (clientHttpResponse.getStatusCode() == HttpStatus.FORBIDDEN) {
                        logger.debug("Call returned a error 403 forbidden resposne ");
                        return true;
                    }

                    if(clientHttpResponse.getRawStatusCode() % 100 == 5){
                        logger.debug("Call returned a error " + clientHttpResponse.getStatusText());
                        return true;
                    }
                }

                return false;
            }

            @Override
            public void handleError(ClientHttpResponse clientHttpResponse) throws IOException {
            }
        });

        return restTemplate;
    }

    private KeyStore loadPfx(String file, char[] password) throws Exception {
        KeyStore keyStore = KeyStore.getInstance("PKCS12");
        File key = ResourceUtils.getFile(file);
        try (InputStream in = new FileInputStream(key)) {
            keyStore.load(in, password);
        }
        return keyStore;
    }
}
