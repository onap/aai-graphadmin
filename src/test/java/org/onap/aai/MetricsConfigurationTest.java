/**
 * ============LICENSE_START=======================================================
 * org.onap.aai
 * ================================================================================
 * Copyright © 2017-2018 AT&T Intellectual Property. All rights reserved.
 * ================================================================================
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 * <p>
 * http://www.apache.org/licenses/LICENSE-2.0
 * <p>
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 * ============LICENSE_END=========================================================
 */

package org.onap.aai;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertTrue;


import java.io.UnsupportedEncodingException;
import java.util.Base64;
import java.util.Collections;
import org.junit.Before;
import org.junit.BeforeClass;
import org.junit.ClassRule;
import org.junit.Rule;
import org.junit.Test;
import org.onap.aai.config.SpringContextAware;
import org.onap.aai.exceptions.AAIException;
import org.onap.aai.restclient.PropertyPasswordConfiguration;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.boot.actuate.autoconfigure.web.server.LocalManagementPort;
import org.springframework.boot.test.context.SpringBootTest;
import org.springframework.boot.test.context.SpringBootTest.WebEnvironment;
import org.springframework.boot.web.server.LocalServerPort;
import org.springframework.context.annotation.Import;
import org.springframework.http.*;
import org.springframework.test.context.ContextConfiguration;
import org.springframework.test.context.TestPropertySource;
import org.springframework.test.context.junit4.rules.SpringClassRule;
import org.springframework.test.context.junit4.rules.SpringMethodRule;
import org.springframework.web.client.RestTemplate;

/**
 * Test management endpoints against configuration resource.
 */
@SpringBootTest(webEnvironment = SpringBootTest.WebEnvironment.RANDOM_PORT,
    classes = {SpringContextAware.class, GraphAdminApp.class})
@ContextConfiguration(initializers = PropertyPasswordConfiguration.class, classes = {SpringContextAware.class})
@Import(GraphAdminTestConfiguration.class)
@TestPropertySource(locations = "classpath:application-test.properties")
public class MetricsConfigurationTest {

    @ClassRule
    public static final SpringClassRule springClassRule = new SpringClassRule();

    @Rule
    public final SpringMethodRule springMethodRule = new SpringMethodRule();

    @Autowired
    RestTemplate restTemplate;

    @LocalServerPort
    int randomPort;

    @LocalManagementPort
    private long localManagementPort;

    private HttpEntity<String> httpEntity;
    private HttpEntity<String> httpEntityPut;
    private HttpEntity<String> httpEntityPatch;
    private String baseUrl;
    private String actuatorUrl;
    private HttpHeaders headers;

    @BeforeClass
    public static void setupConfig() throws AAIException {
        System.setProperty("AJSC_HOME", "./");
        System.setProperty("BUNDLECONFIG_DIR", "src/main/resources/");
    }

    @Before
    public void setup() throws UnsupportedEncodingException {

        headers = new HttpHeaders();

        headers.setAccept(Collections.singletonList(MediaType.APPLICATION_JSON));
        headers.setContentType(MediaType.APPLICATION_JSON);
        headers.add("Real-Time", "true");
        headers.add("X-FromAppId", "JUNIT");
        headers.add("X-TransactionId", "JUNIT");

        String authorization = Base64.getEncoder().encodeToString("AAI:AAI".getBytes("UTF-8"));
        headers.add("Authorization", "Basic " + authorization);

        httpEntity = new HttpEntity<String>(headers);
        baseUrl = "http://localhost:" + randomPort;
        actuatorUrl = "http://localhost:" + localManagementPort;
    }


    @Test
    public void testManagementEndpointConfiguration() {
        ResponseEntity responseEntity = null;
        String responseBody = null;

        //set Accept as text/plain in order to get access of endpoint "/actuator/prometheus"
        headers.set("Accept", "text/plain");
        httpEntity = new HttpEntity<String>(headers);
        responseEntity =
            restTemplate.exchange(actuatorUrl + "/actuator/prometheus", HttpMethod.GET, httpEntity, String.class);
        responseBody = (String) responseEntity.getBody();
        assertEquals(HttpStatus.OK, responseEntity.getStatusCode());

        //Set Accept as MediaType.APPLICATION_JSON in order to get access of endpoint "/actuator/info" and "/actuator/health"
        headers.setAccept(Collections.singletonList(MediaType.APPLICATION_JSON));
        httpEntity = new HttpEntity<String>(headers);
        responseEntity =
            restTemplate.exchange(actuatorUrl + "/actuator/info", HttpMethod.GET, httpEntity, String.class);
        responseBody = (String) responseEntity.getBody();
        assertEquals(HttpStatus.OK, responseEntity.getStatusCode());

        responseEntity =
            restTemplate.exchange(actuatorUrl + "/actuator/health", HttpMethod.GET, httpEntity, String.class);
        responseBody = (String) responseEntity.getBody();
        assertEquals(HttpStatus.OK, responseEntity.getStatusCode());
        assertTrue(responseBody.contains("UP"));
    }
}
