/**
 * ============LICENSE_START=======================================================
 * org.onap.aai
 * ================================================================================
 * Copyright © 2024 Deutsche Telekom. All rights reserved.
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

import static org.mockito.Mockito.when;

import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.onap.aai.config.WebClientConfiguration;
import org.onap.aai.restclient.PropertyPasswordConfiguration;
import org.onap.aai.service.SchemaJobStatusService;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.boot.test.context.SpringBootTest;
import org.springframework.boot.test.context.SpringBootTest.WebEnvironment;
import org.springframework.boot.test.mock.mockito.MockBean;
import org.springframework.context.annotation.Import;
import org.springframework.test.context.ContextConfiguration;
import org.springframework.test.web.reactive.server.WebTestClient;


@Import(WebClientConfiguration.class)
@SpringBootTest(webEnvironment = WebEnvironment.RANDOM_PORT)
@ContextConfiguration(initializers = PropertyPasswordConfiguration.class)
public class SchemaJobStatusControllerTest {

    @MockBean 
    private SchemaJobStatusService schemaJobStatusService;

    @Autowired
    private SchemaJobStatusController schemaJobStatusController;

    @Autowired
    WebTestClient webClient;

    @BeforeEach
    void setUp() {
        // Bind the WebTestClient to the controller
        webClient = WebTestClient.bindToController(schemaJobStatusController).build();
    }

    @Test
    void testIsSchemaInitializedTrue() throws Exception {
        when(schemaJobStatusService.isSchemaInitialized()).thenReturn(true);
       
        webClient.get()
               .uri("/isSchemaInitialized")
               .exchange()
               .expectStatus()
               .isOk()
               .expectBody(Boolean.class).isEqualTo(true);
    }

    @Test
    void testIsSchemaInitializedFalse() throws Exception {
        when(schemaJobStatusService.isSchemaInitialized()).thenReturn(false);
       
        webClient.get()
               .uri("/isSchemaInitialized")
               .exchange()
               .expectStatus()
               .isOk()
               .expectBody(Boolean.class).isEqualTo(false);
    }

    @Test
    void testInvalidEndpoint() {
        webClient.get()
                .uri("/nonexistentEndpoint")
                .exchange()
                .expectStatus()
                .isNotFound();
    }
}