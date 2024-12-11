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
package org.onap.aai.schema;

import static com.github.tomakehurst.wiremock.client.WireMock.aResponse;
import static com.github.tomakehurst.wiremock.client.WireMock.get;
import static com.github.tomakehurst.wiremock.client.WireMock.matching;
import static com.github.tomakehurst.wiremock.client.WireMock.stubFor;
import static com.github.tomakehurst.wiremock.client.WireMock.urlEqualTo;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertNotNull;

import org.junit.jupiter.api.Test;
import org.onap.aai.config.SchemaServiceClientConfiguration;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.boot.test.context.SpringBootTest;
import org.springframework.cloud.contract.wiremock.AutoConfigureWireMock;
import org.springframework.http.HttpHeaders;
import org.springframework.http.MediaType;
import org.springframework.web.reactive.function.client.WebClient;

@SpringBootTest(classes = {SchemaChecksumService.class, SchemaServiceClientConfiguration.class, WebClient.class})
@AutoConfigureWireMock(port = 0)
public class SchemaChecksumServiceTest {

  @Value("${wiremock.server.port}")
  private int wiremockPort;

  @Autowired
  SchemaChecksumService schemaChecksumService;

  @Test
  void thatChecksumsCanBeRetrieved() {
    stubFor(get(urlEqualTo("/aai/schema-service/v1/nodes/checksums"))
        .withHeader("X-FromAppId", matching(".*"))
        .withHeader("X-TransactionId", matching(".*"))
        .willReturn(aResponse()
            .withHeader(HttpHeaders.CONTENT_TYPE, MediaType.APPLICATION_JSON_VALUE)
            .withBodyFile("checksumResponse.json")));
    ChecksumResponse checksumResponse = schemaChecksumService.getChecksums();
    assertNotNull(checksumResponse);
    assertEquals(1109582923, checksumResponse.getChecksumMap().get("v29"));
  }
}
