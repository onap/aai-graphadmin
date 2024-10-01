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

package org.onap.aai.rest.util;

import static org.mockito.Mockito.when;

import org.junit.jupiter.api.Test;
import org.onap.aai.config.WebClientConfiguration;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.beans.factory.annotation.Qualifier;
import org.springframework.boot.test.context.SpringBootTest;
import org.springframework.boot.test.context.SpringBootTest.WebEnvironment;
import org.springframework.boot.test.mock.mockito.MockBean;
import org.springframework.context.annotation.Import;
import org.springframework.test.annotation.DirtiesContext;
import org.springframework.test.context.ActiveProfiles;
import org.springframework.test.context.TestPropertySource;
import org.springframework.test.web.reactive.server.WebTestClient;


@Import(WebClientConfiguration.class)
@SpringBootTest(webEnvironment = WebEnvironment.DEFINED_PORT, properties = "server.port=8080")
@TestPropertySource(properties = {"aai.actuator.echo.enabled=true",
		 							"server.ssl.enabled=false"})
public class EchoHealthIndicatorTest {

  @Autowired
  @Qualifier("mgmtClient")
  WebTestClient webClient;

  @MockBean private AaiGraphChecker aaiGraphChecker;

  @Test
  public void thatActuatorCheckIsHealthy() {
    when(aaiGraphChecker.isAaiGraphDbAvailable()).thenReturn(true);

    webClient.get()
      .uri("/actuator/health")
      .exchange()
      .expectStatus()
      .isOk();
  }

  @Test
  public void thatActuatorCheckIsUnhealthy() {
    when(aaiGraphChecker.isAaiGraphDbAvailable()).thenReturn(false);

    webClient.get()
      .uri("/actuator/health")
      .exchange()
      .expectStatus()
      .is5xxServerError();
  }
}
