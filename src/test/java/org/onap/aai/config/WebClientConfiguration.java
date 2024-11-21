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

package org.onap.aai.config;

import java.time.Duration;
import java.util.Collections;

import org.onap.aai.setup.SchemaVersions;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.boot.test.context.TestConfiguration;
import org.springframework.boot.web.server.LocalServerPort;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Lazy;
import org.springframework.context.annotation.Primary;
import org.springframework.http.MediaType;
import org.springframework.test.web.reactive.server.WebTestClient;

@TestConfiguration
public class WebClientConfiguration {

  @Lazy
  @Bean
  WebTestClient mgmtClient(@Value("${local.management.port}") int port) {
    return WebTestClient.bindToServer()
      .baseUrl("http://localhost:" + port)
      .responseTimeout(Duration.ofSeconds(300))
      .defaultHeaders(headers -> {
        headers.setAccept(Collections.singletonList(MediaType.APPLICATION_JSON));
      })
      .build();
  }

  @Lazy
  @Bean
  @Primary
  WebTestClient appClient(@Value("${server.port}") int port) {
    return WebTestClient.bindToServer()
      .baseUrl("http://localhost:" + port)
      .responseTimeout(Duration.ofSeconds(300))
      .defaultHeaders(headers -> {
        headers.setAccept(Collections.singletonList(MediaType.APPLICATION_JSON));
      })
      .build();
  }
}
