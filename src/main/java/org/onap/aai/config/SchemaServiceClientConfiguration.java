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

import org.onap.aai.interceptors.AAIHeaderProperties;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;
import org.springframework.web.reactive.function.client.WebClient;

@Configuration
public class SchemaServiceClientConfiguration {

  @Value("${schema.service.base.url}")
  String schemaServiceBaseUrl;

  @Bean
  WebClient schemaClient() {
    // WebClient schemaClient(WebClient.Builder webClientBuilder) { // this is unfortunately not possible due to some context scanning issue
    return WebClient.builder()
      .baseUrl(schemaServiceBaseUrl)
      .defaultHeader(AAIHeaderProperties.FROM_APP_ID, "aai-graphadmin")
      .defaultHeader(AAIHeaderProperties.TRANSACTION_ID, "transaction-id")
      .build();
  }
}
