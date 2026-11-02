/**
 * ============LICENSE_START=======================================================
 * org.onap.aai
 * ================================================================================
 * Copyright (C) 2026 Deutsche Telekom AG.
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
package org.onap.aai.interceptors.pre;

import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertTrue;
import static org.mockito.ArgumentMatchers.eq;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;

import jakarta.servlet.http.HttpServletRequest;
import jakarta.ws.rs.container.ContainerRequestContext;
import jakarta.ws.rs.core.MultivaluedHashMap;
import jakarta.ws.rs.core.MultivaluedMap;
import java.io.ByteArrayInputStream;
import java.io.IOException;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.mockito.ArgumentCaptor;
import org.mockito.InjectMocks;
import org.mockito.Mock;
import org.mockito.MockitoAnnotations;
import org.onap.aai.interceptors.AAIHeaderProperties;

public class RequestTransactionLoggingTest {

    @InjectMocks
    private RequestTransactionLogging requestTransactionLogging;

    @Mock
    private HttpServletRequest httpServletRequest;

    @Mock
    private ContainerRequestContext requestContext;

    @BeforeEach
    void setUp() {
        MockitoAnnotations.openMocks(this);
    }

    @Test
    void loggedRequestMasksAuthorizationButKeepsOtherHeaders() throws IOException {
        MultivaluedMap<String, String> headers = new MultivaluedHashMap<>();
        headers.add("Authorization", "Basic dXNlcjpzZWNyZXQ=");
        headers.add("X-FromAppId", "test-app");
        headers.add("Accept", "application/json");
        headers.add("Content-Type", "application/json");
        when(requestContext.getHeaders()).thenReturn(headers);
        when(requestContext.getHeaderString("Accept")).thenReturn("application/json");
        when(requestContext.getHeaderString("Content-Type")).thenReturn("application/json");
        when(requestContext.getMethod()).thenReturn("PUT");
        when(requestContext.getEntityStream()).thenReturn(new ByteArrayInputStream(new byte[0]));

        requestTransactionLogging.filter(requestContext);

        ArgumentCaptor<String> loggedRequest = ArgumentCaptor.forClass(String.class);
        verify(requestContext).setProperty(eq(AAIHeaderProperties.AAI_REQUEST), loggedRequest.capture());
        assertFalse(loggedRequest.getValue().contains("dXNlcjpzZWNyZXQ="));
        assertTrue(loggedRequest.getValue().contains("Authorization=[***]"));
        assertTrue(loggedRequest.getValue().contains("X-FromAppId=[test-app]"));
    }
}
