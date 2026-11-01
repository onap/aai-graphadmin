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
package org.onap.aai.interceptors;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertTrue;

import jakarta.ws.rs.core.MediaType;
import jakarta.ws.rs.core.MultivaluedHashMap;
import jakarta.ws.rs.core.MultivaluedMap;
import org.junit.jupiter.api.Test;

public class AAIContainerFilterTest {

    private final AAIContainerFilter filter = new AAIContainerFilter() {};

    @Test
    void headersForLogMasksCredentialHeadersCaseInsensitively() {
        MultivaluedMap<String, String> headers = new MultivaluedHashMap<>();
        headers.add("Authorization", "Basic dXNlcjpzZWNyZXQ=");
        headers.add("proxy-authorization", "Basic cHJveHk6c2VjcmV0");
        headers.add("COOKIE", "JSESSIONID=abc123");
        headers.add("X-FromAppId", "test-app");

        String logged = filter.headersForLog(headers);

        assertFalse(logged.contains("dXNlcjpzZWNyZXQ="));
        assertFalse(logged.contains("cHJveHk6c2VjcmV0"));
        assertFalse(logged.contains("abc123"));
        assertTrue(logged.contains("Authorization=[***]"));
        assertTrue(logged.contains("proxy-authorization=[***]"));
        assertTrue(logged.contains("COOKIE=[***]"));
        assertTrue(logged.contains("X-FromAppId=[test-app]"));
    }

    @Test
    void headersForLogMasksSetCookieInResponseHeaders() {
        MultivaluedMap<String, Object> headers = new MultivaluedHashMap<>();
        headers.add("Set-Cookie", "JSESSIONID=abc123");
        headers.add("Content-Type", MediaType.APPLICATION_JSON_TYPE);

        String logged = filter.headersForLog(headers);

        assertFalse(logged.contains("abc123"));
        assertTrue(logged.contains("Set-Cookie=[***]"));
        assertTrue(logged.contains("Content-Type=[application/json]"));
    }

    @Test
    void headersForLogKeepsFormatOfNonSensitiveHeaders() {
        MultivaluedMap<String, String> headers = new MultivaluedHashMap<>();
        headers.add("X-TransactionId", "tx-1");
        headers.add("Accept", "application/json");
        headers.add("Accept", "application/xml");

        assertEquals(headers.toString(), filter.headersForLog(headers));
    }
}
