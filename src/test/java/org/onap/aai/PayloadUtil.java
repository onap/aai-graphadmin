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

import org.apache.commons.io.IOUtils;

import java.io.IOException;

import static org.junit.jupiter.api.Assertions.assertNotNull;
import java.io.InputStream;
import java.util.HashMap;
import java.util.Map;
import java.util.regex.Matcher;
import java.util.regex.Pattern;

public class PayloadUtil {

    private static final Map<String, String> cache = new HashMap<>();
    private static final Pattern TEMPLATE_PATTERN = Pattern.compile("\\$\\{[^}]+\\}");

    public static String getExpectedPayload(String fileName) throws IOException {

        InputStream inputStream = PayloadUtil.class.getClassLoader().getResourceAsStream("payloads/expected/" + fileName);

        String message = "Unable to find the %s in src/test/resources".formatted(fileName);
        assertNotNull(inputStream, message);

        String resource = IOUtils.toString(inputStream);

        inputStream.close();
        return resource;
    }

    public static String getResourcePayload(String fileName) throws IOException {

        InputStream inputStream = PayloadUtil.class.getClassLoader().getResourceAsStream("payloads/resource/" + fileName);

        String message = "Unable to find the %s in src/test/resources".formatted(fileName);
        assertNotNull(inputStream, message);

        String resource = IOUtils.toString(inputStream);

        inputStream.close();
        return resource;
    }

    public static String getTemplatePayload(String fileName, Map<String, String> templateValueMap) throws Exception {

        InputStream inputStream = PayloadUtil.class.getClassLoader().getResourceAsStream("payloads/templates/" + fileName);

        String message = "Unable to find the %s in src/test/resources".formatted(fileName);
        assertNotNull(inputStream, message);

        String resource;

        if(cache.containsKey(fileName)){
            resource = cache.get(fileName);
        } else {
            resource = IOUtils.toString(inputStream);
            cache.put(fileName, resource);
        }

        Matcher matcher = TEMPLATE_PATTERN.matcher(resource);

        String resourceWithTemplateValues = resource;

        while(matcher.find()){
            int start = matcher.start() + 2;
            int end = matcher.end() - 1;
            String key = resource.substring(start, end);
            if(templateValueMap.containsKey(key)){
                resourceWithTemplateValues = resourceWithTemplateValues.replaceAll("\\$\\{" + key +"\\}", templateValueMap.get(key));
            } else {
                throw new RuntimeException("Unable to find the key value pair in map for the template processing for key " + key);
            }
        }

        inputStream.close();
        return resourceWithTemplateValues;
    }
    
    public static String getNamedQueryPayload(String fileName) throws IOException {

        InputStream inputStream = PayloadUtil.class.getClassLoader().getResourceAsStream("payloads/named-queries/" + fileName);

        String message = "Unable to find the %s in src/test/resources/payloads/named-queries".formatted(fileName);
        assertNotNull(inputStream, message);

        String resource = IOUtils.toString(inputStream);

        inputStream.close();
        return resource;
    }
}
