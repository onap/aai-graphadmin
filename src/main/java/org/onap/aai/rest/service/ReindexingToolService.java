/**
 * ============LICENSE_START=======================================================
 * org.onap.aai
 * ================================================================================
 * Copyright © Deutsche Telekom. All rights reserved.
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
package org.onap.aai.rest.service;

import com.fasterxml.jackson.databind.ObjectMapper;
import jakarta.validation.ValidationException;
import org.onap.aai.dbgen.ReindexingTool;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.context.annotation.Bean;
import org.springframework.scheduling.annotation.Async;
import org.springframework.scheduling.concurrent.ThreadPoolTaskExecutor;
import org.springframework.stereotype.Service;

import java.util.*;
import java.util.concurrent.Executor;

@Service
public class ReindexingToolService {

    private static final Logger logger = LoggerFactory.getLogger(ReindexingToolService.class);

    @Autowired
    private ObjectMapper objectMapper;

    @Async("reindexingExecutor")
    public void execute(String requestBody) {

        try {
            logger.info("Incoming JSON: {}", requestBody);
            Map<String, Object> requestMap = objectMapper.readValue(requestBody, Map.class);
            validateRequest(requestMap);

            String[] args = getArgsList(requestMap).toArray(new String[0]);

            ReindexingTool reindexingTool = new ReindexingTool();
            reindexingTool.execute(args);

        } catch (Exception e) {
            logger.error("Error:", e);
        }
    }

    public Set<String> getListOfIndexes(){
        ReindexingTool reindexingTool = new ReindexingTool();
        return reindexingTool.getListOfIndexes();
    }

    private void validateRequest(Map<String, Object> requestMap) {
        if(!requestMap.containsKey("indexNames"))
            throw new ValidationException("indexNames must be provided, either one or more(comma separated)!");
    }

    private static List<String> getArgsList(Map<String,Object> request) {
        List<String> argsList = new LinkedList<>();

        argsList.add("-indexNames");
//        argsList.add(request.get("indexNames").toString());
        String indexStr = null;
        Object indexNamesObject = request.get("indexNames");
        if (indexNamesObject instanceof List) {
            List<String> indexNamesList = (List<String>) indexNamesObject;

            indexStr = String.join(",", indexNamesList);
        }
        argsList.add(indexStr);

        return argsList;
    }

    @Bean(name = "reindexingExecutor")
    public Executor reindexingExecutor() {
        ThreadPoolTaskExecutor executor = new ThreadPoolTaskExecutor();
        executor.setCorePoolSize(4);
        executor.setMaxPoolSize(8);
        executor.setQueueCapacity(100);
        executor.setThreadNamePrefix("reindexing-async-");
        executor.initialize();
        return executor;
    }
}
