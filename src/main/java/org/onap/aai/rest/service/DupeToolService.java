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

import com.fasterxml.jackson.core.JsonProcessingException;
import com.fasterxml.jackson.databind.ObjectMapper;
import jakarta.validation.ValidationException;
import org.onap.aai.dbgen.DupeTool;
import org.onap.aai.exceptions.AAIException;
import org.onap.aai.introspection.LoaderFactory;
import org.onap.aai.rest.model.DupeToolRequest;
import org.onap.aai.setup.SchemaVersions;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.context.annotation.Bean;
import org.springframework.scheduling.annotation.Async;
import org.springframework.scheduling.concurrent.ThreadPoolTaskExecutor;
import org.springframework.stereotype.Service;

import java.util.LinkedList;
import java.util.List;
import java.util.concurrent.Executor;

@Service
public class DupeToolService {

    private static final Logger logger = LoggerFactory.getLogger(DupeToolService.class);

    @Autowired
    private ObjectMapper objectMapper;

    @Autowired
    LoaderFactory loaderFactory;

    @Autowired
    SchemaVersions schemaVersions;

    @Async("dupeExecutor")
    public void executeAsync(String requestBody) throws AAIException, JsonProcessingException {

        try {
            logger.info("Incoming JSON: {}", requestBody);

            DupeToolRequest request = objectMapper.readValue(requestBody, DupeToolRequest.class);
            validateRequest(request);

            String[] args = getArgsList(request).toArray(new String[0]);

            DupeTool tool = new DupeTool(loaderFactory, schemaVersions);
            tool.execute(args);


        } catch (Exception e) {
            logger.error("Error:", e);
            throw e;
        }
    }


    private static List<String> getArgsList(DupeToolRequest request) {
        List<String> argsList = new LinkedList<>();
        // boolean
        if (request.isDoAutoFix())
            argsList.add("-autoFix");
        if (request.getSkipHostCheck())
            argsList.add("-skipHostCheck");
        if (request.isSpecialTenantRule())
            argsList.add("-specialTenantRule");
        if (request.isForAllNodeTypes())
            argsList.add("-allNodeTypes");
        else{
            argsList.add("-nodeTypes");
            String[] nodeTypesList = request.getNodeTypes();
            argsList.add(String.join(",", nodeTypesList));
        }
        // rest of the fields
        argsList.add("-filterParams");
        argsList.add(request.getFilterParams());
        argsList.add("-maxFix");
        argsList.add(String.valueOf(request.getMaxRecordsToFix()));
        argsList.add("-sleepMinutes");
        argsList.add(String.valueOf(request.getSleepMinutes()));
        argsList.add("-timeWindowMinutes");
        argsList.add(String.valueOf(request.getTimeWindowMinutes()));
        argsList.add("-userId");
        argsList.add(String.valueOf(request.getUserId()));
        return argsList;
    }

    private void validateRequest(DupeToolRequest req) {
        boolean hasNodeType = req.getNodeTypes() != null && req.getNodeTypes().length>0;
        boolean hasAllNodesFlag = req.isForAllNodeTypes();
        if (!hasNodeType && !hasAllNodesFlag) {
            throw new ValidationException("Either nodeType must be provided OR forAllNodeTypes must be true");
        }
        if (hasNodeType && hasAllNodesFlag) {
            throw new ValidationException("Both nodeType and forAllNodeTypes cannot be provided together");
        }
        if (req.getUserId() == null || req.getUserId().isEmpty()) {
            throw new ValidationException("userId is required");
        }
        if (req.getMaxRecordsToFix() <= 0) {
            throw new ValidationException("maxRecordsToFix must be > 0");
        }
    }

    @Bean(name = "dupeExecutor")
    public Executor dupeExecutor() {
        ThreadPoolTaskExecutor executor = new ThreadPoolTaskExecutor();
        executor.setCorePoolSize(4);
        executor.setMaxPoolSize(8);
        executor.setQueueCapacity(100);
        executor.setThreadNamePrefix("dupe-async-");
        executor.initialize();
        return executor;
    }
}

