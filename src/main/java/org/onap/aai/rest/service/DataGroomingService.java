/**
 * ============LICENSE_START=======================================================
 * org.onap.aai
 * ================================================================================
 * Copyright © 2025 Deutsche Telekom. All rights reserved.
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
import lombok.RequiredArgsConstructor;
import org.onap.aai.datagrooming.DataGrooming;
import org.onap.aai.introspection.LoaderFactory;
import org.onap.aai.rest.model.DataGroomingRequest;
import org.onap.aai.setup.SchemaVersions;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.PropertySource;
import org.springframework.scheduling.annotation.Async;
import org.springframework.scheduling.concurrent.ThreadPoolTaskExecutor;
import org.springframework.stereotype.Service;

import java.util.LinkedList;
import java.util.List;
import java.util.concurrent.Executor;

@Service
@PropertySource("file:${server.local.startpath}/etc/appprops/aaiconfig.properties")
@RequiredArgsConstructor
public class DataGroomingService {

    private static final Logger logger = LoggerFactory.getLogger(DataGroomingService.class);

    @Autowired
    private final ObjectMapper objectMapper;

    @Autowired
    final LoaderFactory loaderFactory;

    @Autowired
    final SchemaVersions schemaVersions;

    @Async("dataGroomingExecutor")
    public void executeAsync(DataGroomingRequest requestBody) throws JsonProcessingException {

        try {
            logger.info("Incoming JSON: {}", requestBody);

            String[] args = getArgsList(requestBody).toArray(new String[0]);

            DataGrooming tool = new DataGrooming(loaderFactory, schemaVersions);
            tool.execute(args);

        } catch (Exception e) {
            logger.error("Error:", e);
            throw e;
        }
    }

    private static List<String> getArgsList(DataGroomingRequest request) {
        List<String> argsList = new LinkedList<>();
        // boolean
        if (request.isAutoFix())
            argsList.add("-autoFix");
        if (request.isSkipHostCheck())
            argsList.add("-skipHostCheck");
        if (request.isDontFixOrphans())
            argsList.add("-dontFixOrphans");
        if (request.isEdgesOnly())
            argsList.add("-edgesOnly");
        if(request.isDupeFixOn())
            argsList.add("-dupeFixOn");
        if(request.isDupeCheckOff())
            argsList.add("-dupeCheckOff");
        if(request.isGhost2CheckOff())
            argsList.add("-ghost2CheckOff");
        if(request.isGhost2FixOn())
            argsList.add("-ghost2FixOn");
        if(request.isSkipEdgeChecks())
            argsList.add("-skipEdgeChecks");
        // rest of the fields
        if(null != request.getOldFileName() && !request.getOldFileName().isEmpty()){
            argsList.add("-f oldFileName");
            argsList.add(request.getOldFileName());
        }
        argsList.add("-maxFix");
        argsList.add(String.valueOf(request.getMaxRecordsToFix()));
        argsList.add("-sleepMinutes");
        argsList.add(String.valueOf(request.getSleepMinutes()));
        argsList.add("-timeWindowMinutes");
        argsList.add(String.valueOf(request.getTimeWindowMinutes()));
        return argsList;
    }


    @Bean(name = "dataGroomingExecutor")
    public Executor dataGroomingExecutor() {
        ThreadPoolTaskExecutor executor = new ThreadPoolTaskExecutor();
        executor.setCorePoolSize(4);
        executor.setMaxPoolSize(8);
        executor.setQueueCapacity(100);
        executor.setThreadNamePrefix("data-grooming-async-");
        executor.initialize();
        return executor;
    }
}
