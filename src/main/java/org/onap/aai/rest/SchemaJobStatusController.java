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
package org.onap.aai.rest;

import javax.ws.rs.Consumes;
import javax.ws.rs.Produces;
import javax.ws.rs.core.MediaType;

import org.onap.aai.exceptions.AAIException;
import org.onap.aai.restcore.RESTAPI;
import org.onap.aai.service.SchemaJobStatusService;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.web.bind.annotation.GetMapping;
import org.springframework.web.bind.annotation.RequestMapping;
import org.springframework.web.bind.annotation.RestController;


@RestController
@RequestMapping("/aai")
public class SchemaJobStatusController extends RESTAPI {

    private static final Logger LOGGER = LoggerFactory.getLogger(SchemaJobStatusController.class);

    private final SchemaJobStatusService schemaJobStatusService;

    public SchemaJobStatusController(SchemaJobStatusService schemaJobStatusService) {
        this.schemaJobStatusService = schemaJobStatusService;
    }

    @GetMapping("/isSchemaInitialized")
    @Consumes({ MediaType.APPLICATION_JSON, MediaType.APPLICATION_XML })
    @Produces({ MediaType.APPLICATION_JSON, MediaType.APPLICATION_XML })
    public boolean isSchemaInitialized() throws AAIException {

        LOGGER.info("Checking if schema is initialized.");
        return schemaJobStatusService.isSchemaInitialized();
    }

    
}