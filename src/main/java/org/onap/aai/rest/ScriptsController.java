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
package org.onap.aai.rest;

import org.onap.aai.rest.model.DataGroomingRequest;
import org.onap.aai.rest.model.DupeToolRequest;
import org.onap.aai.rest.service.DataGroomingService;
import org.onap.aai.rest.service.DataGroomingSummaryService;
import org.onap.aai.rest.service.DupeToolService;
import org.onap.aai.rest.service.ReindexingToolService;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.context.annotation.PropertySource;
import org.springframework.http.ResponseEntity;
import org.springframework.scheduling.annotation.EnableAsync;
import org.springframework.web.bind.annotation.*;

import java.io.IOException;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.concurrent.CompletableFuture;

@RestController
@RequestMapping("/scripts")
@EnableAsync
@PropertySource("file:${schema.ingest.file:${server.local.startpath}/application.properties}")
public class ScriptsController {
    public static final String RESPONSE = "response";

    @Autowired
    private final DataGroomingService dataGroomingService;

    @Autowired
    private final DupeToolService dupeToolService;

    @Autowired
    private final ReindexingToolService reindexingToolService;

    @Autowired
    private final DataGroomingSummaryService dataGroomingSummaryService;

    private static final Logger logger = LoggerFactory.getLogger(ScriptsController.class.getSimpleName());

    @Value("${aai.datagrooming.summarypath}")
    private String filePath;

    public ScriptsController(DataGroomingService dataGroomingService, DupeToolService dupeToolService,
                             ReindexingToolService reindexingToolService, DataGroomingSummaryService dataGroomingSummaryService) {
        this.dataGroomingService = dataGroomingService;
        this.dupeToolService = dupeToolService;
        this.reindexingToolService = reindexingToolService;
        this.dataGroomingSummaryService = dataGroomingSummaryService;
    }

    @PostMapping("/grooming")
    public CompletableFuture<ResponseEntity<Map<String, String>>> runDataGrooming(@RequestBody DataGroomingRequest requestBody) {

        logger.info(">>> Inside runDataGrooming");
        try {
            dataGroomingService.executeAsync(requestBody);
            return CompletableFuture.completedFuture(ResponseEntity.accepted()
                    .body(Map.of(RESPONSE, "DataGrooming tool has started!")));
        }catch (Exception e){
            return CompletableFuture.failedFuture(e);
        }
    }

    @PostMapping("/dupes")
    public CompletableFuture<ResponseEntity<Map<String, String>>> runDupeTool(@RequestBody DupeToolRequest requestBody) {

        logger.info(">>> Inside runDupeToolForAllNodes");
        try {
            dupeToolService.executeAsync(requestBody);
            return CompletableFuture.completedFuture(ResponseEntity.accepted()
                    .body(Map.of(RESPONSE, "DupeTool tool has started!")));
        }catch (Exception e){
            return CompletableFuture.failedFuture(e);
        }

    }

    @PostMapping("/reindex")
    public ResponseEntity<Map<String, String>> runReindexing(@RequestBody String requestBody) {

        logger.info(">>> Inside runReindexing");

        reindexingToolService.execute(requestBody);
        return ResponseEntity.accepted()
                .body(Map.of(RESPONSE, "Reindexing started"));
    }

    @GetMapping("/indexes")
    public ResponseEntity<Map<String, Set<String>>> getIndexes() {
        logger.info(">>> inside getIndexes");

        Set<String> setOfIndexes = reindexingToolService.getListOfIndexes();
        return ResponseEntity.ok().body(Map.of("indexes", setOfIndexes));
    }

    @GetMapping("/grooming/summary/latest")
    public ResponseEntity<?> getLatestSummary() throws IOException {
        try {
            List<Map<String, Object>> summary = dataGroomingSummaryService.getLatestFileSummary();
            return ResponseEntity.ok(summary);
        } catch (IllegalStateException e) {
            // No files etc.
            return ResponseEntity.status(404).body(
                    Map.of("error", e.getMessage())
            );
        } catch (Exception e) {
            return ResponseEntity.internalServerError().body(
                    Map.of("error", e.getMessage())
            );
        }
    }


    @GetMapping("/grooming/files/present")
    public ResponseEntity<Map<String, Object>> checkIfFilesPresent() throws IOException {
        boolean present = dataGroomingSummaryService.hasGroomingFiles();

        return ResponseEntity.ok(
                Map.of(
                        "filesPresent", present,
                        "path", filePath
                )
        );
    }

}
