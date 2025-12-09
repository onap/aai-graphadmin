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

import lombok.RequiredArgsConstructor;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.stereotype.Service;

import java.io.BufferedReader;
import java.io.IOException;
import java.nio.charset.StandardCharsets;
import java.nio.file.*;
import java.util.*;
import java.util.regex.Matcher;
import java.util.regex.Pattern;
import java.util.stream.Stream;

@Service
@RequiredArgsConstructor
public class DataGroomingSummaryService {

    public static final Pattern PATTERN = Pattern.compile("last\\s+(\\d+)\\s+minutes");
    @Value("${aai.datagrooming.summarypath}")
    private String filePathProp;

    /**
     * Find files for the latest run:
     * - If latest is FULL  -> return FULL file(s) for that timestamp
     * - If latest is PARTIAL -> return all PARTIAL files for that timestamp
     */
    public List<Map<String, Object>> getLatestFileSummary() throws IOException {

        List<Path> latestFiles = findLatestRunFiles();

        if (latestFiles.isEmpty()) {
            throw new IllegalStateException(
                    "No dataGrooming FULL/PARTIAL files found in directory: " + getFilePath());
        }

        List<Map<String, Object>> summaries = new ArrayList<>();

        for (Path file : latestFiles) {
            Map<String, Object> summary = extractSummary(file);

            // always include fileName in the summary
            summary.put("fileName", file.getFileName().toString());

            summaries.add(summary);
        }

        return summaries;
    }

    private Path getFilePath(){
        Path filePath =  Path.of(filePathProp);
        return filePath;
    }

    private Map<String, Object> extractSummary(Path file) throws IOException {
        Map<String, Object> summaryMap = new LinkedHashMap<>();

        boolean summaryStarted = false;

        try (BufferedReader reader = Files.newBufferedReader(file, StandardCharsets.UTF_8)) {
            String line;
            while ((line = reader.readLine()) != null) {

                // detect summary section start
                if (line.contains("============ Summary ==============")) {
                    summaryStarted = true;
                    continue;
                }

                // If summary has started and we reach another section, stop
                if (summaryStarted && line.startsWith(" ------------- Delete Candidates")) {
                    break;
                }

                if (summaryStarted) {
                    String trimmed = line.trim();
                    if (!trimmed.isEmpty()) {
                        parseSummaryLine(trimmed, summaryMap);
                    }
                }
            }
        }

        return summaryMap;
    }

    private void parseSummaryLine(String line, Map<String, Object> summaryMap) {

        // Example:
        // Ran PARTIAL data grooming just looking at data added/updated in the last 10500 minutes.
        if (line.startsWith("Ran ") && line.contains("data grooming")) {
            if (line.contains("PARTIAL")) {
                summaryMap.put("runType", "PARTIAL");
            } else if (line.contains("FULL")) {
                summaryMap.put("runType", "FULL");
            }

            // Parse "last 10500 minutes"
            Matcher m = PATTERN.matcher(line);
            if (m.find()) {
                summaryMap.put("timeWindowMinutes", Integer.parseInt(m.group(1)));
            }
        }

        // Example (very long line):
        // Ran these nodeTypes: ,flavors,autonomous-system,...
        if (line.startsWith("Ran these nodeTypes:")) {
            String value = line.substring("Ran these nodeTypes:".length()).trim();
            String[] raw = value.split(",");
            List<String> nodeTypes = Arrays.stream(raw)
                    .map(String::trim)
                    .filter(s -> !s.isEmpty())
                    .toList();

            summaryMap.put("nodeTypesCount", nodeTypes.size());
            summaryMap.put("nodeTypes", nodeTypes);
        }

        // Metrics lines
        extractMetric(line, "delete candidates from previous run", "deleteCandidatesPreviousRun", summaryMap);
        extractMetric(line, "Deleted this many delete candidates", "deletedCandidates", summaryMap);
        extractMetric(line, "Ghost Nodes identified", "ghostNodes", summaryMap);
        extractMetric(line, "Orphan Nodes identified", "orphanNodes", summaryMap);
        extractMetric(line, "Missing aai-node-type Nodes identified", "missingNodeTypeNodes", summaryMap);
        extractMetric(line, "Bad Edges identified", "badEdges", summaryMap);
        extractMetric(line, "Bad aai-uri property Nodes identified", "badAaiUriNodes", summaryMap);
        extractMetric(line, "Bad index property Nodes identified", "badIndexPropertyNodes", summaryMap);
        extractMetric(line, "Duplicate Groups count", "duplicateGroups", summaryMap);
        extractMetric(line, "MisMatching Label/aai-node-type count", "mismatchingLabelNodeType", summaryMap);
        extractMetric(line, "Total number of nodes looked at", "totalNodesLookedAt", summaryMap);
    }

    private void extractMetric(String line, String marker, String key, Map<String, Object> map) {
        if (line.contains(marker) && line.contains("=")) {
            String afterEquals = line.substring(line.indexOf('=') + 1).trim();
            // afterEquals should now be something like "0" or "18"
            try {
                int value = Integer.parseInt(afterEquals.split("\\s+")[0]);
                map.put(key, value);
            } catch (NumberFormatException ignored) {
                // ignore bad formats
            }
        }
    }

    public boolean hasGroomingFiles() throws IOException {

        // Check if path exists & is directory
        if (!Files.exists(getFilePath()) || !Files.isDirectory(getFilePath())) {
            return false;
        }

        // Scan for dataGrooming files
        try (Stream<Path> stream = Files.list(getFilePath())) {
            return stream
                    .filter(Files::isRegularFile)
                    .map(path -> path.getFileName().toString())
                    .anyMatch(name -> name.startsWith("dataGrooming") && name.endsWith(".out"));
        }
    }



    /**
     * Find files belonging to the latest run:
     * - Considers dataGrooming.PARTIAL.YYYYMMDDHHMM.out
     *   and dataGrooming.FULL.YYYYMMDDHHMM.out
     * - Finds max timestamp across all
     * - If any FULL with that timestamp -> returns FULL file(s)
     * - Else returns all PARTIAL files with that timestamp
     */
    private List<Path> findLatestRunFiles() throws IOException {
        if (!Files.exists(getFilePath()) || !Files.isDirectory(getFilePath())) {
            return List.of();
        }

        List<FileWithTimestamp> files;
        try (Stream<Path> stream = Files.list(getFilePath())) {
            files = stream
                    .filter(Files::isRegularFile)
                    .map(Path::getFileName)
                    .map(Path::toString)
                    .filter(name -> name.startsWith("dataGrooming.")
                            && name.endsWith(".out"))
                    .map(name -> {
                        String type = extractType(name);      // FULL or PARTIAL
                        long ts = extractTimestamp(name);     // YYYYMMDDHHMM as long
                        return new FileWithTimestamp(name, ts, type);
                    })
                    .filter(f -> f.timestamp > 0L && f.type != null) // keep only valid
                    .toList();
        }

        if (files.isEmpty()) {
            return List.of();
        }

        // Find latest timestamp across all files
        long latestTs = files.stream()
                .mapToLong(f -> f.timestamp)
                .max()
                .orElseThrow();

        // All files with latest timestamp
        List<FileWithTimestamp> latest = files.stream()
                .filter(f -> f.timestamp == latestTs)
                .toList();

        // Prefer FULL if present at this timestamp, otherwise use PARTIAL
        boolean hasFull = latest.stream().anyMatch(f -> "FULL".equals(f.type));

        return latest.stream()
                .filter(f -> hasFull ? "FULL".equals(f.type) : "PARTIAL".equals(f.type))
                .map(f -> getFilePath().resolve(f.fileName))
                .sorted(Comparator.comparing(p -> p.getFileName().toString()))
                .toList();
    }

    /**
     * Extracts type from file name:
     * dataGrooming.PARTIAL.202512081310.out -> PARTIAL
     * dataGrooming.FULL.202512081310.out    -> FULL
     */
    private String extractType(String fileName) {
        try {
            String[] parts = fileName.split("\\.");
            // ["dataGrooming", "PARTIAL", "202512081310", "out"]
            if (parts.length >= 3) {
                return parts[1];
            }
        } catch (Exception ignored) {
        }
        return null;
    }

    /**
     * Extracts timestamp from file name:
     * dataGrooming.PARTIAL.202512081310.out -> 202512081310
     */
    private long extractTimestamp(String fileName) {
        try {
            String[] parts = fileName.split("\\.");
            // ["dataGrooming", "PARTIAL/FULL", "202512081310", "out"]
            if (parts.length >= 3) {
                return Long.parseLong(parts[2]);
            }
        } catch (Exception ignored) {
        }
        return 0L;
    }

    /**
     * Helper class to bind filename, timestamp and type (FULL/PARTIAL).
     */
    private static class FileWithTimestamp {
        final String fileName;
        final long timestamp;
        final String type;

        FileWithTimestamp(String fileName, long timestamp, String type) {
            this.fileName = fileName;
            this.timestamp = timestamp;
            this.type = type;
        }
    }
}
