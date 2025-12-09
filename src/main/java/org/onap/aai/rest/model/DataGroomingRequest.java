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
package org.onap.aai.rest.model;

import com.fasterxml.jackson.annotation.JsonProperty;

import lombok.Data;

import org.onap.aai.util.GraphAdminConstants;

@Data
public class DataGroomingRequest {

    @JsonProperty("oldFileName")
    private String oldFileName;
    @JsonProperty("autoFix")
    private boolean autoFix;
    @JsonProperty("sleepMinutes")
    private int sleepMinutes = GraphAdminConstants.AAI_GROOMING_DEFAULT_SLEEP_MINUTES;
    @JsonProperty("edgesOnly")
    private boolean edgesOnly;
    @JsonProperty("skipEdgeChecks")
    private boolean skipEdgeChecks;
    @JsonProperty("timeWindowMinutes")
    private int timeWindowMinutes = 0;
    @JsonProperty("dontFixOrphans")
    private boolean dontFixOrphans;
    @JsonProperty("maxFix")
    private int maxRecordsToFix = GraphAdminConstants.AAI_GROOMING_DEFAULT_MAX_FIX;
    @JsonProperty("skipHostCheck")
    private boolean skipHostCheck = false;
    @JsonProperty("dupeCheckOff")
    private boolean dupeCheckOff;
    @JsonProperty("dupeFixOn")
    private boolean dupeFixOn;
    @JsonProperty("ghost2CheckOff")
    private boolean ghost2CheckOff;
    @JsonProperty("ghost2FixOn")
    private boolean ghost2FixOn;

}
