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
package org.onap.aai.rest.model;

import com.fasterxml.jackson.annotation.JsonProperty;
import org.onap.aai.util.GraphAdminConstants;

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

    public String getOldFileName() {
        return oldFileName;
    }

    public void setOldFileName(String oldFileName) {
        this.oldFileName = oldFileName;
    }

    public boolean isAutoFix() {
        return autoFix;
    }

    public void setAutoFix(boolean autoFix) {
        this.autoFix = autoFix;
    }

    public int getSleepMinutes() {
        return sleepMinutes;
    }

    public void setSleepMinutes(int sleepMinutes) {
        this.sleepMinutes = sleepMinutes;
    }

    public boolean isEdgesOnly() {
        return edgesOnly;
    }

    public void setEdgesOnly(boolean edgesOnly) {
        this.edgesOnly = edgesOnly;
    }

    public boolean isSkipEdgeChecks() {
        return skipEdgeChecks;
    }

    public void setSkipEdgeChecks(boolean skipEdgeChecks) {
        this.skipEdgeChecks = skipEdgeChecks;
    }

    public int getTimeWindowMinutes() {
        return timeWindowMinutes;
    }

    public void setTimeWindowMinutes(int timeWindowMinutes) {
        this.timeWindowMinutes = timeWindowMinutes;
    }

    public boolean isDontFixOrphans() {
        return dontFixOrphans;
    }

    public void setDontFixOrphans(boolean dontFixOrphans) {
        this.dontFixOrphans = dontFixOrphans;
    }

    public int getMaxRecordsToFix() {
        return maxRecordsToFix;
    }

    public void setMaxRecordsToFix(int maxRecordsToFix) {
        this.maxRecordsToFix = maxRecordsToFix;
    }

    public boolean isSkipHostCheck() {
        return skipHostCheck;
    }

    public void setSkipHostCheck(boolean skipHostCheck) {
        this.skipHostCheck = skipHostCheck;
    }

    public boolean isDupeCheckOff() {
        return dupeCheckOff;
    }

    public void setDupeCheckOff(boolean dupeCheckOff) {
        this.dupeCheckOff = dupeCheckOff;
    }

    public boolean isDupeFixOn() {
        return dupeFixOn;
    }

    public void setDupeFixOn(boolean dupeFixOn) {
        this.dupeFixOn = dupeFixOn;
    }

    public boolean isGhost2CheckOff() {
        return ghost2CheckOff;
    }

    public void setGhost2CheckOff(boolean ghost2CheckOff) {
        this.ghost2CheckOff = ghost2CheckOff;
    }

    public boolean isGhost2FixOn() {
        return ghost2FixOn;
    }

    public void setGhost2FixOn(boolean ghost2FixOn) {
        this.ghost2FixOn = ghost2FixOn;
    }

}
