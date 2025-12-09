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

import java.util.List;

public class DupeToolRequest {

    @JsonProperty("autoFix")
    private boolean doAutoFix = false;
    @JsonProperty("maxFix")
    private int maxRecordsToFix = GraphAdminConstants.AAI_GROOMING_DEFAULT_MAX_FIX;
    @JsonProperty("sleepMinutes")
    private int sleepMinutes = GraphAdminConstants.AAI_GROOMING_DEFAULT_SLEEP_MINUTES;
    @JsonProperty("userId")
    private String userId = "amd8383";
    @JsonProperty("nodeTypes")
    private String[] nodeTypes ;
    @JsonProperty("timeWindowMinutes")
    private int timeWindowMinutes = 0;
    @JsonProperty("skipHostCheck")
    private boolean skipHostCheck = false;
    @JsonProperty("specialTenantRule")
    private boolean specialTenantRule = false;
    @JsonProperty("filterParams")
    private String filterParams = "";
    @JsonProperty("allNodeTypes")
    private boolean forAllNodeTypes = false;

    public boolean isDoAutoFix() {
        return doAutoFix;
    }

    public void setDoAutoFix(boolean doAutoFix) {
        this.doAutoFix = doAutoFix;
    }

    public int getMaxRecordsToFix() {
        return maxRecordsToFix;
    }

    public void setMaxRecordsToFix(int maxRecordsToFix) {
        this.maxRecordsToFix = maxRecordsToFix;
    }

    public int getSleepMinutes() {
        return sleepMinutes;
    }

    public void setSleepMinutes(int sleepMinutes) {
        this.sleepMinutes = sleepMinutes;
    }

    public String getUserId() {
        return userId;
    }

    public void setUserId(String userId) {
        this.userId = userId;
    }

    public String[] getNodeTypes() {
        return nodeTypes;
    }

    public void setNodeTypes(String[] nodeTypes) {
        this.nodeTypes = nodeTypes;
    }

    public int getTimeWindowMinutes() {
        return timeWindowMinutes;
    }

    public void setTimeWindowMinutes(int timeWindowMinutes) {
        this.timeWindowMinutes = timeWindowMinutes;
    }

    public boolean getSkipHostCheck() {
        return skipHostCheck;
    }

    public void setSkipHostCheck(Boolean skipHostCheck) {
        this.skipHostCheck = skipHostCheck;
    }

    public boolean isSpecialTenantRule() {
        return specialTenantRule;
    }

    public void setSpecialTenantRule(boolean specialTenantRule) {
        this.specialTenantRule = specialTenantRule;
    }

    public String getFilterParams() {
        return filterParams;
    }

    public void setFilterParams(String filterParams) {
        this.filterParams = filterParams;
    }

    public boolean isForAllNodeTypes() {
        return forAllNodeTypes;
    }

    public void setForAllNodeTypes(boolean forAllNodeTypes) {
        this.forAllNodeTypes = forAllNodeTypes;
    }
}

