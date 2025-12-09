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
package org.onap.aai.dbgen;

import com.beust.jcommander.Parameter;
import org.onap.aai.util.GraphAdminConstants;

import java.util.ArrayList;
import java.util.List;

public class DupeToolCommandLineArgs {

    @Parameter(names = "-autoFix", description = "doautofix")
    public boolean doAutoFix = false;

    @Parameter(names = "-maxFix", description = "maxFix")
    public int maxRecordsToFix = GraphAdminConstants.AAI_GROOMING_DEFAULT_MAX_FIX;

    @Parameter(names = "-sleepMinutes", description = "sleepMinutes")
    public int sleepMinutes = GraphAdminConstants.AAI_GROOMING_DEFAULT_SLEEP_MINUTES;

    @Parameter(names = "-userId", description = "userId under which the script will run")
    public String userId = "amd8383";

    @Parameter(names = "-nodeTypes", description = "nodeType")
    public String nodeTypes ;

    // A value of 0 means that we will not have a time-window -- we will look
    // at all nodes of the passed-in nodeType.
    @Parameter(names = "-timeWindowMinutes", description = "timeWindowMinutes")
    public int timeWindowMinutes = 0;

    @Parameter(names = "-skipHostCheck", description = "skipHostCheck")
    public boolean skipHostCheck = false;

    @Parameter(names= "-specialTenantRule" , description = "specialTenantRule")
    public boolean specialTenantRule = false;

    @Parameter(names = "-filterParams", description = "specific filter parameters")
    public String filterParams = "";

    @Override
    public String toString() {
        return  "doAutoFix=" + doAutoFix +
                ", maxRecordsToFix=" + maxRecordsToFix +
                ", sleepMinutes=" + sleepMinutes +
                ", userId='" + userId + '\'' +
                ", nodeType='" + nodeTypes + '\'' +
                ", timeWindowMinutes=" + timeWindowMinutes +
                ", skipHostCheck=" + skipHostCheck +
                ", specialTenantRule=" + specialTenantRule +
                ", filterParams='" + filterParams + '\'' +
                ", forAllNodeTypes=" + forAllNodeTypes;
    }

    @Parameter(names = "-allNodeTypes", description = "to run for all node types")
    public boolean forAllNodeTypes = false;
}
