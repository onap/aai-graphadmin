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
package org.onap.aai.util;

public final class GraphAdminConstants {
    
    public static final int AAI_SNAPSHOT_DEFAULT_THREADS_FOR_CREATE = 15;
    public static final Long AAI_SNAPSHOT_DEFAULT_MAX_NODES_PER_FILE_FOR_CREATE = 120000L;
    public static final int AAI_SNAPSHOT_DEFAULT_MAX_ERRORS_PER_THREAD = 25;
    public static final Long AAI_SNAPSHOT_DEFAULT_VERTEX_ADD_DELAY_MS = 1L;
    public static final Long AAI_SNAPSHOT_DEFAULT_EDGE_ADD_DELAY_MS = 1L;
    public static final Long AAI_SNAPSHOT_DEFAULT_FAILURE_DELAY_MS = 50L;
    public static final Long AAI_SNAPSHOT_DEFAULT_RETRY_DELAY_MS = 1500L;
    public static final Long AAI_SNAPSHOT_DEFAULT_VERTEX_TO_EDGE_PROC_DELAY_MS = 9000L;
    public static final Long AAI_SNAPSHOT_DEFAULT_STAGGER_THREAD_DELAY_MS = 5000L;

    public static final int AAI_GROOMING_DEFAULT_MAX_FIX = 150;
    public static final int AAI_GROOMING_DEFAULT_SLEEP_MINUTES = 7;

    public static final int AAI_DUPETOOL_DEFAULT_MAX_FIX = 25;
    public static final int AAI_DUPETOOL_DEFAULT_SLEEP_MINUTES = 7;

    /** Micro-service Names */
    public static final String AAI_GRAPHADMIN_MS = "aai-graphadmin";
    
    
    /**
     * Instantiates a new GraphAdmin constants.
     */
    private GraphAdminConstants() {
        // prevent instantiation
    }

}
