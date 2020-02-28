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

package org.onap.aai.dbgen;

import org.janusgraph.core.JanusGraph;
import org.onap.aai.util.AAIConstants;
import com.att.eelf.configuration.Configuration;
import org.slf4j.MDC;

import java.util.Properties;
import java.util.UUID;

public class UpdatePropertyTool {

    private static 	final  String    FROMAPPID = "AAI-DB";
    private static 	final  String    TRANSID   = UUID.randomUUID().toString();

    public static boolean SHOULD_EXIT_VM = true;

    public static int EXIT_VM_STATUS_CODE = -1;
    public static int EXIT_VM_STATUS_CODE_SUCCESS = 0;
    public static int EXIT_VM_STATUS_CODE_FAILURE = 1;
    public static final String PROPERTY_LOGGING_FILE_NAME = "updatePropertyTool-logback.xml";

    public static void exit(int statusCode){
        if(SHOULD_EXIT_VM){
            System.exit(statusCode);
        }
        EXIT_VM_STATUS_CODE = statusCode;
    }

    /**
     * The main method.
     *
     * @param args the arguments
     */
    public static void main(String[] args)
    {
        System.setProperty("aai.service.name", UpdatePropertyTool.class.getSimpleName());
		// Set the logging file properties to be used by EELFManager
		Properties props = System.getProperties();
		props.setProperty(Configuration.PROPERTY_LOGGING_FILE_NAME, PROPERTY_LOGGING_FILE_NAME);
		props.setProperty(Configuration.PROPERTY_LOGGING_FILE_PATH, AAIConstants.AAI_HOME_BUNDLECONFIG);
		MDC.put("logFilenameAppender", UpdatePropertyTool.class.getSimpleName());


        UpdatePropertyToolInternal updatePropertyToolInternal = new UpdatePropertyToolInternal();
        JanusGraph graph = updatePropertyToolInternal.openGraph(AAIConstants.REALTIME_DB_CONFIG);

        try {
            EXIT_VM_STATUS_CODE = updatePropertyToolInternal.run(graph, args) ? EXIT_VM_STATUS_CODE_SUCCESS : EXIT_VM_STATUS_CODE_FAILURE;
        } catch (Exception e) {
            e.printStackTrace();
            EXIT_VM_STATUS_CODE = EXIT_VM_STATUS_CODE_FAILURE;
        } finally {
            updatePropertyToolInternal.closeGraph(graph);
        }

        exit(EXIT_VM_STATUS_CODE);
    }
}