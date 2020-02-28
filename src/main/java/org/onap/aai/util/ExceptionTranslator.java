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

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.apache.commons.lang3.exception.ExceptionUtils;
import org.onap.aai.GraphAdminApp;
import org.onap.aai.exceptions.AAIException;
import org.onap.aai.logging.LogFormatTools;

public class ExceptionTranslator {
    private static final Logger LOGGER = LoggerFactory.getLogger(ExceptionTranslator.class);
    public static AAIException schemaServiceExceptionTranslator(Exception ex) {
        AAIException aai = null;
        if ( ExceptionUtils.getRootCause(ex) == null || ExceptionUtils.getRootCause(ex).getMessage() == null ) {
        	aai = new  AAIException("AAI_3025","Error parsing exception - Please Investigate" + 
                	LogFormatTools.getStackTop(ex));
        } else {
	        LOGGER.info("Exception is " + ExceptionUtils.getRootCause(ex).getMessage() + "Root cause is"+ ExceptionUtils.getRootCause(ex).toString());
	        if(ExceptionUtils.getRootCause(ex).getMessage().contains("NodeIngestor")){
	            aai = new  AAIException("AAI_3026","Error reading OXM from SchemaService - Investigate");
	        }
	        else if(ExceptionUtils.getRootCause(ex).getMessage().contains("EdgeIngestor")){
	            aai = new  AAIException("AAI_3027","Error reading EdgeRules from SchemaService - Investigate");
	        }
	        else if(ExceptionUtils.getRootCause(ex).getMessage().contains("Connection refused")){
	            aai = new  AAIException("AAI_3025","Error connecting to SchemaService - Investigate");
	        }else {
	            aai = new  AAIException("AAI_3025","Error connecting to SchemaService - Please Investigate");
	        }
        }

        return aai;
    }
}
