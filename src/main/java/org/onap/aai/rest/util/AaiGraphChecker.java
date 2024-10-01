/*
 * ============LICENSE_START=======================================================
 * Copyright (C) 2022 Bell Canada
 * Modification Copyright (C) 2022 Deutsche Telekom SA
 * ================================================================================
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 * http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 * ============LICENSE_END=========================================================
 */
package org.onap.aai.rest.util;

import java.util.Iterator;

import org.janusgraph.core.JanusGraphException;
import org.janusgraph.core.JanusGraphTransaction;
import org.onap.aai.dbmap.AAIGraph;
import org.onap.aai.logging.ErrorLogHelper;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.beans.factory.config.ConfigurableBeanFactory;
import org.springframework.context.annotation.Scope;
import org.springframework.stereotype.Component;

/**
 * Singleton class responsible to check that AAI service is able to connect to its back-end
 * database.
 */
@Component
@Scope(ConfigurableBeanFactory.SCOPE_SINGLETON)
public class AaiGraphChecker {
    private static final Logger LOGGER = LoggerFactory.getLogger(AaiGraphChecker.class);

    private AaiGraphChecker() {
    }

    /**
     * Checks whether a connection to the graph database can be made.
     * 
     * @return
     *         <li>true, if database is available</li>
     *         <li>false, if database is NOT available</li>
     */
    public Boolean isAaiGraphDbAvailable() {
        Boolean dbAvailable;
        JanusGraphTransaction transaction = null;
        try {
            transaction = AAIGraph.getInstance().getGraph().newTransaction();
            final Iterator<?> vertexIterator = transaction.query().limit(1).vertices().iterator();
            vertexIterator.hasNext();
            dbAvailable = Boolean.TRUE;
        } catch (JanusGraphException e) {
            String message = "Database is not available (after JanusGraph exception)";
            ErrorLogHelper.logError("500", message + ": " + e.getMessage());
            LOGGER.error(message, e);
            dbAvailable = Boolean.FALSE;
        } catch (Error e) {
            String message = "Database is not available (after error)";
            ErrorLogHelper.logError("500", message + ": " + e.getMessage());
            LOGGER.error(message, e);
            dbAvailable = Boolean.FALSE;
        } catch (Exception e) {
            String message = "Database availability can not be determined";
            ErrorLogHelper.logError("500", message + ": " + e.getMessage());
            LOGGER.error(message, e);
            dbAvailable = null;
        } finally {
            if (transaction != null && !transaction.isClosed()) {
                // check if transaction is open then closed instead of flag
                try {
                    transaction.rollback();
                } catch (Exception e) {
                    String message = "Exception occurred while closing transaction";
                    LOGGER.error(message, e);
                    ErrorLogHelper.logError("500", message + ": " + e.getMessage());
                }
            }
        }
        return dbAvailable;
    }
}
