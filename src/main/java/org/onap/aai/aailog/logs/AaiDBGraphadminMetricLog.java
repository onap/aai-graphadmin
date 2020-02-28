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

package org.onap.aai.aailog.logs;

import org.onap.aai.util.AAIConstants;
import org.onap.logging.filter.base.Constants;
import org.onap.logging.filter.base.MDCSetup;
import org.onap.logging.filter.base.ONAPComponents;
import org.onap.logging.ref.slf4j.ONAPLogConstants;
import org.slf4j.*;
import org.springframework.beans.factory.annotation.Value;

import java.net.URI;
import java.time.ZoneOffset;
import java.time.ZonedDateTime;
import java.time.format.DateTimeFormatter;
import java.util.Optional;

public class AaiDBGraphadminMetricLog extends MDCSetup {

    protected static final Logger logger = LoggerFactory.getLogger(AaiDBGraphadminMetricLog.class);
    private final String partnerName;
    private static final Marker INVOKE_RETURN = MarkerFactory.getMarker("INVOKE-RETURN");
    private static final String TARGET_ENTITY = ONAPComponents.AAI.toString() + ".DB";
    public AaiDBGraphadminMetricLog(String subcomponent) {
        partnerName = getPartnerName(subcomponent);
    }


    protected String getTargetServiceName(Optional<URI> uri) {
        return (getServiceName(uri));
    }

    protected String getServiceName(Optional<URI> uri) {
        String serviceName = Constants.DefaultValues.UNKNOWN;
        if (uri.isPresent()) {
            serviceName = uri.get().getPath();
            if (serviceName != null && (!serviceName.isEmpty())) {
                serviceName = serviceName.replaceAll(",", "\\\\,");
            }
        }
        return serviceName;
    }


    protected String getTargetEntity(Optional<URI> uri) {
        return TARGET_ENTITY;
    }

    protected String getPartnerName(@Value(AAIConstants.AAI_TRAVERSAL_MS) String subcomponent  ) {
        StringBuilder sb = new StringBuilder(ONAPComponents.AAI.toString()).append(subcomponent);
        return (sb.toString());
    }

    public void pre(Optional<URI> uri) {
        try {
            setupMDC(uri);
            setLogTimestamp();
            logger.info(ONAPLogConstants.Markers.INVOKE, "Invoke");
        } catch (Exception e) {
            logger.warn("Error in AaiDBMetricLog pre", e.getMessage());
        }
    }

    public void post() {
        try {
            setLogTimestamp();
            setElapsedTimeInvokeTimestamp();
            setResponseStatusCode(200);
            setResponseDescription(200);
            MDC.put(ONAPLogConstants.MDCs.RESPONSE_CODE, "200");
            logger.info(INVOKE_RETURN, "InvokeReturn");
            clearClientMDCs();
        } catch (Exception e) {
            logger.warn("Error in AaiDBMetricLog post", e.getMessage());
        }
    }

    protected void setupMDC(Optional<URI> uri) {
        MDC.put("InvokeTimestamp", ZonedDateTime.now(ZoneOffset.UTC).format(DateTimeFormatter.ISO_INSTANT));
        MDC.put("TargetServiceName", this.getTargetServiceName(uri));
        MDC.put("StatusCode", ONAPLogConstants.ResponseStatus.INPROGRESS.toString());
        this.setInvocationIdFromMDC();
        if (MDC.get("TargetEntity") == null) {
            String targetEntity = this.getTargetEntity(uri);
            if (targetEntity != null) {
                MDC.put("TargetEntity", targetEntity);
            } else {
                MDC.put("TargetEntity", "Unknown-Target-Entity");
            }
        }
        if (MDC.get("ServiceName") == null) {
            MDC.put("ServiceName", this.getServiceName(uri));
        }
        this.setServerFQDN();
    }
}
