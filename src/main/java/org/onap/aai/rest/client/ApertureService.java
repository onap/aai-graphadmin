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
package org.onap.aai.rest.client;


import com.google.gson.JsonObject;
import org.apache.http.conn.ConnectTimeoutException;
import org.onap.aai.exceptions.AAIException;
import org.onap.aai.restclient.RestClient;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.beans.factory.annotation.Qualifier;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.context.annotation.Profile;
import org.springframework.http.HttpMethod;
import org.springframework.http.ResponseEntity;
import org.springframework.stereotype.Service;
import javax.annotation.PostConstruct;
import java.net.ConnectException;
import java.net.SocketTimeoutException;
import java.util.*;

@Service
public class ApertureService {

    /**
     * Error indicating that the service trying to connect is down
     */
    static final String CONNECTION_REFUSED_STRING =
        "Connection refused to the Aperture microservice due to service unreachable";

    /**
     * Error indicating that the server is unable to reach the port
     * Could be server related connectivity issue
     */
    static final String CONNECTION_TIMEOUT_STRING =
        "Connection timeout to the Aperture microservice as this could " +
        "indicate the server is unable to reach port, " +
        "please check on server by running: nc -w10 -z -v ${APERTURE_HOST} ${APERTURE_PORT}";

    /**
     * Error indicating that the request exceeded the allowed time
     *
     * Note: This means that the service could be active its
     *       just taking some time to process our request
     */
    static final String REQUEST_TIMEOUT_STRING =
        "Request to Aperture service took longer than the currently set timeout";

    static final String APERTURE_ENDPOINT = "/v1/audit/";

    private static final Logger LOGGER = LoggerFactory.getLogger(ApertureService.class);
    private final RestClient apertureRestClient;
    private final String appName;


    @Autowired
    public ApertureService(
        @Qualifier("apertureRestClient") RestClient apertureRestClient,
        @Value("${spring.application.name}") String appName
    ){
        this.apertureRestClient = apertureRestClient;
        this.appName = appName;

        LOGGER.info("Successfully initialized the aperture service");
    }


    public JsonObject runAudit(Long tStamp, String dbName) throws AAIException {

        Map<String, String> httpHeaders = new HashMap<>();

        httpHeaders.put("X-FromAppId", appName);
        httpHeaders.put("X-TransactionID", UUID.randomUUID().toString());
        httpHeaders.put("Accept", "application/json");

        String queryParams = "?timestamp=" + tStamp + "&dbname=" + dbName;

        ResponseEntity responseEntity = null;
        try {
            responseEntity = apertureRestClient.execute(
                    APERTURE_ENDPOINT + queryParams,
                    HttpMethod.GET,
                    httpHeaders
            );

            if(isSuccess(responseEntity)){
                LOGGER.debug("Audit returned following response status code {} and body {}", responseEntity.getStatusCodeValue(), responseEntity.getBody());
            }

        } catch(Exception e){
            // If the exception cause is client side timeout
            // then proceed as if it passed validation
            // resources microservice shouldn't be blocked because of validation service
            // is taking too long or if the validation service is down
            // Any other exception it should block the request from passing?
            if(e.getCause() instanceof SocketTimeoutException){
                LOGGER.error(REQUEST_TIMEOUT_STRING, e.getCause());
            } else if(e.getCause() instanceof ConnectException){
                LOGGER.error(CONNECTION_REFUSED_STRING, e.getCause());
            } else if(e.getCause() instanceof ConnectTimeoutException){
                LOGGER.error(CONNECTION_TIMEOUT_STRING, e.getCause());
            } else {
                LOGGER.error("Unknown exception thrown please investigate", e.getCause());
            }
        }

        JsonObject jsonResults = new JsonObject ();
        if( responseEntity != null ) {
            jsonResults = (JsonObject) (responseEntity.getBody());
        }

        return jsonResults;
    }

 

    boolean isSuccess(ResponseEntity responseEntity){
        return responseEntity != null && responseEntity.getStatusCode().is2xxSuccessful();
    }


 
}
