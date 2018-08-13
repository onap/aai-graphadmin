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
package org.onap.aai.service;

import com.att.eelf.configuration.EELFLogger;
import com.att.eelf.configuration.EELFManager;
import org.eclipse.jetty.util.security.Password;
import org.onap.aai.Profiles;
import org.onap.aai.util.AAIConstants;
import org.springframework.context.annotation.Profile;
import org.springframework.stereotype.Service;

import javax.annotation.PostConstruct;
import java.io.IOException;
import java.io.UnsupportedEncodingException;
import java.nio.file.Files;
import java.nio.file.Paths;
import java.util.Base64;
import java.util.HashMap;
import java.util.Map;
import java.util.stream.Stream;

@Profile(Profiles.ONE_WAY_SSL)
@Service
public class AuthorizationService {

    private static final EELFLogger logger = EELFManager.getInstance().getLogger(AuthorizationService.class);

    private final Map<String, String> authorizedUsers = new HashMap<>();

    private static final Base64.Encoder ENCODER = Base64.getEncoder();

    @PostConstruct
    public void init(){

        String basicAuthFile = getBasicAuthFilePath();

        try(Stream<String> stream = Files.lines(Paths.get(basicAuthFile))){
            stream.filter(line -> !line.startsWith("#")).forEach(str -> {
                byte [] bytes = null;

                String usernamePassword = null;
                String accessType = null;

                try {
                    String [] userAccessType = str.split(",");

                    if(userAccessType == null || userAccessType.length != 2){
                        throw new RuntimeException("Please check the realm.properties file as it is not conforming to the basic auth");
                    }

                    usernamePassword = userAccessType[0];
                    accessType       = userAccessType[1];

                    String[] usernamePasswordArray = usernamePassword.split(":");

                    if(usernamePasswordArray == null || usernamePasswordArray.length != 3){
                        throw new RuntimeException("Not a valid entry for the realm.properties entry: " + usernamePassword);
                    }

                    String username = usernamePasswordArray[0];
                    String password = null;

                    if(str.contains("OBF:")){
                        password = usernamePasswordArray[1] + ":" + usernamePasswordArray[2];
                        password = Password.deobfuscate(password);
                    }

                    bytes = ENCODER.encode((username + ":" + password).getBytes("UTF-8"));

                    authorizedUsers.put(new String(bytes), accessType);

                } catch (UnsupportedEncodingException e)
                {
                    logger.error("Unable to support the encoding of the file" + basicAuthFile);
                }

                authorizedUsers.put(new String(ENCODER.encode(bytes)), accessType);
            });
        } catch (IOException e) {
            logger.error("IO Exception occurred during the reading of realm.properties", e);
        }
    }

    public boolean checkIfUserAuthorized(String authorization){
        return authorizedUsers.containsKey(authorization) && "admin".equals(authorizedUsers.get(authorization));
    }

    public String getBasicAuthFilePath(){
        return AAIConstants.AAI_HOME_ETC_AUTH + AAIConstants.AAI_FILESEP + "realm.properties";
    }
}
