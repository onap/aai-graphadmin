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

import org.springframework.beans.factory.annotation.Value;
import org.springframework.context.annotation.PropertySource;
import org.springframework.stereotype.Service;

import javax.annotation.PostConstruct;
import java.util.Arrays;
import java.util.List;
import java.util.regex.Pattern;
import java.util.stream.Collectors;

@Service
@PropertySource("classpath:retired.properties")
@PropertySource(value = "file:${server.local.startpath}/retired.properties")
public class RetiredService {

    private String retiredPatterns;

    private String retiredAllVersions;

    private List<Pattern> retiredPatternsList;
    private List<Pattern> retiredAllVersionList;

    @PostConstruct
    public void initialize(){
        this.retiredPatternsList = Arrays.stream(retiredPatterns.split(",")).map(Pattern::compile).collect(Collectors.toList());
        this.retiredAllVersionList = Arrays.stream(retiredAllVersions.split(",")).map(Pattern::compile).collect(Collectors.toList());
    }

    @Value("${retired.api.pattern.list}")
    public void setRetiredPatterns(String retiredPatterns){
        this.retiredPatterns = retiredPatterns;
    }

    public List<Pattern> getRetiredPatterns(){
        return retiredPatternsList;
    }

    @Value("${retired.api.all.versions}")
    public void setRetiredAllVersions(String retiredPatterns){
        this.retiredAllVersions = retiredPatterns;
    }

    public List<Pattern> getRetiredAllVersionList(){
        return retiredAllVersionList;
    }
}
