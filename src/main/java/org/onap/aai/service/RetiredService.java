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
