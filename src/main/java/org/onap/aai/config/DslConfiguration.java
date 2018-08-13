package org.onap.aai.config;

import org.onap.aai.edges.EdgeIngestor;
import org.onap.aai.rest.dsl.DslListener;
import org.onap.aai.rest.dsl.DslQueryProcessor;
import org.springframework.beans.factory.config.ConfigurableBeanFactory;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;
import org.springframework.context.annotation.Scope;

@Configuration
public class DslConfiguration {

    @Bean
    @Scope(scopeName = ConfigurableBeanFactory.SCOPE_PROTOTYPE)
    public DslListener dslListener(EdgeIngestor edgeIngestor){
        return new DslListener(edgeIngestor);
    }

    @Bean
    @Scope(scopeName = ConfigurableBeanFactory.SCOPE_PROTOTYPE)
    public DslQueryProcessor dslQueryProcessor(DslListener dslListener){
        return new DslQueryProcessor(dslListener);
    }
}
