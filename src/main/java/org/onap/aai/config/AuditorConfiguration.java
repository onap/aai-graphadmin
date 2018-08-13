package org.onap.aai.config;

import org.onap.aai.db.schema.AuditorFactory;
import org.onap.aai.introspection.LoaderFactory;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;

@Configuration
public class AuditorConfiguration {

    @Bean
    public AuditorFactory auditorFactory(LoaderFactory loaderFactory){
        return new AuditorFactory(loaderFactory);
    }
}
