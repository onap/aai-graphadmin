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
package org.onap.aai;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.onap.aai.aailog.logs.AaiDebugLog;
import org.onap.aai.restclient.PropertyPasswordConfiguration;
import org.onap.aai.dbmap.AAIGraph;
import org.onap.aai.exceptions.AAIException;
import org.onap.aai.logging.ErrorLogHelper;
import org.onap.aai.nodes.NodeIngestor;
import org.onap.aai.util.AAIConfig;
import org.onap.aai.util.ExceptionTranslator;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.boot.SpringApplication;
import org.springframework.boot.autoconfigure.EnableAutoConfiguration;
import org.springframework.boot.autoconfigure.SpringBootApplication;
import org.springframework.boot.autoconfigure.jdbc.DataSourceAutoConfiguration;
import org.springframework.boot.autoconfigure.orm.jpa.HibernateJpaAutoConfiguration;
import org.springframework.context.annotation.ComponentScan;
import org.springframework.context.annotation.EnableAspectJAutoProxy;
import org.springframework.core.env.Environment;
import org.springframework.scheduling.annotation.EnableAsync;
import org.springframework.scheduling.annotation.EnableScheduling;

import javax.annotation.PostConstruct;
import javax.annotation.PreDestroy;

@SpringBootApplication
// Scan the specific packages that has the beans/components
// This will add the ScheduledTask that was created in aai-common
// Add more packages where you would need to scan for files
@ComponentScan(basePackages = {
    "org.onap.aai.tasks",
    "org.onap.aai.config",
    "org.onap.aai.service",
    "org.onap.aai.setup",
    "org.onap.aai.aaf",
    "org.onap.aai.rest",
    "org.onap.aai.web",
    "org.onap.aai.interceptors",
    "org.onap.aai.datasnapshot",
    "org.onap.aai.datagrooming",
    "org.onap.aai.dataexport",
    "org.onap.aai.datacleanup",
    "org.onap.aai.aailog",
    "org.onap.aai.failover",
    "org.onap.aai.audit",
    "org.onap.aai.introspection",
    "org.onap.aai.rest.notification"
})
@EnableAsync
@EnableScheduling
@EnableAspectJAutoProxy
@EnableAutoConfiguration(exclude = {DataSourceAutoConfiguration.class, HibernateJpaAutoConfiguration.class})
public class GraphAdminApp {

    public static final String APP_NAME = "GraphAdmin";
    private static final Logger LOGGER = LoggerFactory.getLogger(GraphAdminApp.class);

    private static AaiDebugLog debugLog = new AaiDebugLog();
	static {
		debugLog.setupMDC();
	}


    @Autowired
    private Environment env;

    @Autowired
    private NodeIngestor nodeIngestor;

    @PostConstruct
    private void initialize(){
        loadDefaultProps();
    }

    @PreDestroy
    public void cleanup(){
        AAIGraph.getInstance().graphShutdown();
    }

    public static void main(String[] args) throws Exception {

        loadDefaultProps();

        ErrorLogHelper.loadProperties();

        Environment env =null;
        AAIConfig.init();
        try {
            SpringApplication app = new SpringApplication(GraphAdminApp.class);
            app.setRegisterShutdownHook(true);
            app.addInitializers(new PropertyPasswordConfiguration());
            env = app.run(args).getEnvironment();
        }

        catch(Exception ex){
            AAIException aai = ExceptionTranslator.schemaServiceExceptionTranslator(ex);
            ErrorLogHelper.logException(aai);
            ErrorLogHelper.logError(aai.getCode(), ex.getMessage() + ", resolve and restart GraphAdmin");
            throw aai;
        }
        LOGGER.info(
                "Application '{}' is running on {}!" ,
                env.getProperty("spring.application.name"),
                env.getProperty("server.port")
        );
        // The main reason this was moved from the constructor is due
        // to the SchemaGenerator needs the bean and during the constructor
        // the Spring Context is not yet initialized

        AAIGraph.getInstance();

        System.setProperty("org.onap.aai.graphadmin.started", "true");
        LOGGER.info("GraphAdmin MicroService Started");
        LOGGER.debug("GraphAdmin MicroService Started");
        System.out.println("GraphAdmin Microservice Started");
    }



    public static void loadDefaultProps(){

        /*
         * Required for DB connection name
         */
        System.setProperty("aai.service.name", GraphAdminApp.class.getSimpleName());
        if(System.getProperty("AJSC_HOME") == null){
            System.setProperty("AJSC_HOME", ".");
        }

        if(System.getProperty("BUNDLECONFIG_DIR") == null){
            System.setProperty("BUNDLECONFIG_DIR", "src/main/resources");
        }
    }

}
