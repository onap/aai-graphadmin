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
package org.onap.aai.schema;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.janusgraph.core.JanusGraph;
import org.janusgraph.core.schema.JanusGraphManagement;
import org.onap.aai.restclient.PropertyPasswordConfiguration;
import org.onap.aai.dbgen.SchemaGenerator4Hist;
import org.onap.aai.dbmap.AAIGraph;
import org.onap.aai.exceptions.AAIException;
import org.onap.aai.logging.ErrorLogHelper;
import org.onap.aai.util.*;
import org.springframework.context.annotation.AnnotationConfigApplicationContext;

import java.util.Properties;
import java.util.UUID;


public class GenTester4Hist {

	private static Logger LOGGER;
	private static boolean historyEnabled;

	/**
	 * The main method.
	 *
	 * @param args the arguments
	 */
	public static void main(String[] args) throws AAIException{

		JanusGraph graph = null;
		System.setProperty("aai.service.name", GenTester4Hist.class.getSimpleName());

		LOGGER = LoggerFactory.getLogger(GenTester4Hist.class);
		boolean addDefaultCR = false;  // For History, we do not add the default CloudRegion

		AnnotationConfigApplicationContext ctx = new AnnotationConfigApplicationContext();
		PropertyPasswordConfiguration initializer = new PropertyPasswordConfiguration();
		initializer.initialize(ctx);
		try {
			ctx.scan(
					"org.onap.aai.config",
					"org.onap.aai.setup",
					"org.onap.aai.introspection"
			);
			ctx.refresh();
		} catch (Exception e) {
			AAIException aai = ExceptionTranslator.schemaServiceExceptionTranslator(e);
			LOGGER.error("Problems running the tool "+aai.getMessage());
			ErrorLogHelper.logError(aai.getCode(), e.getMessage() + ", resolve and retry");
			throw aai;
		}

		historyEnabled = Boolean.parseBoolean(ctx.getEnvironment().getProperty("history.enabled","false"));
		if( !historyEnabled ) {
	    	   String amsg = "GenTester4Hist may only be used when history.enabled=true. ";
	    	   System.out.println(amsg);
	    	   LOGGER.debug(amsg);
	           return;
		}

		try {
            LOGGER.debug("GenTester4Hist uses either cql jar or Cassandra jar");

			AAIConfig.init();
	    	if (args != null && args.length > 0 ){
	    		if( "genDbRulesOnly".equals(args[0]) ){
	    			ErrorLogHelper.logError("AAI_3100",
	    					" This option is no longer supported. What was in DbRules is now derived from the OXM files. ");
	    			return;
	    		}
	    		else if ( "GEN_DB_WITH_NO_SCHEMA".equals(args[0]) ){
		    		// Note this is done to create an empty DB with no Schema so that
					// an HBase copyTable can be used to set up a copy of the db.
					String imsg = "    ---- NOTE --- about to load a graph without doing any schema processing (takes a little while) --------   ";
	            	System.out.println(imsg);
	            	LOGGER.debug(imsg);
					graph = AAIGraph.getInstance().getGraph();

			       if( graph == null ){
					   ErrorLogHelper.logError("AAI_5102", "Error creating JanusGraph graph.");
			           return;
			       }
			       else {
			    	   String amsg = "Successfully loaded a JanusGraph graph without doing any schema work.  ";
			    	   System.out.println(amsg);
			    	   LOGGER.debug(amsg);
			           return;
			       }
	    		} else if ("GEN_DB_WITH_NO_DEFAULT_CR".equals(args[0])) {
	    			addDefaultCR = false;
	    		}
	    		else {
	    			ErrorLogHelper.logError("AAI_3000", "Unrecognized argument passed to GenTester4Hist.java: [" + args[0] + "]. ");

	    			String emsg = "Unrecognized argument passed to GenTester4Hist.java: [" + args[0] + "]. ";
	    			System.out.println(emsg);
	    			LOGGER.error(emsg);

	    			emsg = "Either pass no argument for normal processing, or use 'GEN_DB_WITH_NO_SCHEMA'.";
	    			System.out.println(emsg);
	    			LOGGER.error(emsg);

	    			return;
	    		}
	    	}

			//AAIConfig.init();
			ErrorLogHelper.loadProperties();
			String imsg = "    ---- NOTE --- about to open graph (takes a little while)--------;";
        	System.out.println(imsg);
        	LOGGER.debug(imsg);
			graph = AAIGraph.getInstance().getGraph();

			if( graph == null ){
				ErrorLogHelper.logError("AAI_5102", "Error creating JanusGraph graph. ");
				return;
			}

			GraphAdminDBUtils.logConfigs(graph.configuration());

			// Load the propertyKeys, indexes and edge-Labels into the DB
			JanusGraphManagement graphMgt = graph.openManagement();

	       	imsg = "-- Loading new schema elements into JanusGraph --";
       		System.out.println(imsg);
       		LOGGER.debug(imsg);
       		SchemaGenerator4Hist.loadSchemaIntoJanusGraph(graphMgt, null);

            if( graph != null ){
                imsg = "-- graph commit";
                System.out.println(imsg);
                LOGGER.debug(imsg);
                graph.tx().commit();

                imsg = "-- graph shutdown ";
                System.out.println(imsg);
                LOGGER.debug(imsg);
                graph.close();
            }

	    } catch(Exception ex) {
	    	ErrorLogHelper.logError("AAI_4000", ex.getMessage());
	    	System.exit(1);
	    }

	    LOGGER.debug("-- all done, if program does not exit, please kill.");
	    System.exit(0);
    }



}
