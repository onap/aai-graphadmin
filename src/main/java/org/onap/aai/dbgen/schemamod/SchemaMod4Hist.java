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
package org.onap.aai.dbgen.schemamod;

import org.onap.aai.restclient.PropertyPasswordConfiguration;
import org.onap.aai.exceptions.AAIException;
import org.onap.aai.introspection.Loader;
import org.onap.aai.introspection.LoaderFactory;
import org.onap.aai.introspection.ModelType;
import org.onap.aai.logging.ErrorLogHelper;
import org.onap.aai.serialization.engines.JanusGraphDBEngine;
import org.onap.aai.serialization.engines.QueryStyle;
import org.onap.aai.serialization.engines.TransactionalGraphEngine;
import org.onap.aai.setup.SchemaVersion;
import org.onap.aai.setup.SchemaVersions;
import org.onap.aai.util.AAIConfig;
import org.onap.aai.util.AAIConstants;
import org.onap.aai.util.ExceptionTranslator;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.slf4j.MDC;
import org.springframework.context.annotation.AnnotationConfigApplicationContext;

import java.util.Properties;

public class SchemaMod4Hist {

	private final LoaderFactory loaderFactory;

	private final SchemaVersions schemaVersions;

	private static boolean historyEnabled;

    public SchemaMod4Hist(LoaderFactory loaderFactory, SchemaVersions schemaVersions){
        this.loaderFactory  = loaderFactory;
        this.schemaVersions = schemaVersions;
	}

	public void execute(String[] args) {

		Logger logger = LoggerFactory.getLogger(SchemaMod4Hist.class.getSimpleName());
		MDC.put("logFilenameAppender", SchemaMod4Hist.class.getSimpleName());


		// NOTE -- We're just working with properties that are used for NODES
		// for now.
		String propName = "";
		String targetDataType = "";
		String targetIndexInfo = "";
		String preserveDataFlag = "";

		String usageString = "Usage: SchemaMod4Hist propertyName targetDataType targetIndexInfo preserveDataFlag \n";
		if (args.length != 4) {
			String emsg = "Four Parameters are required.  \n" + usageString;
			logAndPrint(logger, emsg);
			System.exit(1);
		} else {
			propName = args[0];
			targetDataType = args[1];
			targetIndexInfo = args[2];
			preserveDataFlag = args[3];  // Note - even if they pass in "false", History will preserve the data
		}

		if (propName.equals("")) {
			String emsg = "Bad parameter - propertyName cannot be empty.  \n" + usageString;
			logAndPrint(logger, emsg);
			System.exit(1);
		} else if (!targetDataType.equals("String") && !targetDataType.equals("Set<String>")
				&& !targetDataType.equals("Integer") && !targetDataType.equals("Long")
				&& !targetDataType.equals("Boolean")) {
			String emsg = "Unsupported targetDataType.  We only support String, Set<String>, Integer, Long or Boolean for now.\n"
					+ usageString;
			logAndPrint(logger, emsg);
			System.exit(1);
		} else if (!targetIndexInfo.equals("index") && !targetIndexInfo.equals("noIndex")) {
			String emsg = "Unsupported IndexInfo.  We only support: 'index' or 'noIndex' for History db.\n"
					+ usageString;
			logAndPrint(logger, emsg);
			System.exit(1);
		}

		try {
			AAIConfig.init();
			ErrorLogHelper.loadProperties();
		} catch (Exception ae) {
			String emsg = "Problem with either AAIConfig.init() or ErrorLogHelper.LoadProperties(). ";
			logAndPrint(logger, emsg + "[" + ae.getMessage() + "]");
			System.exit(1);
		}

		// Give a big warning if the DbMaps.PropertyDataTypeMap value does not
		// agree with what we're doing
		String warningMsg = "";

		if (!warningMsg.equals("")) {
			logAndPrint(logger, "\n>>> WARNING <<<< ");
			logAndPrint(logger, ">>> " + warningMsg + " <<<");
		}

		logAndPrint(logger, ">>> Processing will begin in 5 seconds (unless interrupted). <<<");
		try {
			// Give them a chance to back out of this
			Thread.sleep(5000);
		} catch (java.lang.InterruptedException ie) {
			logAndPrint(logger, " DB Schema Update has been aborted. ");
			System.exit(1);
		}

        logAndPrint(logger, "    ---- NOTE --- about to open graph (takes a little while)\n");

        SchemaVersion version = schemaVersions.getDefaultVersion();
        QueryStyle queryStyle = QueryStyle.TRAVERSAL;
        ModelType introspectorFactoryType = ModelType.MOXY;
        Loader loader = loaderFactory.createLoaderForVersion(introspectorFactoryType, version);
        TransactionalGraphEngine engine = null;
        try {
            engine = new JanusGraphDBEngine(queryStyle, loader);
            //NOTE - no matter what they passed in, we're passing in "true" for the
            //   preserve-data-flag last parameter since this is for HISTORY.
            SchemaModInternal4Hist internal = new SchemaModInternal4Hist(engine, logger, propName, targetDataType, targetIndexInfo, true);
            internal.execute();
            logAndPrint(logger, "------ Completed the SchemaMod -------- ");
        } catch (Exception e) {
            String emsg = "Not able to complete the requested SchemaMod4Hist \n";
            logAndPrint(logger, e.getMessage());
            logAndPrint(logger, emsg);
            System.exit(1);
        }
	}
	/**
	 * Log and print.
	 *
	 * @param logger the logger
	 * @param msg the msg
	 */
	protected void logAndPrint(Logger logger, String msg) {
		System.out.println(msg);
		logger.debug(msg);
	}

	public static void main(String[] args) throws AAIException {

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
			System.out.println("Problems running SchemaMod4Hist "+aai.getMessage());
			ErrorLogHelper.logError(aai.getCode(), e.getMessage() + ", resolve and retry");
			throw aai;
		}

		historyEnabled = Boolean.parseBoolean(ctx.getEnvironment().getProperty("history.enabled","false"));
		if( !historyEnabled ) {
			String emsg = "SchemaMod4Hist may only be used when history.enabled=true. ";
			System.out.println(emsg);
			throw new AAIException("AAI-4005",emsg);
		}

		LoaderFactory loaderFactory = ctx.getBean(LoaderFactory.class);
		SchemaVersions schemaVersions = (SchemaVersions) ctx.getBean("schemaVersions");
		SchemaMod4Hist schemaMod4H = new SchemaMod4Hist(loaderFactory, schemaVersions);
		schemaMod4H.execute(args);

		System.exit(0);
	}

}
