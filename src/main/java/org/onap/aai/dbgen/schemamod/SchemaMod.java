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

import com.att.eelf.configuration.Configuration;
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

public class SchemaMod {

	private final LoaderFactory loaderFactory;

	private final SchemaVersions schemaVersions;

	private static boolean historyEnabled;

	private Logger logger = LoggerFactory.getLogger(SchemaMod.class.getSimpleName());


    public SchemaMod(LoaderFactory loaderFactory, SchemaVersions schemaVersions){
        this.loaderFactory  = loaderFactory;
        this.schemaVersions = schemaVersions;
	}

	public void execute(String[] args) {

        logger = LoggerFactory.getLogger(SchemaMod.class.getSimpleName());

		// NOTE -- We're just working with properties that are used for NODES
		// for now.
		String propName = "";
		String targetDataType = "";
		String targetIndexInfo = "";
		String preserveDataFlag = "";
		String consistencyLockFlag = "";
		String commitBlockSizeStr = "";
		long commitBlockSize = 120000;

		String usageString = "Usage: SchemaMod propertyName targetDataType targetIndexInfo preserveDataFlag consistencyLockFlag [blockSize] \n";

		if (args.length == 5) {
			propName = args[0];
			targetDataType = args[1];
			targetIndexInfo = args[2];
			preserveDataFlag = args[3];
			consistencyLockFlag = args[4];
		}
		else if (args.length == 6) {
			propName = args[0];
			targetDataType = args[1];
			targetIndexInfo = args[2];
			preserveDataFlag = args[3];
			consistencyLockFlag = args[4];
			commitBlockSizeStr = args[5];
		}
		else {
			String emsg = "Incorrect number of Parameters passed.  \n" + usageString;
			logAndPrint(logger, emsg);
			System.exit(1);
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
		} else if (!targetIndexInfo.equals("uniqueIndex") && !targetIndexInfo.equals("index")
				&& !targetIndexInfo.equals("noIndex")) {
			String emsg = "Unsupported IndexInfo.  We only support: 'uniqueIndex', 'index' or 'noIndex'.\n"
					+ usageString;
			logAndPrint(logger, emsg);
			System.exit(1);
		}

		try {
			if( !commitBlockSizeStr.equals("")) {
				// They're over-riding the commitBlockSize
				commitBlockSize = Long.parseLong(commitBlockSizeStr);
			}
		} catch (NumberFormatException nfe) {
			String emsg = "NumberFormatException - Bad block size passed in: [" + commitBlockSizeStr + "]. ";
			logAndPrint(logger, emsg );
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
            SchemaModInternalBatch internal = new SchemaModInternalBatch(engine, logger, propName, targetDataType, targetIndexInfo, Boolean.parseBoolean(preserveDataFlag), Boolean.parseBoolean(consistencyLockFlag), commitBlockSize);
            internal.execute();
            logAndPrint(logger, "------ Completed the SchemaMod -------- ");
        } catch (Exception e) {
            String emsg = "Not able to complete the requested SchemaMod \n";
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

		Properties props = System.getProperties();
		props.setProperty(Configuration.PROPERTY_LOGGING_FILE_NAME, AAIConstants.AAI_SCHEMA_MOD_LOGBACK_PROPS);
		props.setProperty(Configuration.PROPERTY_LOGGING_FILE_PATH, AAIConstants.AAI_HOME_BUNDLECONFIG);

		MDC.put("logFilenameAppender", SchemaMod.class.getSimpleName());

		AnnotationConfigApplicationContext ctx = new AnnotationConfigApplicationContext();
		PropertyPasswordConfiguration initializer = new PropertyPasswordConfiguration();
		initializer.initialize(ctx);
		try {
			ctx.scan(
					"org.onap.aai.config",
					"org.onap.aai.setup"
			);
			ctx.refresh();
		} catch (Exception e) {
			AAIException aai = ExceptionTranslator.schemaServiceExceptionTranslator(e);
			System.out.println("Problems running SchemaMod "+aai.getMessage());
			ErrorLogHelper.logError(aai.getCode(), e.getMessage() + ", resolve and retry");
			throw aai;
		}

		historyEnabled = Boolean.parseBoolean(ctx.getEnvironment().getProperty("history.enabled","false"));
		if( historyEnabled ) {
			String emsg = "Regular SchemaMod may not be used when history.enabled=true. ";
			System.out.println(emsg);
			throw new AAIException("AAI-4005",emsg);
		}

		LoaderFactory loaderFactory = ctx.getBean(LoaderFactory.class);
		SchemaVersions schemaVersions = (SchemaVersions) ctx.getBean("schemaVersions");
		SchemaMod schemaMod = new SchemaMod(loaderFactory, schemaVersions);
		schemaMod.execute(args);

		System.exit(0);
	}

}
