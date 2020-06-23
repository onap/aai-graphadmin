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
package org.onap.aai.datagrooming;

import java.io.BufferedReader;
import java.io.InputStream;
import java.io.InputStreamReader;
import java.text.SimpleDateFormat;
import java.util.ArrayList;
import java.util.Date;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

import org.onap.aai.aailog.logs.AaiScheduledTaskAuditLog;
import org.onap.aai.exceptions.AAIException;
import org.onap.aai.introspection.LoaderFactory;
import org.onap.aai.logging.ErrorLogHelper;
import org.onap.aai.logging.LogFormatTools;
import org.onap.aai.setup.SchemaVersions;
import org.onap.aai.util.AAIConfig;
import org.onap.logging.filter.base.ONAPComponents;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.context.annotation.PropertySource;
import org.springframework.scheduling.annotation.Scheduled;
import org.springframework.stereotype.Component;

@Component
@PropertySource("file:${server.local.startpath}/etc/appprops/datatoolscrons.properties")
public class DataGroomingTasks {

	private AaiScheduledTaskAuditLog auditLog;
	
	private static final Logger LOGGER = LoggerFactory.getLogger(DataGroomingTasks.class);
	private final SimpleDateFormat dateFormat = new SimpleDateFormat("HH:mm:ss");

	@Autowired
	private LoaderFactory loaderFactory;

	@Autowired
	private SchemaVersions schemaVersions;

	@Scheduled(cron = "${datagroomingtasks.cron}" )
	public void groomingScheduleTask() throws AAIException, Exception   {
		auditLog = new AaiScheduledTaskAuditLog();
		auditLog.logBefore("dataGroomingTask", ONAPComponents.AAI.toString());

		if(!"true".equals(AAIConfig.get("aai.disable.check.grooming.running", "false"))){
			if(checkIfDataGroomingIsRunning()){
				LOGGER.debug("Data Grooming is already running on the system");
				return;
			}
		}
		
		LOGGER.debug("Started cron job dataGrooming @ " + dateFormat.format(new Date()));

		Map<String, String> dataGroomingFlagMap = new HashMap<>();
		append("enableautofix" , AAIConfig.get("aai.datagrooming.enableautofix"), dataGroomingFlagMap);
		append("enabledupefixon" , AAIConfig.get("aai.datagrooming.enabledupefixon"), dataGroomingFlagMap);
		append("enabledontfixorphans" , AAIConfig.get("aai.datagrooming.enabledontfixorphans"), dataGroomingFlagMap);
		append("enabletimewindowminutes" , AAIConfig.get("aai.datagrooming.enabletimewindowminutes"), dataGroomingFlagMap);
		append("enableskiphostcheck" , AAIConfig.get("aai.datagrooming.enableskiphostcheck"), dataGroomingFlagMap);
		append("enablesleepminutes" , AAIConfig.get("aai.datagrooming.enablesleepminutes"), dataGroomingFlagMap);
		append("enableedgesonly" , AAIConfig.get("aai.datagrooming.enableedgesonly"), dataGroomingFlagMap);
		append("enableskipedgechecks" , AAIConfig.get("aai.datagrooming.enableskipedgechecks"), dataGroomingFlagMap);
		append("enablemaxfix" , AAIConfig.get("aai.datagrooming.enablemaxfix"), dataGroomingFlagMap);
		append("enabledupecheckoff" , AAIConfig.get("aai.datagrooming.enabledupecheckoff"), dataGroomingFlagMap);
		append("enableghost2checkoff" , AAIConfig.get("aai.datagrooming.enableghost2checkoff"), dataGroomingFlagMap);
		append("enableghost2fixon" , AAIConfig.get("aai.datagrooming.enableghost2fixon"), dataGroomingFlagMap);
		append("enableskipindexupdatefix" , AAIConfig.get("aai.datagrooming.enableskipindexupdatefix"), dataGroomingFlagMap);
		append("enablef" , AAIConfig.get("aai.datagrooming.enablef"), dataGroomingFlagMap);
		append("fvalue" , AAIConfig.get("aai.datagrooming.fvalue"), dataGroomingFlagMap);
		append("timewindowminutesvalue" , AAIConfig.get("aai.datagrooming.timewindowminutesvalue"), dataGroomingFlagMap);
		append("sleepminutesvalue" , AAIConfig.get("aai.datagrooming.sleepminutesvalue"), dataGroomingFlagMap);
		append("maxfixvalue" , AAIConfig.get("aai.datagrooming.maxfixvalue"), dataGroomingFlagMap);
		// Note: singleNodeType parameter is not used when running from the cron

		if(LOGGER.isDebugEnabled()){
			LOGGER.debug("DataGrooming Flag Values : ");
		    dataGroomingFlagMap.forEach((key, val) -> LOGGER.debug("Key: {} Value: {}", key, val));
		}

		List<String> paramsArray  = new ArrayList();
		try {
			if("true".equals(dataGroomingFlagMap.get("enableautofix"))){
				paramsArray.add("-autoFix");
			}
			if("true".equals(dataGroomingFlagMap.get("enabledupefixon"))){
				paramsArray.add("-dupeFixOn");
			}
			if("true".equals(dataGroomingFlagMap.get("enabledontfixorphans"))){
				paramsArray.add("-dontFixOrphans");
			}
			if("true".equals(dataGroomingFlagMap.get("enabletimewindowminutes"))){
				paramsArray.add("-timeWindowMinutes");			
				paramsArray.add(dataGroomingFlagMap.get("timewindowminutesvalue"));
			}
			if("true".equals(dataGroomingFlagMap.get("enableskiphostcheck"))){
				paramsArray.add("-skipHostCheck");
			}

			if("true".equals(dataGroomingFlagMap.get("enablesleepminutes"))) {
				paramsArray.add("-sleepMinutes");		
				paramsArray.add(dataGroomingFlagMap.get("sleepminutesvalue"));
			}
		
			if("true".equals(dataGroomingFlagMap.get("enableedgesonly"))){
				paramsArray.add("-edgesOnly");
			}
			if("true".equals(dataGroomingFlagMap.get("enableskipedgechecks"))) {
				paramsArray.add("-skipEdgeChecks");
			}
		
			if("true".equals(dataGroomingFlagMap.get("enablemaxfix"))) {
				paramsArray.add("-maxFix"); 
				paramsArray.add(dataGroomingFlagMap.get("maxfixvalue"));
			}
			if("true".equals(dataGroomingFlagMap.get("enabledupecheckoff"))){
				paramsArray.add("-dupeCheckOff");
			}
			if("true".equals(dataGroomingFlagMap.get("enableghost2checkoff"))){
				paramsArray.add("-ghost2CheckOff");
			}
			if("true".equals(dataGroomingFlagMap.get("enableghost2fixon"))){
				paramsArray.add("-ghost2FixOn");
			}
			if("true".equals(dataGroomingFlagMap.get("enableskipindexupdatefix"))){
				paramsArray.add("-skipIndexUpdateFix");
			}

			if("true".equals(dataGroomingFlagMap.get("enablef"))) {
				paramsArray.add("-f");
				paramsArray.add(dataGroomingFlagMap.get("fvalue"));
			}
				
            DataGrooming dataGrooming = new DataGrooming(loaderFactory, schemaVersions);
            String[] paramsList = paramsArray.toArray(new String[0]);
            if (AAIConfig.get("aai.cron.enable.dataGrooming").equals("true")) {
				dataGrooming.execute(paramsList);
				System.out.println("returned from main method ");
            }
        }
		catch (Exception e) {
            ErrorLogHelper.logError("AAI_4000", "Exception running cron job for dataGrooming"+LogFormatTools.getStackTop(e));
            LOGGER.debug("AAI_4000", "Exception running cron job for dataGrooming"+LogFormatTools.getStackTop(e));
		} finally {
			LOGGER.debug("Ended cron job dataGrooming @ " + dateFormat.format(new Date()));
		}
		auditLog.logAfter();
	}

	private boolean checkIfDataGroomingIsRunning(){

		Process process = null;

		int count = 0;
		try {
			process = new ProcessBuilder().command("bash", "-c", "ps -ef | grep '[D]ataGrooming'").start();
			InputStream is = process.getInputStream();
			InputStreamReader isr = new InputStreamReader(is);
			BufferedReader br = new BufferedReader(isr);

			while (br.readLine() != null){
			    count++;
			}

			int exitVal = process.waitFor();
			LOGGER.debug("Exit value of the dataGrooming check process: " + exitVal);
		} catch (Exception e) {
			LOGGER.debug("AAI_4000", "Exception running dataGrooming check process "+LogFormatTools.getStackTop(e));
		}

		if(count > 0){
		    return true;
		} else {
			return false;
		}
	}

	private void append(String key, String value, Map<String, String> hashMap){
		hashMap.put(key, value);
	}
}
