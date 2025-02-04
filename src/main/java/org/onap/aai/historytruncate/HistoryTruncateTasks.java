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
package org.onap.aai.historytruncate;

import java.io.BufferedReader;
import java.io.InputStream;
import java.io.InputStreamReader;
import java.text.SimpleDateFormat;
import java.util.*;

import org.onap.aai.aailog.logs.AaiScheduledTaskAuditLog;
import org.onap.aai.exceptions.AAIException;
import org.onap.aai.logging.ErrorLogHelper;
import org.onap.aai.logging.LogFormatTools;
import org.onap.aai.util.AAIConfig;
import org.onap.logging.filter.base.ONAPComponents;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.context.annotation.PropertySource;
import org.springframework.scheduling.annotation.Scheduled;
import org.springframework.stereotype.Component;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
@Component
@PropertySource("file:${server.local.startpath}/etc/appprops/datatoolscrons.properties")
public class HistoryTruncateTasks {

	@Autowired
    private AaiScheduledTaskAuditLog auditLog;

	private static final Logger LOGGER = LoggerFactory.getLogger(HistoryTruncateTasks.class);
	private final SimpleDateFormat dateFormat = new SimpleDateFormat("HH:mm:ss");

	@Scheduled(cron = "${historytruncatetasks.cron}" )
	public void historyTruncateScheduleTask() throws AAIException, Exception {

		if(!"true".equals(AAIConfig.get("aai.disable.check.historytruncate.running", "false"))){
			if(checkIfHistoryTruncateIsRunning()){
				LOGGER.debug("History Truncate is already running on the system");
				return;
			}
		}

		auditLog.logBefore("historyTruncateTask", ONAPComponents.AAI.toString() );
		LOGGER.debug("Started cron job HistoryTruncate @ " + dateFormat.format(new Date()));
		try {
			if (AAIConfig.get("aai.cron.enable.historytruncate").equals("true")) {
				// Until we're comfortable with how it is working, we will keep it in "LOG_ONLY" mode
				String defaultTruncMode = "LOG_ONLY";
				String defaultTruncWindowDays = "999";
				String [] params = {"-truncateMode",defaultTruncMode,"-truncateWindowDays",defaultTruncWindowDays};
				HistoryTruncate.main(params);
			}
		}
		catch (Exception e) {
			ErrorLogHelper.logError("AAI_4000", "Exception running cron job for HistoryTruncate "+LogFormatTools.getStackTop(e));
			LOGGER.debug("AAI_4000", "Exception running cron job for HistoryTruncate "+LogFormatTools.getStackTop(e));
		} finally {
			LOGGER.debug("Ended cron job historyTruncate @ " + dateFormat.format(new Date()));
		}
		auditLog.logAfter();

	}

	private boolean checkIfHistoryTruncateIsRunning(){

		Process process = null;

		int count = 0;
		try {
			process = new ProcessBuilder().command("sh", "-c", "ps -ef | grep '[H]istoryTruncate'").start();
			InputStream is = process.getInputStream();
			InputStreamReader isr = new InputStreamReader(is);
			BufferedReader br = new BufferedReader(isr);
			while (br.readLine() != null){
				count++;
			}

			int exitVal = process.waitFor();
			LOGGER.debug("Exit value of the historyTruncate check process: " + exitVal);
		} catch (Exception e) {
			LOGGER.debug("Exception in checkIfHistoryTruncateIsRunning" + LogFormatTools.getStackTop(e));
		}

		return count > 0;
	}
}
