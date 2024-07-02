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
package org.onap.aai.datasnapshot;

import java.io.BufferedReader;
import java.io.InputStream;
import java.io.InputStreamReader;
import java.text.SimpleDateFormat;
import java.util.*;

import org.onap.aai.aailog.logs.AaiScheduledTaskAuditLog;
import org.onap.aai.datagrooming.DataGrooming;
import org.onap.aai.datagrooming.DataGroomingTasks;
import org.onap.aai.exceptions.AAIException;
import org.onap.aai.logging.ErrorLogHelper;
import org.onap.aai.logging.LogFormatTools;
import org.onap.aai.util.AAIConfig;
import org.onap.logging.filter.base.ONAPComponents;
import org.onap.logging.ref.slf4j.ONAPLogConstants;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.context.annotation.PropertySource;
import org.springframework.scheduling.annotation.Scheduled;
import org.springframework.stereotype.Component;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.slf4j.MDC;

@Component
@PropertySource("file:${server.local.startpath}/etc/appprops/datatoolscrons.properties")
public class DataSnapshotTasks {

	private AaiScheduledTaskAuditLog auditLog;

	private static final Logger LOGGER = LoggerFactory.getLogger(DataSnapshotTasks.class);
	private final SimpleDateFormat dateFormat = new SimpleDateFormat("HH:mm:ss");


	@Scheduled(cron = "${datasnapshottasks.cron}" )
	public void snapshotScheduleTask() throws AAIException, Exception {
		auditLog = new AaiScheduledTaskAuditLog();
		auditLog.logBefore("dataSnapshotTask", ONAPComponents.AAI.toString() );
		if(!"true".equals(AAIConfig.get("aai.disable.check.snapshot.running", "false"))){
			if(checkIfDataSnapshotIsRunning()){
				LOGGER.debug("Data Snapshot is already running on the system");
				return;
			}
		}
		LOGGER.debug("Started cron job dataSnapshot @ " + dateFormat.format(new Date()));
		try {
			if (AAIConfig.get("aai.cron.enable.dataSnapshot").equals("true")) {
				String [] dataSnapshotParms = {"-c",AAIConfig.get("aai.datasnapshot.params",  "JUST_TAKE_SNAPSHOT")};
				LOGGER.debug("DataSnapshot Params {}", Arrays.toString(dataSnapshotParms));
				DataSnapshot.main(dataSnapshotParms);
			}
		}
		catch (Exception e) {
			ErrorLogHelper.logError("AAI_4000", "Exception running cron job for DataSnapshot"+LogFormatTools.getStackTop(e));
			LOGGER.debug("AAI_4000", "Exception running cron job for DataSnapshot"+LogFormatTools.getStackTop(e));
		} finally {
			LOGGER.debug("Ended cron job dataSnapshot @ " + dateFormat.format(new Date()));
		}
		auditLog.logAfter();

	}

	private boolean checkIfDataSnapshotIsRunning(){

		Process process = null;

		int count = 0;
		try {
			process = new ProcessBuilder().command("sh", "-c", "ps -ef | grep '[D]ataSnapshot'").start();
			InputStream is = process.getInputStream();
			InputStreamReader isr = new InputStreamReader(is);
			BufferedReader br = new BufferedReader(isr);
			while (br.readLine() != null){
				count++;
			}

			int exitVal = process.waitFor();
			LOGGER.debug("Exit value of the dataSnapshot check process: " + exitVal);
		} catch (Exception e) {
			ErrorLogHelper.logError("AAI_4000", "Exception in checkIfDataSnapshotIsRunning" + LogFormatTools.getStackTop(e));
		}

		return count > 0;
	}
}
