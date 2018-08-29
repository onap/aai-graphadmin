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

import org.onap.aai.datagrooming.DataGrooming;
import org.onap.aai.datagrooming.DataGroomingTasks;
import org.onap.aai.exceptions.AAIException;
import org.onap.aai.logging.ErrorLogHelper;
import org.onap.aai.logging.LoggingContext;
import org.onap.aai.util.AAIConfig;
import org.springframework.context.annotation.PropertySource;
import org.springframework.scheduling.annotation.Scheduled;
import org.springframework.stereotype.Component;
import com.att.eelf.configuration.EELFLogger;
import com.att.eelf.configuration.EELFManager;

@Component
@PropertySource("file:${server.local.startpath}/etc/appprops/datatoolscrons.properties")
public class DataSnapshotTasks {

	private static final EELFLogger LOGGER = EELFManager.getInstance().getLogger(DataSnapshotTasks.class);
	private final SimpleDateFormat dateFormat = new SimpleDateFormat("HH:mm:ss");
	
	@Scheduled(cron = "${datasnapshottasks.cron}" )
	public void snapshotScheduleTask() throws AAIException, Exception {

		LoggingContext.init();
		LoggingContext.requestId(UUID.randomUUID().toString());
		LoggingContext.partnerName("AAI");
		LoggingContext.targetEntity("CronApp");
		LoggingContext.component("dataSnapshot");
		LoggingContext.serviceName("snapshotScheduleTask");
		LoggingContext.targetServiceName("snapshotScheduleTask");
		LoggingContext.statusCode(LoggingContext.StatusCode.COMPLETE);

		if(!"true".equals(AAIConfig.get("aai.disable.check.snapshot.running", "false"))){
			if(checkIfDataSnapshotIsRunning()){
				LOGGER.info("Data Snapshot is already running on the system");
				return;
			}
		}

		LOGGER.info("Started cron job dataSnapshot @ " + dateFormat.format(new Date()));
		try {
			if (AAIConfig.get("aai.cron.enable.dataSnapshot").equals("true")) {
				String [] dataSnapshotParms = AAIConfig.get("aai.datasnapshot.params",  "JUST_TAKE_SNAPSHOT").split("\\s+");
				LOGGER.info("DataSnapshot Params {}", Arrays.toString(dataSnapshotParms));
				DataSnapshot.main(dataSnapshotParms);
			}
		}
		catch (Exception e) {
			ErrorLogHelper.logError("AAI_4000", "Exception running cron job for DataSnapshot"+e.toString());
			LOGGER.info("AAI_4000", "Exception running cron job for DataSnapshot"+e.toString());
			throw e;
		} finally {
			LOGGER.info("Ended cron job dataSnapshot @ " + dateFormat.format(new Date()));
			LoggingContext.clear();
		}

	}

	private boolean checkIfDataSnapshotIsRunning(){

		Process process = null;

		int count = 0;
		try {
			process = new ProcessBuilder().command("bash", "-c", "ps -ef | grep '[D]ataSnapshot'").start();
			InputStream is = process.getInputStream();
			InputStreamReader isr = new InputStreamReader(is);
			BufferedReader br = new BufferedReader(isr);
			while (br.readLine() != null){
				count++;
			}

			int exitVal = process.waitFor();
			LOGGER.info("Exit value of the dataSnapshot check process: " + exitVal);
		} catch (Exception e) {
			LOGGER.error("Exception in checkIfDataSnapshotIsRunning",e);
		}

		return count > 0;
	}
}
		
	