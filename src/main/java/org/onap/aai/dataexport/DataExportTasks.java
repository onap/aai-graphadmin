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
package org.onap.aai.dataexport;

import java.io.BufferedReader;
import java.io.File;
import java.io.FileFilter;
import java.io.IOException;
import java.io.InputStream;
import java.io.InputStreamReader;
import java.text.SimpleDateFormat;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Date;
import java.util.List;
import java.util.Map;
import java.util.NavigableMap;
import java.util.TreeMap;
import java.util.regex.Matcher;
import java.util.regex.Pattern;

import org.onap.aai.aailog.logs.AaiScheduledTaskAuditLog;
import org.onap.aai.dbgen.DynamicPayloadGenerator;
import org.onap.aai.edges.EdgeIngestor;
import org.onap.aai.exceptions.AAIException;
import org.onap.aai.introspection.LoaderFactory;
import org.onap.aai.logging.ErrorLogHelper;
import org.onap.aai.logging.LogFormatTools;
import org.onap.aai.setup.SchemaVersions;
import org.onap.aai.util.AAIConfig;
import org.onap.aai.util.AAIConstants;
import org.onap.logging.filter.base.ONAPComponents;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.boot.autoconfigure.condition.ConditionalOnProperty;
import org.springframework.context.annotation.PropertySource;
import org.springframework.scheduling.annotation.Scheduled;
import org.springframework.stereotype.Component;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.apache.commons.io.comparator.LastModifiedFileComparator;
import org.apache.commons.io.filefilter.DirectoryFileFilter;
import org.apache.commons.io.filefilter.FileFileFilter;
import org.apache.commons.io.FileUtils;
import org.apache.commons.io.filefilter.RegexFileFilter;

/**
 * DataExportTasks obtains a graph snapshot and invokes DynamicPayloadGenerator
 *
 */
@Component
@PropertySource("file:${server.local.startpath}/etc/appprops/datatoolscrons.properties")
@ConditionalOnProperty(prefix = "dataexporttask", name = "cron")
public class DataExportTasks {

	private static final Logger LOGGER;

	private final SimpleDateFormat dateFormat = new SimpleDateFormat("HH:mm:ss");

	static {
		System.setProperty("aai.service.name", DataExportTasks.class.getSimpleName());
		LOGGER = LoggerFactory.getLogger(DataExportTasks.class);
	}

	private LoaderFactory loaderFactory;
	private EdgeIngestor edgeIngestor;
	private SchemaVersions schemaVersions;

	@Autowired
	public DataExportTasks(LoaderFactory loaderFactory, EdgeIngestor edgeIngestor, SchemaVersions schemaVersions){
	    this.loaderFactory  = loaderFactory;
	    this.edgeIngestor   = edgeIngestor;
	    this.schemaVersions = schemaVersions;
	}

	/**
	 * Scheduled task to invoke exportTask
	 */
	@Scheduled(cron = "${dataexporttask.cron}" )
	public void export() {

		try {
			exportTask();
		}
		catch (Exception e) {
			ErrorLogHelper.logError("AAI_8002", "Exception while running export "+ LogFormatTools.getStackTop(e));
		}
	}
	/**
	 * The exportTask method.
	 *
	 * @throws AAIException, Exception
	 */
	public void exportTask() throws AAIException, Exception   {
	    AaiScheduledTaskAuditLog auditLog = new AaiScheduledTaskAuditLog();
		auditLog.logBefore("dataExportTask", ONAPComponents.AAI.toString());
		LOGGER.info("Started exportTask: " + dateFormat.format(new Date()));
		try {
			String isDataExportEnabled = AAIConfig.get("aai.dataexport.enable");
		} catch (AAIException ex){
			LOGGER.info("Ended exportTask: " + dateFormat.format(new Date()) + " " + ex.getMessage());
			auditLog.logAfter();
			throw ex;
		}
		if (AAIConfig.get("aai.dataexport.enable").equalsIgnoreCase("false")) {
			LOGGER.debug("Data Export is not enabled");
			return;
		}
		// Check if the process was started via command line
		if (isDataExportRunning()) {
			LOGGER.debug("There is a dataExport process already running");
			return;
		}

		LOGGER.debug("Started exportTask: " + dateFormat.format(new Date()));

		String enableSchemaValidation = AAIConfig.get("aai.dataexport.enable.schema.validation", "false");
		String outputLocation =  AAIConstants.AAI_HOME_BUNDLECONFIG + AAIConfig.get("aai.dataexport.output.location");
		String enableMultipleSnapshots =  AAIConfig.get("aai.dataexport.enable.multiple.snapshots", "false");
		String nodeConfigurationLocation = AAIConstants.AAI_HOME_BUNDLECONFIG + AAIConfig.get("aai.dataexport.node.config.location");
		String inputFilterConfigurationLocation = AAIConstants.AAI_HOME_BUNDLECONFIG + AAIConfig.get("aai.dataexport.input.filter.config.location");
		String enablePartialGraph = AAIConfig.get("aai.dataexport.enable.partial.graph", "true");

		// Check that the output location exist
		File targetDirFile = new File(outputLocation);
		if ( !targetDirFile.exists() ) {
			targetDirFile.mkdir();
		}
		else {
			//Delete any existing payload files
			deletePayload(targetDirFile);
		}

		File snapshot = null;
		String snapshotFilePath = null;
		if ( "false".equalsIgnoreCase(enableMultipleSnapshots)){
			// find the second to latest data snapshot
			snapshot = findSnapshot();
			snapshotFilePath = snapshot != null ?snapshot.getAbsolutePath() : null;
			if ( "true".equalsIgnoreCase (enablePartialGraph) ) {
					String[] command = new String[2];
					command[0] = AAIConstants.AAI_HOME + AAIConstants.AAI_FILESEP + "bin" + AAIConstants.AAI_FILESEP + "dynamicPayloadPartial.sh";
					command[1] = snapshotFilePath;
					runScript(command);
			}
		}
		else {
			snapshotFilePath = findMultipleSnapshots();
		}

		List<String> paramsList = new ArrayList<>();
		paramsList.add("-s");
		paramsList.add(enableSchemaValidation);
		paramsList.add("-o");
		paramsList.add(outputLocation);
		paramsList.add("-m");
		paramsList.add(enableMultipleSnapshots);
		paramsList.add("-n");
		paramsList.add(nodeConfigurationLocation);
		paramsList.add("-i");
		paramsList.add(inputFilterConfigurationLocation);
		paramsList.add("-p");
		paramsList.add(enablePartialGraph);
		paramsList.add("-d");
		paramsList.add(snapshotFilePath);

		LOGGER.debug("paramsList is : " + paramsList);

		String[] paramsArray = paramsList.toArray(new String[0]);
		try {
			DynamicPayloadGenerator.run(loaderFactory, edgeIngestor, schemaVersions, paramsArray, false);
			LOGGER.debug("DynamicPaylodGenerator completed");
			// tar/gzip payload files
			String[] command = new String[1];
			command[0] = AAIConstants.AAI_HOME + AAIConstants.AAI_FILESEP + "bin" + AAIConstants.AAI_FILESEP + "dynamicPayloadArchive.sh";
			runScript(command);
		}
		catch (Exception e) {
			ErrorLogHelper.logError("AAI_8003", LogFormatTools.getStackTop(e));
			LOGGER.debug("Exception running dataExport task " + LogFormatTools.getStackTop(e));
		} finally {
			LOGGER.debug("Ended exportTask: " + dateFormat.format(new Date()));
		}
		LOGGER.info("Ended exportTask: " + dateFormat.format(new Date()));
		auditLog.logAfter();

	}
	/**
	 * The isDataExportRunning method, checks if the data export task was started separately via command line
	 * @return true if another process is running, false if not
	 */
	private static boolean isDataExportRunning(){

		Process process = null;

		int count = 0;
		try {
			process = new ProcessBuilder().command("sh", "-c", "ps -ef | grep '[D]ynamicPayloadGenerator'").start();
			InputStream is = process.getInputStream();
			InputStreamReader isr = new InputStreamReader(is);
			BufferedReader br = new BufferedReader(isr);

			while (br.readLine() != null){
			    count++;
			}

			int exitVal = process.waitFor();
			LOGGER.debug("Check if dataExport is running returned: " + exitVal);
		} catch (Exception e) {
			ErrorLogHelper.logError("AAI_8002", "Exception while running the check to see if dataExport is running  "+ LogFormatTools.getStackTop(e));
			LOGGER.debug("Exception while running the check to see if dataExport is running "+ LogFormatTools.getStackTop(e));
		}

        return count > 0;
	}

	/**
	 * The findSnapshot method tries to find the second to last data snapshot. If it can't find it, it returns the last one.
	 * @return a single snapshot File
	 */
	private static File findSnapshot() {
		String targetDir = AAIConstants.AAI_HOME + AAIConstants.AAI_FILESEP + "logs" + AAIConstants.AAI_FILESEP + "data" +
				AAIConstants.AAI_FILESEP + "dataSnapshots";
		File snapshot = null;
		File targetDirFile = new File(targetDir);

		File[] allFilesArr = targetDirFile.listFiles((FileFilter) FileFileFilter.FILE);
		if ( allFilesArr == null || allFilesArr.length == 0 ) {
			ErrorLogHelper.logError("AAI_8001", "Unable to find data snapshots at " + targetDir);
			LOGGER.debug("Unable to find data snapshots at " + targetDir);
			return (snapshot);
		}
		if ( allFilesArr.length > 1 ) {
			Arrays.sort(allFilesArr, LastModifiedFileComparator.LASTMODIFIED_REVERSE);
			// need to use the second to last modified
			snapshot = allFilesArr[1];
		}
		else {
			snapshot = allFilesArr[0];
		}
		return (snapshot);
	}

	/**
	 * The method findMultipleSnapshots looks in the data snapshots directory for a set of snapshot files that match the pattern.
	 * @return the file name prefix corresponding to the second to last set of snapshots
	 */
	private static String findMultipleSnapshots() {
		String targetDir = AAIConstants.AAI_HOME + AAIConstants.AAI_FILESEP + "logs" + AAIConstants.AAI_FILESEP + "data" +
				AAIConstants.AAI_FILESEP + "dataSnapshots";
		String snapshotName = null;
		File targetDirFile = new File(targetDir);
		TreeMap<String,List<File>> fileMap = new TreeMap<>(String.CASE_INSENSITIVE_ORDER);

		/*dataSnapshot.graphSON.201804022009.P0
		dataSnapshot.graphSON.201804022009.P1
		dataSnapshot.graphSON.201804022009.P2
		dataSnapshot.graphSON.201804022009.P3
		dataSnapshot.graphSON.201804022009.P4*/
		String snapshotPattern = "^.*dataSnapshot\\.graphSON\\.(\\d+)\\.P.*$";
		Pattern p = Pattern.compile (snapshotPattern);

		FileFilter fileFilter = new RegexFileFilter("^.*dataSnapshot\\.graphSON\\.(\\d+)\\.P.*$");
		File[] allFilesArr = targetDirFile.listFiles(fileFilter);

		if ( allFilesArr == null || allFilesArr.length == 0 ) {
			ErrorLogHelper.logError("AAI_8001", "Unable to find data snapshots at " + targetDir);
			LOGGER.debug("Unable to find data snapshots at " + targetDir);
			return (null);
		}

		if ( allFilesArr.length > 1 ) {
			Arrays.sort(allFilesArr, LastModifiedFileComparator.LASTMODIFIED_REVERSE);
			for ( File f : allFilesArr ) {
				// find the second to last group of multiple snapshots
				Matcher m = p.matcher(f.getPath());
				if ( m.matches() ) {
					String g1 = m.group(1);
					LOGGER.debug ("Found group " + g1);
					if ( !fileMap.containsKey(g1) ) {
						ArrayList<File> l = new ArrayList<File>();
						l.add(f);
						fileMap.put(g1, l);
					}
					else {
						List<File> l = fileMap.get(g1);
						l.add(f);
						fileMap.put(g1, l);
					}
				}

			}
			if ( fileMap.size() > 1 ) {
				NavigableMap<String,List<File>> dmap = fileMap.descendingMap();

				Map.Entry<String,List<File>> fentry = dmap.firstEntry();
				LOGGER.debug ("First key in descending map " + fentry.getKey());

				Map.Entry<String,List<File>> lentry = dmap.higherEntry(fentry.getKey());
				LOGGER.debug ("Next key in descending map " + lentry.getKey());

				List<File> l = lentry.getValue();
				snapshotName = l.get(0).getAbsolutePath();
				// Remove the .P* extension
				int lastDot = snapshotName.lastIndexOf('.');
				if ( lastDot > 0 ) {
					snapshotName = snapshotName.substring(0,lastDot);
				}
				else {
					LOGGER.debug("Invalid snapshot file name format " + snapshotName);
					return null;
				}
			}
		}
		else {
			return null;
		}
		return (snapshotName);
	}
	/**
	 * The deletePayload method deletes all the payload files that it finds at targetDirectory
	 * @param targetDirFile the directory that contains payload files
	 * @throws AAIException
	 */
	private static void deletePayload(File targetDirFile) {

		File[] allFilesArr = targetDirFile.listFiles((FileFilter)DirectoryFileFilter.DIRECTORY);
		if ( allFilesArr == null || allFilesArr.length == 0 ) {
			LOGGER.debug("No payload files found at " + targetDirFile.getPath());
			return;
		}
		for ( File f : allFilesArr ) {
			try {
				FileUtils.deleteDirectory(f);
			}
			catch (IOException e) {

				LOGGER.debug("Unable to delete directory " + f.getAbsolutePath() + " " + e.getMessage());
			}

		}

	}
	/**
	 * The runScript method runs a shell script/command with a variable number of arguments
	 * @param script The script/command arguments
	 */
	private static void runScript(String ...script ) {
		Process process = null;
		try {
			process = new ProcessBuilder().command(script).start();
			int exitVal = process.waitFor();
			LOGGER.debug("dynamicPayloadArchive.sh returned: " + exitVal);
		} catch (Exception e) {
			ErrorLogHelper.logError("AAI_8002", "Exception while running dynamicPayloadArchive.sh "+ LogFormatTools.getStackTop(e));
			LOGGER.debug("Exception while running dynamicPayloadArchive.sh" + LogFormatTools.getStackTop(e));
		}

	}
}
