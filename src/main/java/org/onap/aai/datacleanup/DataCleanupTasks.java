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
package org.onap.aai.datacleanup;
import java.io.File;
import java.io.FileInputStream;
import java.io.FileOutputStream;
import java.nio.file.Files;
import java.nio.file.attribute.BasicFileAttributes;
import java.nio.file.attribute.FileTime;
import java.text.SimpleDateFormat;
import java.util.Calendar;
import java.util.Date;
import java.util.zip.ZipEntry;
import java.util.zip.ZipOutputStream;

import org.onap.aai.aailog.logs.AaiScheduledTaskAuditLog;
import org.onap.aai.logging.ErrorLogHelper;
import org.onap.aai.logging.LogFormatTools;
import org.onap.aai.exceptions.AAIException;
import org.onap.aai.util.AAIConfig;
import org.onap.aai.util.AAIConstants;
import org.onap.logging.filter.base.ONAPComponents;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.context.annotation.PropertySource;
import org.springframework.scheduling.annotation.Scheduled;
import org.springframework.stereotype.Component;

@Component
@PropertySource("file:${server.local.startpath}/etc/appprops/datatoolscrons.properties")
public class DataCleanupTasks {

	@Value("#{new Boolean('${datagroomingcleanup.enabled:true}')}")
	private Boolean groomingCleanupEnabled;

	@Value("#{new Boolean('${datasnapshotcleanup.enabled:true}')}")
	private Boolean snapshotCleanupEnabled;

	@Autowired
  private AaiScheduledTaskAuditLog auditLog;

	private static final Logger logger = LoggerFactory.getLogger(DataCleanupTasks.class);
	private final SimpleDateFormat simpleDateFormat = new SimpleDateFormat("yyyyMMdd");

	/**The function archives/deletes files that end in .out (Ie. dataGrooming.201511111305.out) that sit in our log/data directory structure.
		logDir is the {project_home}/logs
		archiveDir is the ARCHIVE directory where the files will be stored after 5 days.
		ageZip is the number of days after which the file will be moved to the ARCHIVE folder.
		ageDelete is the number of days after which the data files will be deleted i.e after 30 days.
	*/
	@Scheduled(cron = "${datagroomingcleanup.cron}" )
	public void dataGroomingCleanup() {
		if(groomingCleanupEnabled != null && !groomingCleanupEnabled) {
			logger.info("Skipping the scheduled grooming cleanup task since datagroomingcleanup.enabled=false");
			return;
		}

		auditLog.logBefore("dataGroomingCleanup", ONAPComponents.AAI.toString() );

		logger.debug("Started cron job dataGroomingCleanup @ " + simpleDateFormat.format(new Date()));

		try {
			String logDir = AAIConstants.AAI_HOME + AAIConstants.AAI_FILESEP + "logs";
			String dataGroomingDir = logDir + AAIConstants.AAI_FILESEP + "data" + AAIConstants.AAI_FILESEP + "dataGrooming";
			String archiveDir = dataGroomingDir + AAIConstants.AAI_FILESEP + "ARCHIVE";
			String dataGroomingArcDir = archiveDir + AAIConstants.AAI_FILESEP + "dataGrooming";
			File path = new File(dataGroomingDir);
			File archivepath = new File(archiveDir);
			File dataGroomingPath = new File(dataGroomingArcDir);

			logger.debug("The logDir is " + logDir);
			logger.debug("The dataGroomingDir is " + dataGroomingDir);
			logger.debug("The archiveDir is " + archiveDir );
			logger.debug("The dataGroomingArcDir is " + dataGroomingArcDir );

			boolean exists = directoryExists(logDir);
			logger.debug("Directory" + logDir + "exists: " + exists);
			if(!exists)
				logger.debug("The directory" + logDir +"does not exists");

			Integer ageZip = AAIConfig.getInt("aai.datagrooming.agezip");
			Integer ageDelete = AAIConfig.getInt("aai.datagrooming.agedelete");

			Date newAgeZip = getZipDate(ageZip);

			//Iterate through the dataGroomingDir
			File[] listFiles = path.listFiles();
			if(listFiles != null) {
				for(File listFile : listFiles) {
					if (listFile.toString().contains("ARCHIVE")){
						continue;
					}
					if(listFile.isFile()){
						logger.debug("The file name in dataGrooming: " +listFile.getName());
						Date fileCreateDate = fileCreationMonthDate(listFile);
						logger.debug("The fileCreateDate in dataGrooming is " + fileCreateDate);
						if( fileCreateDate.compareTo(newAgeZip) < 0) {
						archive(listFile,archiveDir,dataGroomingArcDir);
						}
					}
				}
			}

			Date newAgeDelete = getZipDate(ageDelete);
			//Iterate through the archive/dataGrooming dir
			File[] listFilesArchive = dataGroomingPath.listFiles();
			if(listFilesArchive != null) {
				for(File listFileArchive : listFilesArchive) {
					if(listFileArchive.isFile()) {
				logger.debug("The file name in ARCHIVE/dataGrooming: " +listFileArchive.getName());
				Date fileCreateDate = fileCreationMonthDate(listFileArchive);
				logger.debug("The fileCreateDate in ARCHIVE/dataGrooming is " + fileCreateDate);
				if(fileCreateDate.compareTo(newAgeDelete) < 0) {
					delete(listFileArchive);
					}
				}
			}
			}
		}
		catch (Exception e) {
			ErrorLogHelper.logError("AAI_4000", "Exception running cron job for DataCleanup"+LogFormatTools.getStackTop(e));
			logger.debug("AAI_4000", "Exception running cron job for DataCleanup"+LogFormatTools.getStackTop(e));
		}
		logger.debug("Ended cron job dataGroomingCleanup @ " + simpleDateFormat.format(new Date()));
		auditLog.logAfter();
	}

    /**
     * This method checks if the directory exists
     * @param dir the Directory
     *
     */
    public boolean directoryExists(String dir) {
    	File path = new File(dir);
		boolean exists = path.exists();
		return exists;
    }

    public Date getZipDate(Integer days) {
    	return getZipDate(days, new Date());
    }

    public Date getZipDate(Integer days, Date date) {

    	Calendar cal = Calendar.getInstance();
    	logger.debug("The current date is " + date );
    	cal.setTime(date);
    	cal.add(Calendar.DATE, -days);
    	Date newAgeZip = cal.getTime();
		logger.debug("The newAgeDate is " +newAgeZip);
		return newAgeZip;
    }


    public Date fileCreationMonthDate (File file) throws Exception {

        BasicFileAttributes attr = Files.readAttributes(file.toPath(),
                                                        BasicFileAttributes.class);
        FileTime time = attr.creationTime();
	    String formatted = simpleDateFormat.format( new Date( time.toMillis() ) );
	    return simpleDateFormat.parse(formatted);
    }

    /**
     * This method will zip the files and add it to the archive folder
     * Checks if the archive folder exists, if not then creates one
     * After adding the file to archive folder it deletes the file from the filepath
     * @throws Exception
     */
    public void archive(File file, String archiveDir, String afterArchiveDir) throws Exception {

    	logger.debug("Inside the archive folder");
    	String filename = file.getName();
    	logger.debug("file name is " +filename);

		String zipFile = afterArchiveDir + AAIConstants.AAI_FILESEP + filename;

		File dataGroomingPath = new File(afterArchiveDir);

		boolean exists = directoryExists(archiveDir);
		logger.debug("Directory" + archiveDir + "exists: " + exists);
		if(!exists) {
			logger.debug("The directory" + archiveDir +"does not exists so will create a new archive folder");
			//Create an archive folder if does not exists
			boolean flag = dataGroomingPath.mkdirs();
			if(!flag)
				logger.debug("Failed to create ARCHIVE folder");
		}
		try(FileOutputStream outputstream = new FileOutputStream(zipFile + ".zip");
				ZipOutputStream zoutputstream = new ZipOutputStream(outputstream);
				FileInputStream inputstream = new FileInputStream(file)) {
			ZipEntry ze = new ZipEntry(file.getName());
			zoutputstream.putNextEntry(ze);
			byte[] buffer = new byte[1024];
			int len;
			while ((len = inputstream.read(buffer)) > 0) {
				zoutputstream.write(buffer,0,len);
			}
			//close all the sources
			zoutputstream.closeEntry();
			//Delete the file after been added to archive folder
			delete(file);
			logger.debug("The file archived is " + file + " at " + afterArchiveDir );
		}
    }

    /**
     * This method will delete all the files from the archive folder that are older than 60 days
     * @param file
     */
    public static void delete(File file) {

    	logger.debug("Deleting the file " + file);
    	boolean deleteStatus = file.delete();
		if(!deleteStatus){
			logger.debug("Failed to delete the file" +file);
		}
    }

    /**The function archives/deletes files that end in .out (Ie. dataGrooming.201511111305.out) that sit in our log/data directory structure.
	logDir is the {project_home}/logs
	archiveDir is the ARCHIVE directory where the files will be stored after 5 days.
	ageZip is the number of days after which the file will be moved to the ARCHIVE folder.
	ageDelete is the number of days after which the data files will be deleted i.e after 30 days.
*/
    @Scheduled(cron = "${datasnapshotcleanup.cron}" )
    public void dataSnapshotCleanup() {
			if(snapshotCleanupEnabled != null && !snapshotCleanupEnabled) {
				logger.info("Skipping the scheduled snapshot cleanup task since datasnapshotcleanup.enabled=false");
				return;
			}

			auditLog.logBefore("dataSnapshotCleanup", ONAPComponents.AAI.toString() );

			logger.debug("Started cron job dataSnapshotCleanup @ " + simpleDateFormat.format(new Date()));

    	try {
    		String logDir = AAIConstants.AAI_HOME + AAIConstants.AAI_FILESEP + "logs";
    		String dataSnapshotDir = logDir + AAIConstants.AAI_FILESEP + "data" + AAIConstants.AAI_FILESEP + "dataSnapshots";
    		String archiveDir = dataSnapshotDir + AAIConstants.AAI_FILESEP + "ARCHIVE";
    		String dataSnapshotArcDir = archiveDir + AAIConstants.AAI_FILESEP + "dataSnapshots";
    		File path = new File(dataSnapshotDir);
    		File dataSnapshotPath = new File(dataSnapshotArcDir);

    		logger.debug("The logDir is " + logDir);
    		logger.debug("The dataSnapshotDir is " + dataSnapshotDir);
    		logger.debug("The archiveDir is " + archiveDir );
    		logger.debug("The dataSnapshotArcDir is " + dataSnapshotArcDir );

    		boolean exists = directoryExists(logDir);
    		logger.debug("Directory" + logDir + "exists: " + exists);
    		if(!exists)
    			logger.debug("The directory" + logDir +"does not exists");

    		Integer ageZipSnapshot = AAIConfig.getInt("aai.datasnapshot.agezip");
    		Integer ageDeleteSnapshot = AAIConfig.getInt("aai.datasnapshot.agedelete");

    		Date newAgeZip = getZipDate(ageZipSnapshot);

    		//Iterate through the dataGroomingDir
    		File[] listFiles = path.listFiles();
    		if(listFiles != null) {
    			for(File listFile : listFiles) {
    				if (listFile.toString().contains("ARCHIVE")){
					continue;
    				}
    				if(listFile.isFile()){
    					logger.debug("The file name in dataSnapshot: " +listFile.getName());
    					Date fileCreateDate = fileCreationMonthDate(listFile);
    					logger.debug("The fileCreateDate in dataSnapshot is " + fileCreateDate);
    					if( fileCreateDate.compareTo(newAgeZip) < 0) {
    						archive(listFile,archiveDir,dataSnapshotArcDir);
    					}
    				}
    			}
    		}

    		Date newAgeDelete = getZipDate(ageDeleteSnapshot);
    		//Iterate through the archive/dataSnapshots dir
    		File[] listFilesArchive = dataSnapshotPath.listFiles();
    		if(listFilesArchive != null) {
    			for(File listFileArchive : listFilesArchive) {
    				if(listFileArchive.isFile()) {
    					logger.debug("The file name in ARCHIVE/dataSnapshot: " +listFileArchive.getName());
    					Date fileCreateDate = fileCreationMonthDate(listFileArchive);
    					logger.debug("The fileCreateDate in ARCHIVE/dataSnapshot is " + fileCreateDate);
    					if(fileCreateDate.compareTo(newAgeDelete) < 0) {
    						delete(listFileArchive);
    					}
    				}
    			}
    		}
    		dmaapEventsDataCleanup(newAgeDelete);
    		dataMigrationCleanup();
	}
	catch (Exception e) {
		ErrorLogHelper.logError("AAI_4000", "Exception running cron job for DataCleanup"+LogFormatTools.getStackTop(e));
		logger.debug("AAI_4000", "Exception running cron job for DataCleanup"+LogFormatTools.getStackTop(e));
	}
    logger.debug("Ended cron job dataSnapshotCleanup @ " + simpleDateFormat.format(new Date()));
	auditLog.logAfter();
  }
	public void dmaapEventsDataCleanup(Date deleteAge) {

		logger.debug("Started dmaapEventsDataCleanup @ " + simpleDateFormat.format(new Date()));

		try {
			String logDir = AAIConstants.AAI_HOME + AAIConstants.AAI_FILESEP + "logs";
			String dmaapEventsDataDir = logDir + AAIConstants.AAI_FILESEP + "data" + AAIConstants.AAI_FILESEP + "dmaapEvents";
			File path = new File(dmaapEventsDataDir);

			logger.debug("The logDir is " + logDir);
			logger.debug("The dmaapEventsDataDir is " + dmaapEventsDataDir);

			//Iterate through the files
			File[] listFiles = path.listFiles();
			if(listFiles != null) {
				for(File listFile : listFiles) {
					if(listFile.isFile()){
						logger.debug("The file name in dmaapEvents is: " +listFile.getName());
						Date fileCreateDate = fileCreationMonthDate(listFile);
						logger.debug("The fileCreateDate in dmaapEvents is " + fileCreateDate);
						if( fileCreateDate.compareTo(deleteAge) < 0) {
							delete(listFile);
							logger.debug("Deleted " + listFile.getName());
						}
					}
				}
			}

		}
		catch (Exception e) {
			ErrorLogHelper.logError("AAI_4000", "Exception in dmaapEventsDataCleanup");
			logger.debug("AAI_4000", "Exception in dmaapEventsDataCleanup "+LogFormatTools.getStackTop(e));
		}
		logger.debug("Ended cron dmaapEventsDataCleanup @ " + simpleDateFormat.format(new Date()));
	}

    public void dataMigrationCleanup() throws AAIException {
		Integer ageDeleteSnapshot = AAIConfig.getInt("aai.datamigration.agedelete");

		Date deleteAge = getZipDate(ageDeleteSnapshot);

		logger.debug("Started dataMigrationCleanup @ " + simpleDateFormat.format(new Date()));

    	try {
    		String logDir = AAIConstants.AAI_HOME + AAIConstants.AAI_FILESEP + "logs";
    		String dataMigrationCleanupDir = logDir + AAIConstants.AAI_FILESEP + "data" + AAIConstants.AAI_FILESEP + "migration-input-files";
    		File path = new File(dataMigrationCleanupDir);

    		logger.debug("The logDir is " + logDir);
			logger.debug("The migrationInputFilesDir is " + dataMigrationCleanupDir);

			//Iterate through the files
			File[] listFiles = path.listFiles();
			if(listFiles != null) {
				for(File listFile : listFiles) {
					if(listFile.isFile()){
						logger.debug("The file name in migration-input-files is: " +listFile.getName());
						Date fileCreateDate = fileCreationMonthDate(listFile);
						logger.debug("The fileCreateDate in migration-input-files is " + fileCreateDate);
						if( fileCreateDate.compareTo(deleteAge) < 0) {
							delete(listFile);
							logger.debug("Deleted " + listFile.getName());
						}
					}
				}
			}

		}
		catch (Exception e) {
			ErrorLogHelper.logError("AAI_4000", "Exception in dataMigrationCleanup");
			logger.debug("AAI_4000", "Exception in dataMigrationCleanup "+LogFormatTools.getStackTop(e));
		}
		logger.debug("Ended cron dataMigrationCleanup @ " + simpleDateFormat.format(new Date()));
	}
}
