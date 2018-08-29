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
import java.io.IOException;
import java.nio.file.Files;
import java.nio.file.attribute.BasicFileAttributes;
import java.nio.file.attribute.FileTime;
import java.text.SimpleDateFormat;
import java.util.Calendar;
import java.util.Date;
import java.util.zip.ZipEntry;
import java.util.zip.ZipOutputStream;

import org.onap.aai.exceptions.AAIException;
import org.onap.aai.logging.ErrorLogHelper;
import org.onap.aai.util.AAIConfig;
import org.onap.aai.util.AAIConstants;
import org.springframework.context.annotation.PropertySource;
import org.springframework.scheduling.annotation.Scheduled;
import org.springframework.stereotype.Component;

import com.att.eelf.configuration.EELFLogger;
import com.att.eelf.configuration.EELFManager;

@Component
@PropertySource("file:${server.local.startpath}/etc/appprops/datatoolscrons.properties")
public class DataCleanupTasks {

	private static final EELFLogger logger = EELFManager.getInstance().getLogger(DataCleanupTasks.class);
	private final SimpleDateFormat simpleDateFormat = new SimpleDateFormat("yyyyMMdd");
	/**The function archives/deletes files that end in .out (Ie. dataGrooming.201511111305.out) that sit in our log/data directory structure.
		logDir is the {project_home}/logs
		archiveDir is the ARCHIVE directory where the files will be stored after 5 days.
		ageZip is the number of days after which the file will be moved to the ARCHIVE folder.
		ageDelete is the number of days after which the data files will be deleted i.e after 30 days.
	*/
	@Scheduled(cron = "${datagroomingcleanup.cron}" )
	public void dataGroomingCleanup() throws AAIException, Exception {
		
		logger.info("Started cron job dataGroomingCleanup @ " + simpleDateFormat.format(new Date()));
		
		try {
			String logDir = AAIConstants.AAI_HOME + AAIConstants.AAI_FILESEP + "logs";
			String dataGroomingDir = logDir + AAIConstants.AAI_FILESEP + "data" + AAIConstants.AAI_FILESEP + "dataGrooming";
			String archiveDir = dataGroomingDir + AAIConstants.AAI_FILESEP + "ARCHIVE";
			String dataGroomingArcDir = archiveDir + AAIConstants.AAI_FILESEP + "dataGrooming";		
			File path = new File(dataGroomingDir);
			File archivepath = new File(archiveDir);
			File dataGroomingPath = new File(dataGroomingArcDir);
		
			logger.info("The logDir is " + logDir);
			logger.info("The dataGroomingDir is " + dataGroomingDir);
			logger.info("The archiveDir is " + archiveDir );
			logger.info("The dataGroomingArcDir is " + dataGroomingArcDir );
		
			boolean exists = directoryExists(logDir);
			logger.info("Directory" + logDir + "exists: " + exists);
			if(!exists)
				logger.error("The directory" + logDir +"does not exists");
		
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
						logger.info("The file name in dataGrooming: " +listFile.getName()); 
						Date fileCreateDate = fileCreationMonthDate(listFile);
						logger.info("The fileCreateDate in dataGrooming is " + fileCreateDate);
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
				logger.info("The file name in ARCHIVE/dataGrooming: " +listFileArchive.getName()); 
				Date fileCreateDate = fileCreationMonthDate(listFileArchive);
				logger.info("The fileCreateDate in ARCHIVE/dataGrooming is " + fileCreateDate);
				if(fileCreateDate.compareTo(newAgeDelete) < 0) {
					delete(listFileArchive);
					}
				}	
			}
			}
		}
		catch (Exception e) {
			ErrorLogHelper.logError("AAI_4000", "Exception running cron job for DataCleanup"+e.toString());
			logger.info("AAI_4000", "Exception running cron job for DataCleanup"+e.toString());
			throw e;
		}
	}
	
    /**
     * This method checks if the directory exists
     * @param DIR
     * 
     */
    public boolean directoryExists(String dir) {
    	File path = new File(dir);
		boolean exists = path.exists();
		return exists;	
    }
    
    public Date getZipDate(Integer days) throws Exception {
    	return getZipDate(days, new Date());
    }
    
    public Date getZipDate(Integer days, Date date) throws Exception{
    	
    	Calendar cal = Calendar.getInstance();
    	logger.info("The current date is " + date );
    	cal.setTime(date);	
    	cal.add(Calendar.DATE, -days);
    	Date newAgeZip = cal.getTime();
		logger.info("The newAgeDate is " +newAgeZip);
		return newAgeZip;		
    }
    
    
    public Date fileCreationMonthDate (File file) throws Exception {

        BasicFileAttributes attr = Files.readAttributes(file.toPath(),
                                                        BasicFileAttributes.class);
        FileTime time = attr.creationTime();
	    String formatted = simpleDateFormat.format( new Date( time.toMillis() ) );
	    Date d = simpleDateFormat.parse(formatted);
	    return d;
    }
    
    /**
     * This method will zip the files and add it to the archive folder
     * Checks if the archive folder exists, if not then creates one
     * After adding the file to archive folder it deletes the file from the filepath
     * @throws AAIException
     * @throws Exception
     */
    public void archive(File file, String archiveDir, String afterArchiveDir) throws AAIException, Exception {
		
    	logger.info("Inside the archive folder");  
    	String filename = file.getName();
    	logger.info("file name is " +filename);
		File archivepath = new File(archiveDir);
		
		String zipFile = afterArchiveDir + AAIConstants.AAI_FILESEP + filename;
		
		File dataGroomingPath = new File(afterArchiveDir);
	
		boolean exists = directoryExists(archiveDir);
		logger.info("Directory" + archiveDir + "exists: " + exists);		
		if(!exists) {
			logger.error("The directory" + archiveDir +"does not exists so will create a new archive folder");
			//Create an archive folder if does not exists		
			boolean flag = dataGroomingPath.mkdirs();
			if(!flag)
				logger.error("Failed to create ARCHIVE folder");		
		}
		try(FileOutputStream outputstream = new FileOutputStream(zipFile + ".gz");
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
			logger.info("The file archived is " + file + " at " + afterArchiveDir );
		}	
	 catch (IOException e) {
		 ErrorLogHelper.logError("AAI_4000", "Exception running cron job for DataCleanup " + e.getStackTrace());
		 logger.info("AAI_4000", "Exception running cron job for DataCleanup", e);
		 throw e;
	 	}
    }
    
    /**
     * This method will delete all the files from the archive folder that are older than 60 days
     * @param file
     */
    public static void delete(File file) {
    	
    	logger.info("Deleting the file " + file);
    	boolean deleteStatus = file.delete();
		if(!deleteStatus){
			logger.error("Failed to delete the file" +file);			
		}
    }
    
    /**The function archives/deletes files that end in .out (Ie. dataGrooming.201511111305.out) that sit in our log/data directory structure.
	logDir is the {project_home}/logs
	archiveDir is the ARCHIVE directory where the files will be stored after 5 days.
	ageZip is the number of days after which the file will be moved to the ARCHIVE folder.
	ageDelete is the number of days after which the data files will be deleted i.e after 30 days.
*/
    @Scheduled(cron = "${datasnapshotcleanup.cron}" )
    public void dataSnapshotCleanup() throws AAIException, Exception {
	
    	logger.info("Started cron job dataSnapshotCleanup @ " + simpleDateFormat.format(new Date()));
	
    	try {
    		String logDir = AAIConstants.AAI_HOME + AAIConstants.AAI_FILESEP + "logs";
    		String dataSnapshotDir = logDir + AAIConstants.AAI_FILESEP + "data" + AAIConstants.AAI_FILESEP + "dataSnapshots";
    		String archiveDir = dataSnapshotDir + AAIConstants.AAI_FILESEP + "ARCHIVE";
    		String dataSnapshotArcDir = archiveDir + AAIConstants.AAI_FILESEP + "dataSnapshots";		
    		File path = new File(dataSnapshotDir);
    		File archivepath = new File(archiveDir);
    		File dataSnapshotPath = new File(dataSnapshotArcDir);
	
    		logger.info("The logDir is " + logDir);
    		logger.info("The dataSnapshotDir is " + dataSnapshotDir);
    		logger.info("The archiveDir is " + archiveDir );
    		logger.info("The dataSnapshotArcDir is " + dataSnapshotArcDir );
	
    		boolean exists = directoryExists(logDir);
    		logger.info("Directory" + logDir + "exists: " + exists);
    		if(!exists)
    			logger.error("The directory" + logDir +"does not exists");
	
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
    					logger.info("The file name in dataSnapshot: " +listFile.getName()); 
    					Date fileCreateDate = fileCreationMonthDate(listFile);
    					logger.info("The fileCreateDate in dataSnapshot is " + fileCreateDate);
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
    					logger.info("The file name in ARCHIVE/dataSnapshot: " +listFileArchive.getName()); 
    					Date fileCreateDate = fileCreationMonthDate(listFileArchive);
    					logger.info("The fileCreateDate in ARCHIVE/dataSnapshot is " + fileCreateDate);
    					if(fileCreateDate.compareTo(newAgeDelete) < 0) {
    						delete(listFileArchive);
    					}
    				}	
    			}
    		}
	}
	catch (Exception e) {
		ErrorLogHelper.logError("AAI_4000", "Exception running cron job for DataCleanup"+e.toString());
		logger.info("AAI_4000", "Exception running cron job for DataCleanup"+e.toString());
		throw e;
	}
  }   
}
