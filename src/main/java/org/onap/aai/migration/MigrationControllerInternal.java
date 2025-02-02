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

package org.onap.aai.migration;

import com.beust.jcommander.JCommander;
import com.beust.jcommander.Parameter;
import org.apache.commons.configuration2.ex.ConfigurationException;
import org.apache.commons.configuration2.PropertiesConfiguration;
import org.apache.commons.configuration2.builder.FileBasedConfigurationBuilder;
import org.apache.commons.configuration2.builder.fluent.Parameters;
import org.apache.commons.lang3.exception.ExceptionUtils;
import org.onap.aai.datasnapshot.DataSnapshot;
import org.onap.aai.db.props.AAIProperties;
import org.onap.aai.dbmap.AAIGraph;
import org.onap.aai.edges.EdgeIngestor;
import org.onap.aai.exceptions.AAIException;
import org.onap.aai.introspection.Loader;
import org.onap.aai.introspection.LoaderFactory;
import org.onap.aai.introspection.ModelType;
import org.onap.aai.serialization.db.EdgeSerializer;
import org.onap.aai.serialization.engines.JanusGraphDBEngine;
import org.onap.aai.serialization.engines.QueryStyle;
import org.onap.aai.serialization.engines.TransactionalGraphEngine;
import org.onap.aai.setup.SchemaVersion;
import org.onap.aai.setup.SchemaVersions;
import org.onap.aai.util.AAIConstants;
import org.onap.aai.util.FormatDate;
import org.reflections8.Reflections;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.slf4j.MDC;

import java.io.File;
import java.io.IOException;
import java.lang.reflect.InvocationTargetException;
import java.nio.file.Files;
import java.nio.file.Path;
import java.util.ArrayList;
import java.util.List;
import java.util.Set;
import java.util.stream.Collectors;

/**
 * Runs a series of migrations from a defined directory based on the presence of
 * the {@link org.onap.aai.migration.Enabled Enabled} annotation
 *
 * It will also write a record of the migrations run to the database.
 */
public class MigrationControllerInternal {

	private Logger logger;
	public static final String VERTEX_TYPE = "migration-list-1707";
	private final List<String> resultsSummary = new ArrayList<>();
	private final List<NotificationHelper> notifications = new ArrayList<>();
	private static final String SNAPSHOT_LOCATION = AAIConstants.AAI_HOME + AAIConstants.AAI_FILESEP + "logs" + AAIConstants.AAI_FILESEP + "data" + AAIConstants.AAI_FILESEP + "migrationSnapshots";

	private LoaderFactory loaderFactory;
	private EdgeIngestor edgeIngestor;
	private EdgeSerializer edgeSerializer;
	private final SchemaVersions schemaVersions;

	public MigrationControllerInternal(LoaderFactory loaderFactory, EdgeIngestor edgeIngestor, EdgeSerializer edgeSerializer, SchemaVersions schemaVersions){
	    this.loaderFactory = loaderFactory;
		this.edgeIngestor = edgeIngestor;
		this.edgeSerializer = edgeSerializer;
		this.schemaVersions = schemaVersions;

	}

	/**
	 * The main method.
	 *
	 * @param args
	 *            the arguments
	 */
	public void run(String[] args) {
		// Set the logging file properties to be used by EELFManager
		System.setProperty("aai.service.name", MigrationController.class.getSimpleName());
		MDC.put("logFilenameAppender", MigrationController.class.getSimpleName());
        logger = LoggerFactory.getLogger(MigrationControllerInternal.class.getSimpleName());

		CommandLineArgs cArgs = new CommandLineArgs();

		JCommander jCommander = new JCommander(cArgs, args);
		jCommander.setProgramName(MigrationController.class.getSimpleName());

		// Set flag to load from snapshot based on the presence of snapshot and
		// graph storage backend of inmemory
		if (cArgs.dataSnapshot != null && !cArgs.dataSnapshot.isEmpty()) {
			try {
				FileBasedConfigurationBuilder<PropertiesConfiguration> builder =
    			new FileBasedConfigurationBuilder<PropertiesConfiguration>(PropertiesConfiguration.class)
    			.configure(new Parameters().properties()
        	.setFileName(cArgs.config));
					PropertiesConfiguration config = builder.getConfiguration();
				// PropertiesConfiguration config = new PropertiesConfiguration(cArgs.config);
				if (config.getString("storage.backend").equals("inmemory")) {
					System.setProperty("snapshot.location", cArgs.dataSnapshot);
					String snapshotLocation =cArgs.dataSnapshot;
					String snapshotDir;
					String snapshotFile;
					int index = snapshotLocation.lastIndexOf("\\");
					if (index == -1){
						//Use default directory path
						snapshotDir =  AAIConstants.AAI_HOME + AAIConstants.AAI_FILESEP + "snapshots";
						snapshotFile = snapshotLocation;
					} else {
						snapshotDir = snapshotLocation.substring(0, index+1);
						snapshotFile = snapshotLocation.substring(index+1) ;
					}
					String [] dataSnapShotArgs = {"-c","MULTITHREAD_RELOAD","-f", snapshotFile, "-oldFileDir",snapshotDir, "-caller","migration"};
					DataSnapshot dataSnapshot = new DataSnapshot();
					dataSnapshot.executeCommand(dataSnapShotArgs, true, false, null, "MULTITHREAD_RELOAD", snapshotFile);
				}
			} catch (ConfigurationException e) {
				logAndPrint("ERROR: Could not load janusgraph configuration.\n" + ExceptionUtils.getStackTrace(e));
				return;
			}
		}
		else {
			System.setProperty("realtime.db.config", cArgs.config);
			logAndPrint("\n\n---------- Connecting to Graph ----------");
			AAIGraph.getInstance();
		}

		logAndPrint("---------- Connection Established ----------");
		SchemaVersion version = schemaVersions.getDefaultVersion();
		QueryStyle queryStyle = QueryStyle.TRAVERSAL;
		ModelType introspectorFactoryType = ModelType.MOXY;
		Loader loader = loaderFactory.createLoaderForVersion(introspectorFactoryType, version);
		TransactionalGraphEngine engine = new JanusGraphDBEngine(queryStyle, loader);

		if (cArgs.help) {
			jCommander.usage();
			engine.rollback();
			return;
		}

		// TODO: do not rely on the transitive reflections dependency of janusgraph-core
		Reflections reflections = new Reflections("org.onap.aai.migration");
		List<Class<? extends Migrator>> migratorClasses = new ArrayList<>(findClasses(reflections));
		//Displays list of migration classes which needs to be executed.Pass flag "-l" following by the class names
		if (cArgs.list) {
			listMigrationWithStatus(migratorClasses, engine);
			return;
		}

		logAndPrint("---------- Looking for migration scripts to be executed. ----------");
		//Excluding any migration class when run migration from script.Pass flag "-e" following by the class names
		if (!cArgs.excludeClasses.isEmpty()) {
			migratorClasses = filterMigrationClasses(cArgs.excludeClasses, migratorClasses);
			listMigrationWithStatus(migratorClasses, engine);
		}
		List<Class<? extends Migrator>> migratorClassesToRun = createMigratorList(cArgs, migratorClasses);

		sortList(migratorClassesToRun);

		if (!cArgs.scripts.isEmpty() && migratorClassesToRun.isEmpty()) {
			logAndPrint("\tERROR: Failed to find migrations " + cArgs.scripts + ".");
			logAndPrint("---------- Done ----------");
		}

		logAndPrint("\tFound " + migratorClassesToRun.size() + " migration scripts.");
		logAndPrint("---------- Executing Migration Scripts ----------");


		if (!cArgs.skipPreMigrationSnapShot) {
			takePreSnapshotIfRequired(engine);
		}

		for (Class<? extends Migrator> migratorClass : migratorClassesToRun) {
			String name = migratorClass.getSimpleName();
			Migrator migrator;
			if (cArgs.runDisabled.contains(name) || migratorClass.isAnnotationPresent(Enabled.class)) {

				try {
					engine.startTransaction();
					if (!cArgs.forced && hasAlreadyRun(name, engine)) {
						logAndPrint("Migration " + name + " has already been run on this database and will not be executed again. Use -f to force execution");
						continue;
					}
					migrator = migratorClass
						.getConstructor(
							TransactionalGraphEngine.class,
							LoaderFactory.class,
							EdgeIngestor.class,
							EdgeSerializer.class,
							SchemaVersions.class
						).newInstance(engine, loaderFactory, edgeIngestor, edgeSerializer,schemaVersions);
				} catch (InstantiationException | IllegalAccessException | IllegalArgumentException | InvocationTargetException | NoSuchMethodException | SecurityException e) {
					logAndPrint("EXCEPTION caught initalizing migration class " + migratorClass.getSimpleName() + ".\n" + ExceptionUtils.getStackTrace(e));
					engine.rollback();
					continue;
				}
				logAndPrint("\tRunning " + migratorClass.getSimpleName() + " migration script.");
				logAndPrint("\t\t See " + System.getProperty("AJSC_HOME") + "/logs/migration/" + migratorClass.getSimpleName() + "/* for logs.");
				MDC.put("logFilenameAppender", migratorClass.getSimpleName() + "/" + migratorClass.getSimpleName());

				migrator.run();

				commitChanges(engine, migrator, cArgs);
			} else {
				logAndPrint("\tSkipping " + migratorClass.getSimpleName() + " migration script because it has been disabled.");
			}
		}
		MDC.put("logFilenameAppender", MigrationController.class.getSimpleName());
		for (NotificationHelper notificationHelper : notifications) {
			try {
				notificationHelper.triggerEvents();
			} catch (AAIException e) {
				logAndPrint("\tcould not event");
				logger.error("could not event", e);
			}
		}
		logAndPrint("---------- Done ----------");

		// Save post migration snapshot if snapshot was loaded
		if (!cArgs.skipPostMigrationSnapShot) {
			generateSnapshot(engine, "post");
		}

		outputResultsSummary();
	}

	/**
	 * This method is used to remove excluded classes from migration from the
	 * script command.
	 *
	 * @param excludeClasses
	 *            : Classes to be removed from Migration
	 * @param migratorClasses
	 *            : Classes to execute migration.
	 * @return
	 */
	private List<Class<? extends Migrator>> filterMigrationClasses(
			List<String> excludeClasses,
			List<Class<? extends Migrator>> migratorClasses) {

		return migratorClasses
				.stream()
				.filter(migratorClass -> !excludeClasses.contains(migratorClass
						.getSimpleName())).collect(Collectors.toList());
	}

	private void listMigrationWithStatus(List<Class<? extends Migrator>> migratorClasses, TransactionalGraphEngine engine) {
			sortList(migratorClasses);
			engine.startTransaction();
			System.out.println("---------- List of all migrations ----------");
			migratorClasses.forEach(migratorClass -> {
				boolean enabledAnnotation = migratorClass.isAnnotationPresent(Enabled.class);
				String enabled = enabledAnnotation ? "Enabled" : "Disabled";
				StringBuilder sb = new StringBuilder();
				sb.append(migratorClass.getSimpleName());
				sb.append(" in package ");
				sb.append(migratorClass.getPackage().getName().substring(migratorClass.getPackage().getName().lastIndexOf('.')+1));
				sb.append(" is ");
				sb.append(enabled);
				sb.append(" ");
				sb.append("[" + getDbStatus(migratorClass.getSimpleName(), engine) + "]");
				System.out.println(sb.toString());
			});
			engine.rollback();
			System.out.println("---------- Done ----------");
		}

	private String getDbStatus(String name, TransactionalGraphEngine engine) {
		if (hasAlreadyRun(name, engine)) {
			return "Already executed in this env";
		}
		return "Will be run on next execution if Enabled";
	}

	private boolean hasAlreadyRun(String name, TransactionalGraphEngine engine) {
		return engine.asAdmin().getReadOnlyTraversalSource().V().has(AAIProperties.NODE_TYPE, VERTEX_TYPE).has(name, true).hasNext();
	}
	private Set<Class<? extends Migrator>> findClasses(Reflections reflections) {
        Set<Class<? extends Migrator>> migratorClasses = reflections.getSubTypesOf(Migrator.class).stream()
                       .filter(clazz -> clazz.isAnnotationPresent(MigrationPriority.class))
                       .collect(Collectors.toSet());
		/*
		 * TODO- Change this to make sure only classes in the specific $release are added in the runList
		 * Or add a annotation like exclude which folks again need to remember to add ??
		 */

		migratorClasses.remove(PropertyMigrator.class);
		migratorClasses.remove(EdgeMigrator.class);
		return migratorClasses;
	}


	private void takePreSnapshotIfRequired(TransactionalGraphEngine engine) {
		generateSnapshot(engine, "pre");

	}


    private List<Class<? extends Migrator>> createMigratorList(CommandLineArgs cArgs,
            List<Class<? extends Migrator>> migratorClasses) {
        List<Class<? extends Migrator>> migratorClassesToRun = new ArrayList<>();
        if (cArgs.scripts.isEmpty()) {
            return migratorClasses;

        }
        for (Class<? extends Migrator> migratorClass : migratorClasses) {
            if (migratorExplicitlySpecified(cArgs, migratorClass.getSimpleName()) || migratorToRunWhenDisabled(cArgs, migratorClass.getSimpleName())) {
                migratorClassesToRun.add(migratorClass);
            }
        }
        return migratorClassesToRun;
    }

    private boolean migratorExplicitlySpecified(CommandLineArgs cArgs, String migratorName){
        return !cArgs.scripts.isEmpty() && cArgs.scripts.contains(migratorName);
    }
    private boolean migratorToRunWhenDisabled(CommandLineArgs cArgs, String migratorName){
        return !cArgs.runDisabled.isEmpty() && cArgs.runDisabled.contains(migratorName);
    }

	private void sortList(List<Class<? extends Migrator>> migratorClasses) {
		migratorClasses.sort((m1, m2) -> {
			try {
				if (m1.getAnnotation(MigrationPriority.class).value() > m2.getAnnotation(MigrationPriority.class).value()) {
					return 1;
				} else if (m1.getAnnotation(MigrationPriority.class).value() < m2.getAnnotation(MigrationPriority.class).value()) {
					return -1;
				} else {
					return m1.getSimpleName().compareTo(m2.getSimpleName());
				}
			} catch (Exception e) {
				return 0;
			}
		});
	}


	private void generateSnapshot(TransactionalGraphEngine engine, String phase) {

		FormatDate fd = new FormatDate("yyyyMMddHHmm", "GMT");
		String dateStr= fd.getDateTime();
		String fileName = SNAPSHOT_LOCATION + File.separator + phase + "Migration." + dateStr + ".graphson";
		logAndPrint("Saving snapshot of graph " + phase + " migration to " + fileName);
		try {

			Path pathToFile = Path.of(fileName);
			if (!pathToFile.toFile().exists()) {
				Files.createDirectories(pathToFile.getParent());
			}
			String [] dataSnapshotArgs = {"-c","THREADED_SNAPSHOT", "-fileName",fileName, "-caller","migration"};
			DataSnapshot dataSnapshot = new DataSnapshot();
			dataSnapshot.executeCommand(dataSnapshotArgs, true, false, null, "THREADED_SNAPSHOT", null);
		} catch (IOException e) {
			logAndPrint("ERROR: Could not write in memory graph to " + phase + "Migration file. \n" + ExceptionUtils.getStackTrace(e));
			engine.rollback();
		}

		logAndPrint( phase + " migration snapshot saved to " + fileName);
	}
	/**
	 * Log and print.
	 *
	 * @param msg
	 *            the msg
	 */
	protected void logAndPrint(String msg) {
		System.out.println(msg);
		logger.debug(msg);
	}

	/**
	 * Commit changes.
	 *
	 * @param engine
	 *            the graph transaction
	 * @param migrator
	 *            the migrator
	 * @param cArgs
	 */
	protected void commitChanges(TransactionalGraphEngine engine, Migrator migrator, CommandLineArgs cArgs) {

		String simpleName = migrator.getClass().getSimpleName();
		String message;
		if (migrator.getStatus().equals(Status.FAILURE)) {
			message = "Migration " + simpleName + " Failed. Rolling back.";
			logAndPrint("\t" + message);
			migrator.rollback();
		} else if (migrator.getStatus().equals(Status.CHECK_LOGS)) {
			message = "Migration " + simpleName + " encountered an anomaly, check logs. Rolling back.";
			logAndPrint("\t" + message);
			migrator.rollback();
		} else {
			MDC.put("logFilenameAppender", simpleName + "/" + simpleName);

			if (cArgs.commit) {
				if (!engine.asAdmin().getTraversalSource().V().has(AAIProperties.NODE_TYPE, VERTEX_TYPE).hasNext()) {
					engine.asAdmin().getTraversalSource().addV(VERTEX_TYPE).property(AAIProperties.NODE_TYPE, VERTEX_TYPE).iterate();
				}
				engine.asAdmin().getTraversalSource().V().has(AAIProperties.NODE_TYPE, VERTEX_TYPE)
				.property(simpleName, true).iterate();
				MDC.put("logFilenameAppender", MigrationController.class.getSimpleName());
				notifications.add(migrator.getNotificationHelper());
				migrator.commit();
				message = "Migration " + simpleName + " Succeeded. Changes Committed.";
				logAndPrint("\t"+ message +"\t");
			} else {
				message = "--commit not specified. Not committing changes for " + simpleName + " to database.";
				logAndPrint("\t" + message);
				migrator.rollback();
			}

		}

		resultsSummary.add(message);

	}

	private void outputResultsSummary() {
		logAndPrint("---------------------------------");
		logAndPrint("-------------Summary-------------");
		for (String result : resultsSummary) {
			logAndPrint(result);
		}
		logAndPrint("---------------------------------");
		logAndPrint("---------------------------------");
	}

}

class CommandLineArgs {

    @Parameter(names = "--help", help = true)
    public boolean help;

    @Parameter(names = "-c", description = "location of configuration file")
    public String config;

    @Parameter(names = "-m", description = "names of migration scripts")
    public List<String> scripts = new ArrayList<>();

    @Parameter(names = "-l", description = "list the status of migrations")
    public boolean list = false;

    @Parameter(names = "-d", description = "location of data snapshot", hidden = true)
    public String dataSnapshot;

    @Parameter(names = "-f", description = "force migrations to be rerun")
    public boolean forced = false;

    @Parameter(names = "--commit", description = "commit changes to graph")
    public boolean commit = false;

    @Parameter(names = "-e", description = "exclude list of migrator classes")
    public List<String> excludeClasses = new ArrayList<>();

    @Parameter(names = "--skipPreMigrationSnapShot", description = "skips taking the PRE migration snapshot")
    public boolean skipPreMigrationSnapShot = false;

    @Parameter(names = "--skipPostMigrationSnapShot", description = "skips taking the POST migration snapshot")
    public boolean skipPostMigrationSnapShot = false;

    @Parameter(names = "--runDisabled", description = "List of migrators which are to be run even when disabled")
    public List<String> runDisabled = new ArrayList<>();

}
