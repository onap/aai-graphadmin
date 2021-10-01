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
package org.onap.aai.migration.v15;

import java.io.FileNotFoundException;
import java.io.IOException;
import java.nio.file.Files;
import java.nio.file.Paths;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.atomic.AtomicInteger;

import java.util.stream.Collectors;
import org.apache.tinkerpop.gremlin.process.traversal.dsl.graph.GraphTraversalSource;
import org.apache.tinkerpop.gremlin.structure.Vertex;
import org.javatuples.Pair;
import org.onap.aai.db.props.AAIProperties;
import org.onap.aai.edges.EdgeIngestor;
import org.onap.aai.introspection.LoaderFactory;
import org.onap.aai.migration.Enabled;
import org.onap.aai.migration.MigrationDangerRating;
import org.onap.aai.migration.MigrationPriority;
import org.onap.aai.migration.Migrator;
import org.onap.aai.migration.Status;
import org.onap.aai.serialization.db.EdgeSerializer;
import org.onap.aai.serialization.engines.TransactionalGraphEngine;
import org.onap.aai.setup.SchemaVersions;


@MigrationPriority(20)
@MigrationDangerRating(2)
@Enabled
public class MigrateCloudRegionUpgradeCycle extends Migrator {

	protected static final String CLOUD_REGION_NODE_TYPE = "cloud-region";
	protected static final String CLOUD_OWNER = "cloud-owner";
	protected static final String CLOUD_REGION_ID = "cloud-region-id";
	protected static final String UPGRADE_CYCLE = "upgrade-cycle";
	private static final String homeDir = System.getProperty("AJSC_HOME");

	protected final AtomicInteger skippedRowsCount = new AtomicInteger(0);
	protected final AtomicInteger processedRowsCount = new AtomicInteger(0);

	private boolean success = true;
	private GraphTraversalSource g = null;
	protected int headerLength;

	protected final AtomicInteger falloutRowsCount = new AtomicInteger(0);

	public MigrateCloudRegionUpgradeCycle(TransactionalGraphEngine engine, LoaderFactory loaderFactory,
			EdgeIngestor edgeIngestor, EdgeSerializer edgeSerializer, SchemaVersions schemaVersions) {
		super(engine, loaderFactory, edgeIngestor, edgeSerializer, schemaVersions);
	}

	@Override
	public void run() {
		logger.info("---------- Start Updating upgrade-cycle for cloud-region  ----------");

		String configDir = System.getProperty("BUNDLECONFIG_DIR");
        if (homeDir == null) {
            logger.info("ERROR: Could not find sys prop AJSC_HOME");
            success = false;
            return;
        }
        if (configDir == null) {
            success = false;
            return;
        }
        
        String feedDir = homeDir + "/" + configDir + "/" + "migration-input-files/CloudRegion-ART-migration-data/";
		String fileName = feedDir + "CloudRegion-ART-migration-data.csv";
		logger.info(fileName);
		logger.info("---------- Processing Region Entries from file  ----------");

		Map cloudRegionVertexMap = new HashMap();

		try {
			int cloudRegionCount = 0;
			int cloudRegionErrorCount = 0;
			ArrayList data = loadFile(fileName);

			Map<String, String> cloudRegionMapFromART = (Map) data.get(0);
			Map<String, String> cloudAliasMapFromART = (Map) data.get(1);

			List<Vertex> cloudRegionList = this.engine.asAdmin().getTraversalSource().V()
					.has(AAIProperties.NODE_TYPE, CLOUD_REGION_NODE_TYPE).has(CLOUD_OWNER, "att-aic").toList();

			for (Vertex vertex : cloudRegionList) {
				String cloudRegionId = null;
				cloudRegionId = getCloudRegionIdNodeValue(vertex);
				cloudRegionVertexMap.put(cloudRegionId, vertex);
			}

			for (Map.Entry<String, String> entry : cloudRegionMapFromART.entrySet()) {
				boolean regionFound = false;
				String regionFromART = "";
				String aliasFromART = "";
				String vertexKey = "";
				
				regionFromART = (String) entry.getKey();

				if (cloudRegionVertexMap.containsKey(regionFromART)) {
					regionFound = true;
					vertexKey = regionFromART;
				} else {
					aliasFromART = cloudAliasMapFromART.get(regionFromART).toString();
					if (aliasFromART != null && !"".equals(aliasFromART)
							&& cloudRegionVertexMap.containsKey(aliasFromART)) {
						regionFound = true;
						vertexKey = aliasFromART;
					}
				}

				if (regionFound) {
					String upgradeCycle = "";
					try {
						upgradeCycle = (String) entry.getValue();

						if (upgradeCycle != null && !"".equals(upgradeCycle)) {
							Vertex vertex = (Vertex) cloudRegionVertexMap.get(vertexKey);
							vertex.property(UPGRADE_CYCLE, upgradeCycle);
							this.touchVertexProperties(vertex, false);
							logger.info("Updated cloud-region, upgrade-cycle to " + upgradeCycle
									+ " having cloud-region-id : " + vertexKey);
							cloudRegionCount++;
						} else {
							logger.info("upgrade-cycle value from ART is null or empty for the cloud-region-id : "
									+ vertexKey);
						}
					} catch (Exception e) {
						success = false;
						cloudRegionErrorCount++;
						logger.error(MIGRATION_ERROR
								+ "encountered exception for upgrade-cycle update having cloud-region-id :" + vertexKey,
								e);
					}
				} else {
					logger.info("Region "+regionFromART+" from ART is not found in A&AI");
				}

			}

			logger.info("\n \n ******* Final Summary of Updated upgrade-cycle for cloud-region  Migration ********* \n");
			logger.info(MIGRATION_SUMMARY_COUNT + "Number of cloud-region updated: " + cloudRegionCount + "\n");
			logger.info(MIGRATION_SUMMARY_COUNT + "Number of cloud-region failed to update due to error : "
					+ cloudRegionErrorCount + "\n");

		} catch (FileNotFoundException e) {
			logger.info("ERROR: Could not file file " + fileName, e.getMessage());
			success = false;
		} catch (IOException e) {
			logger.info("ERROR: Issue reading file " + fileName, e);
			success = false;
		} catch (Exception e) {
			logger.info("encountered exception", e);
			success = false;
		}
	}

	/**
	 * Load file to the map for processing
	 * 
	 * @param fileName
	 * @return
	 * @throws Exception
	 */
	protected ArrayList loadFile(String fileName) throws Exception {
		List<String> lines = Files.readAllLines(Paths.get(fileName));
		return this.getFileContents(lines);
	}

	/**
	 * Get lines from file.
	 * 
	 * @param lines
	 * @return
	 * @throws Exception
	 */
	protected ArrayList getFileContents(List<String> lines) throws Exception {

		final Map<String, String> regionMap = new ConcurrentHashMap<>();
		final Map<String, String> aliasMap = new ConcurrentHashMap<>();
		final ArrayList fileContent = new ArrayList();

		processAndRemoveHeader(lines);

		logger.info("Total rows count excluding header: " + lines.size());

		lines.stream().filter(line -> !line.isEmpty()).map(line -> Arrays.stream(line.split(",", -1)).map(String::trim).collect(Collectors.toList()))
				.map(this::processRegionUpgradeCycle).filter(Optional::isPresent).map(Optional::get).forEach(p -> {
					processedRowsCount.getAndIncrement();
					String pnfName = p.getValue0();
					if (!regionMap.containsKey(pnfName)) {
						regionMap.put(p.getValue0(), p.getValue1());
					}
				});

		fileContent.add(regionMap);

		lines.stream().filter(line -> !line.isEmpty()).map(line -> Arrays.stream(line.split(",", -1)).map(String::trim).collect(Collectors.toList()))
				.map(this::processRegionAlias).filter(Optional::isPresent).map(Optional::get).forEach(p -> {
					processedRowsCount.getAndIncrement();
					String pnfName = p.getValue0();
					if (!aliasMap.containsKey(pnfName)) {
						aliasMap.put(p.getValue0(), p.getValue1());
					}
				});
		fileContent.add(aliasMap);
		return fileContent;

	}

	/**
	 * Verify line has the necessary details.
	 * 
	 * @param line
	 * @return
	 */
	protected boolean verifyLine(List<String> line) {
		if (line.size() != headerLength) {
			logger.info("ERROR: INV line should contain " + headerLength + " columns, contains " + line.size()
					+ " instead.");
			this.skippedRowsCount.getAndIncrement();
			return false;
		}
		return true;
	}

	/**
	 * * Get the pnf name and interface name from the line.
	 * 
	 * @param line
	 * @return
	 */
	protected Optional<Pair<String, String>> processRegionAlias(List<String> line) {
		//logger.info("Processing line... " + line.toString());
		int lineSize = line.size();
		if (lineSize < 4) {
			logger.info("Skipping line, does not contain region and/or upgrade-cycle columns");
			skippedRowsCount.getAndIncrement();
			return Optional.empty();
		}

		String cloudRegion = line.get(0);
		String upgradeCycle = line.get(1).replaceAll("^\"|\"$", "").replaceAll("\\s+", "");

		if (cloudRegion.isEmpty()) {
			logger.info("Line missing cloudRegion name" + line);
			falloutRowsCount.getAndIncrement();
			return Optional.empty();
		}

		return Optional.of(Pair.with(cloudRegion, upgradeCycle));
	}

	/**
	 * * Get the pnf name and interface name from the line.
	 * 
	 * @param line
	 * @return
	 */
	protected Optional<Pair<String, String>> processRegionUpgradeCycle(List<String> line) {
		//logger.info("Processing line... " + line.toString());
		int lineSize = line.size();
		if (lineSize < 4) {
			logger.info("Skipping line, does not contain region and/or upgrade-cycle columns");
			skippedRowsCount.getAndIncrement();
			return Optional.empty();
		}

		String cloudRegion = line.get(0);
		String upgradeCycle = line.get(3).replaceAll("^\"|\"$", "").replaceAll("\\s+", "");

		if (cloudRegion.isEmpty()) {
			logger.info("Line missing cloudRegion name" + line);
			falloutRowsCount.getAndIncrement();
			return Optional.empty();
		}

		return Optional.of(Pair.with(cloudRegion, upgradeCycle));
	}

	/**
	 * Verify header of the csv and remove it from the list.
	 * 
	 * @param lines
	 * @throws Exception
	 */
	protected String processAndRemoveHeader(List<String> lines) throws Exception {
		String firstLine;
		if (lines.isEmpty()) {
			String msg = "ERROR: Missing Header in file";
			success = false;
			logger.error(msg);
			throw new Exception(msg);
		} else {
			firstLine = lines.get(0);
		}

		this.headerLength = firstLine.split(",", -1).length;
		logger.info("headerLength: " + headerLength);
		if (this.headerLength < 4) {
			String msg = "ERROR: Input file should have 4 columns";
			success = false;
			logger.error(msg);
			throw new Exception(msg);
		}

		return lines.remove(0);
	}

	private String getCloudRegionIdNodeValue(Vertex vertex) {
		String propertyValue = "";
		if (vertex != null && vertex.property(CLOUD_REGION_ID).isPresent()) {
			propertyValue = vertex.property(CLOUD_REGION_ID).value().toString();
		}
		return propertyValue;
	}

	@Override
	public Status getStatus() {
		if (success) {
			return Status.SUCCESS;
		} else {
			return Status.FAILURE;
		}
	}

	@Override
	public Optional<String[]> getAffectedNodeTypes() {
		return Optional.of(new String[] { CLOUD_REGION_NODE_TYPE });
	}

	@Override
	public String getMigrationName() {
		return "MigrateCloudRegionUpgradeCycle";
	}

}
