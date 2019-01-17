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
package org.onap.aai.migration.v12;
/*-
 * ============LICENSE_START=======================================================
 * org.onap.aai
 * ================================================================================
 * Copyright (C) 2017 AT&T Intellectual Property. All rights reserved.
 * ================================================================================
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 * 
 *      http://www.apache.org/licenses/LICENSE-2.0
 * 
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 * ============LICENSE_END=========================================================
 */


import org.apache.tinkerpop.gremlin.process.traversal.dsl.graph.GraphTraversalSource;
import org.apache.tinkerpop.gremlin.structure.Vertex;
import org.javatuples.Pair;
import org.onap.aai.db.props.AAIProperties;
import org.onap.aai.edges.EdgeIngestor;
import org.onap.aai.introspection.Introspector;
import org.onap.aai.introspection.LoaderFactory;
import org.onap.aai.migration.*;
import org.onap.aai.serialization.db.EdgeSerializer;
import org.onap.aai.serialization.engines.TransactionalGraphEngine;
import org.onap.aai.setup.SchemaVersions;

import java.io.FileNotFoundException;
import java.io.IOException;
import java.nio.file.Files;
import java.nio.file.Paths;
import java.util.*;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.atomic.AtomicInteger;


@MigrationPriority(25)
@MigrationDangerRating(100)
public class MigrateINVPhysicalInventory extends Migrator {

	private static final String NODE_TYPE_PNF = "pnf";
	private static final String NODE_TYPE_PINTERFACE = "p-interface";
	private static final String NODE_TYPE_PINTERFACES = "p-interfaces";
	private static final String PROPERTY_PNF_NAME = "pnf-name";
	private static final String PROPERTY_INTERFACE_NAME = "interface-name";
	protected final AtomicInteger skippedRowsCount = new AtomicInteger(0);
	protected final AtomicInteger processedRowsCount = new AtomicInteger(0);

	private boolean success = true;
    private boolean checkLog = false;
    private GraphTraversalSource g = null;
    protected int headerLength;

	protected final AtomicInteger falloutRowsCount = new AtomicInteger(0);
	private static final String homeDir = System.getProperty("AJSC_HOME");
	private static List<String> dmaapMsgList = new ArrayList<String>();

	public MigrateINVPhysicalInventory(TransactionalGraphEngine engine, LoaderFactory loaderFactory, EdgeIngestor edgeIngestor, EdgeSerializer edgeSerializer, SchemaVersions schemaVersions) {
		super(engine, loaderFactory, edgeIngestor, edgeSerializer, schemaVersions);
        this.g = this.engine.asAdmin().getTraversalSource();
    }

	@Override
    public void run() {
        logger.info("---------- Start migration of INV File Physical Inventory  ----------");
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
        
        String feedDir = homeDir + "/" + configDir + "/" + "migration-input-files/sarea-inventory/";
        String fileName = feedDir+ "inv.csv";
        logger.info(fileName);
        logger.info("---------- Processing INV Entries from file  ----------");


		try {
			Map<String, Set<String>> data = loadFile(fileName);
			this.processData(data);
			
			logger.info("\n ******* Summary Report for Inv File Physical Migration *******");
			logger.info("Number of distinct pnfs processed: "+data.keySet().size());
			logger.info("Rows processed: " + processedRowsCount);
			logger.info("Rows skipped: "+ skippedRowsCount);
			logger.info("Fallout Rows count: " + falloutRowsCount);
			
		} catch (FileNotFoundException e) {
            logger.info("ERROR: Could not file file " + fileName, e.getMessage());
            success = false;
            checkLog = true;
        } catch (IOException e) {
            logger.info("ERROR: Issue reading file " + fileName, e);
            success = false;
        } catch (Exception e) {
            logger.info("encountered exception", e);
            e.printStackTrace();
            success = false;
        }
    }

	protected void processData(Map<String, Set<String>> data) throws Exception{

		for (Map.Entry<String, Set<String>> entry : data.entrySet()) {
			String pnfName = entry.getKey();
			final Set<String> newPInterfaces = entry.getValue();
			Introspector pnf;
			Vertex pnfVertex;
			EventAction eventAction = EventAction.UPDATE;
			boolean pnfChangesMade = false;

			if (pnfExists(pnfName)) {
				pnf = serializer.getLatestVersionView(getPnf(pnfName));
				pnfVertex = getPnf(pnfName);
			} else {
				pnf = loader.introspectorFromName(NODE_TYPE_PNF);
				pnf.setValue(PROPERTY_PNF_NAME, pnfName);
				pnfVertex = serializer.createNewVertex(pnf);
				eventAction = EventAction.CREATE;
				pnfChangesMade = true;
			}

			if (pnfChangesMade) {
				serializer.serializeSingleVertex(pnfVertex, pnf, getMigrationName());
				logger.info ("\t Pnf [" + pnfName +"] created with vertex id "+pnfVertex);
//				pnf = serializer.getLatestVersionView(pnfVertex);
//				this.notificationHelper.addEvent(pnfVertex, serializer.getLatestVersionView(pnfVertex), eventAction, this.serializer.getURIForVertex(pnfVertex, false));
//				logger.info("\t Dmaap notification sent for creation of pnf ");
				String dmaapMsg = System.nanoTime() + "_" + pnfVertex.id().toString() + "_"	+ pnfVertex.value("resource-version").toString();
				dmaapMsgList.add(dmaapMsg);
			} else {
				logger.info("\t Pnf ["+ pnfName +"] already exists ");
			}

			if (!newPInterfaces.isEmpty()) {
				Introspector pInterfacesIntrospector = pnf.getWrappedValue(NODE_TYPE_PINTERFACES);
				if ( pInterfacesIntrospector == null) {
					pInterfacesIntrospector = pnf.newIntrospectorInstanceOfProperty(NODE_TYPE_PINTERFACES);
					pnf.setValue(NODE_TYPE_PINTERFACES, pInterfacesIntrospector.getUnderlyingObject());
				}

				for (Introspector introspector : pInterfacesIntrospector.getWrappedListValue(NODE_TYPE_PINTERFACE)) {
					String interfaceName = introspector.getValue(PROPERTY_INTERFACE_NAME).toString();
					if (newPInterfaces.contains(interfaceName)) {
						newPInterfaces.remove(interfaceName);
					}
				}

				for (String pInterfaceName : newPInterfaces) {
					Introspector pInterface = loader.introspectorFromName(NODE_TYPE_PINTERFACE);
					pInterface.setValue(PROPERTY_INTERFACE_NAME, pInterfaceName);
					Vertex pInterfaceVertex = serializer.createNewVertex(pInterface);
					pInterfaceVertex.property(AAIProperties.AAI_URI, pnfVertex.property(AAIProperties.AAI_URI).value() + "/p-interfaces/p-interface/" + pInterfaceName);
					edgeSerializer.addTreeEdge(g, pnfVertex, pInterfaceVertex);
					eventAction = EventAction.CREATE;
					serializer.serializeSingleVertex(pInterfaceVertex, pInterface, getMigrationName());
					logger.info ("\t p-interface [" + pInterfaceName +"] created with vertex id "+ pInterfaceVertex + " on pnf ["+pnfName+"]");
//					pInterface = serializer.getLatestVersionView(pInterfaceVertex);
//					this.notificationHelper.addEvent(pInterfaceVertex, pInterface, eventAction, this.serializer.getURIForVertex(pInterfaceVertex, false));
//					logger.info("\t Dmaap notification sent for creation of p-interface ");
					String dmaapMsg = System.nanoTime() + "_" + pInterfaceVertex.id().toString() + "_"	+ pInterfaceVertex.value("resource-version").toString();
					dmaapMsgList.add(dmaapMsg);
				}
			}
		}
	}

	protected boolean pnfExists(String pnfName) {
		return g.V().has(PROPERTY_PNF_NAME, pnfName).has(AAIProperties.NODE_TYPE, NODE_TYPE_PNF).hasNext();
	}

	protected Vertex getPnf(String pnfName) {
		return g.V().has(PROPERTY_PNF_NAME, pnfName).has(AAIProperties.NODE_TYPE, NODE_TYPE_PNF).next();
	}

	/**
	 * Load file to the map for processing
	 * @param fileName
	 * @return
	 * @throws Exception
	 */
	protected Map<String,Set<String>> loadFile(String fileName) throws Exception {
		List<String> lines = Files.readAllLines(Paths.get(fileName));
		return this.getFileContents(lines);
	}

	/**
	 * Get lines from file.
	 * @param lines
	 * @return
	 * @throws Exception
	 */
	protected Map<String,Set<String>> getFileContents(List<String> lines) throws Exception {

		final Map<String,Set<String>> fileContents = new ConcurrentHashMap<>();

		processAndRemoveHeader(lines);
		
		logger.info("Total rows count excluding header: "+ lines.size());
		
		lines.stream()
			.filter(line -> !line.isEmpty())
			.map(line -> Arrays.asList(line.split("\\s*,\\s*", -1)))
//			.filter(this::verifyLine)
			.map(this::processLine)
			.filter(Optional::isPresent)
			.map(Optional::get)
			.forEach(p -> {
				processedRowsCount.getAndIncrement();
				String pnfName = p.getValue0();
				if (!fileContents.containsKey(pnfName)) {
					Set<String> s = new HashSet<>();
					fileContents.put(p.getValue0(), s);
				}
				if (p.getValue1() != null) {
					fileContents.get(p.getValue0()).add(p.getValue1());
				}
			})
		;
		
		return fileContents;


	}

	/**
	 * Verify line has the necessary details.
	 * @param line
	 * @return
	 */
	protected boolean verifyLine(List<String> line) {
		if (line.size() != headerLength) {
			logger.info("ERROR: INV line should contain " + headerLength + " columns, contains " + line.size() + " instead.");
			this.skippedRowsCount.getAndIncrement();
			return false;
		}
		return true;
	}

	/**
* 	 * Get the pnf name and interface name from the line.
	 * @param line
	 * @return
	 */
	protected Optional<Pair<String,String>> processLine(List<String> line) {
		logger.info("Processing line... " + line.toString());
		int lineSize = line.size();
		if (lineSize < 11){
			logger.info("Skipping line, does not contain pnf and/or port columns");
			skippedRowsCount.getAndIncrement();
			return Optional.empty();
		}
		
		String pnfName = line.get(0);
		String portAid = line.get(11).replaceAll("^\"|\"$", "").replaceAll("\\s+","");
		
		if (pnfName.isEmpty() && portAid.isEmpty()) {
			logger.info("Line missing pnf name and port " + line);
			falloutRowsCount.getAndIncrement();
			return Optional.empty();
		} else if (pnfName.isEmpty()) {
			logger.info("Line missing pnf name" + line);
			falloutRowsCount.getAndIncrement();
			return Optional.empty();
		} else if (portAid.isEmpty()) {
			logger.info("Line missing port " + line);
			return Optional.of(Pair.with(pnfName, null));
		}
		return Optional.of(Pair.with(pnfName, portAid));
	}

	/**
	 * Verify header of the csv and remove it from the list.
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

		this.headerLength = firstLine.split("\\s*,\\s*", -1).length;
		logger.info("headerLength: " + headerLength);
		if (this.headerLength < 21){
			String msg = "ERROR: Input file should have 21 columns";
			success = false;
			logger.error(msg);
			throw new Exception(msg);
		}

		return lines.remove(0);
	}


    @Override
    public Status getStatus() {
        if (checkLog) {
            return Status.CHECK_LOGS;
        }
        else if (success) {
            return Status.SUCCESS;
        }
        else {
            return Status.FAILURE;
        }
    }
    
    @Override
	public void commit() {
		engine.commit();
		createDmaapFiles(dmaapMsgList);
	}

    @Override
    public Optional<String[]> getAffectedNodeTypes() {
        return Optional.of(new String[]{NODE_TYPE_PNF});
    }

    @Override
    public String getMigrationName() {
        return "MigrateINVPhysicalInventory";
    }

}
