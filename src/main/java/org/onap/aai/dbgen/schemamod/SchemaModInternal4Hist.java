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
package org.onap.aai.dbgen.schemamod;

import java.util.Iterator;

import org.apache.tinkerpop.gremlin.structure.Graph;
import org.apache.tinkerpop.gremlin.structure.Vertex;
import org.onap.aai.dbmap.AAIGraph;
import org.onap.aai.serialization.engines.TransactionalGraphEngine;
import org.onap.aai.util.FormatDate;
import org.slf4j.Logger;
import org.janusgraph.core.Cardinality;
import org.janusgraph.core.PropertyKey;
import org.janusgraph.core.schema.JanusGraphManagement;
import org.janusgraph.graphdb.database.management.ManagementSystem;

public class SchemaModInternal4Hist {
	private final TransactionalGraphEngine engine;
	private final String propName;
	private final Class<?> type;
	private final String indexType;
	private final boolean preserveData;
	private final Cardinality cardinality;
	private final Logger logger;

	public SchemaModInternal4Hist(TransactionalGraphEngine engine, Logger logger, String propName, String type, String indexType, boolean preserveData) {
		this.engine = engine;
		this.propName = propName;
		this.type = determineClass(type);
		this.indexType = indexType;
		this.preserveData = preserveData;
		this.cardinality = Cardinality.LIST; // Always use this for History
		this.logger = logger;
	}

	private Class<?> determineClass(String type) {
		final Class<?> result;
		if (type.equals("String")) {
			result = String.class;
		} else if (type.equals("Set<String>")) {
			result = String.class;
		} else if (type.equals("Integer")) {
			result = Integer.class;
		} else if (type.equals("Boolean")) {
			result = Boolean.class;
		} else if (type.equals("Character")) {
			result = Character.class;
		} else if (type.equals("Long")) {
			result = Long.class;
		} else if (type.equals("Float")) {
			result = Float.class;
		} else if (type.equals("Double")) {
			result = Double.class;
		} else {
			String emsg = "Not able translate the targetDataType [" + type + "] to a Class variable.\n";
			logAndPrint(logger, emsg);
			throw new RuntimeException(emsg);
		}

		return result;
	}

	public void execute() {
		JanusGraphManagement graphMgt = null;
		boolean success = false;
		try {
			// Make sure this property is in the DB.
			graphMgt = engine.asAdmin().getManagementSystem();
			if (graphMgt == null) {
				String emsg = "Not able to get a graph Management object in SchemaModInternal4Hist.java\n";
				logAndPrint(logger, emsg);
				System.exit(1);
			}
			PropertyKey origPropKey = graphMgt.getPropertyKey(propName);
			if (origPropKey == null) {
				String emsg = "The propName = [" + propName + "] is not defined in our graph. ";
				logAndPrint(logger, emsg);
				System.exit(1);
			}

			// Rename this property to a backup name (old name with "retired_"
			// appended plus a dateStr)
			FormatDate fd = new FormatDate("MMddHHmm", "GMT");
			String dteStr= fd.getDateTime();

			String retiredName = propName + "-" + dteStr + "-RETIRED";
			graphMgt.changeName(origPropKey, retiredName);

			// Create a new property using the original property name and the
			// targetDataType
			PropertyKey freshPropKey = graphMgt.makePropertyKey(propName).dataType(type)
					.cardinality(cardinality).make();

			// Create an index if needed (regular index will be used instead of unique for history)
			boolean needsIndex = indexType.equals("index") || indexType.equals("uniqueIndex");
			String freshIndexName = propName + dteStr;
			if (needsIndex) {
				graphMgt.buildIndex(freshIndexName, Vertex.class).addKey(freshPropKey).buildCompositeIndex();
			}

			logAndPrint(logger, "Committing schema changes with graphMgt.commit()");
			graphMgt.commit();
			engine.commit();
			if (needsIndex) {
				ManagementSystem.awaitGraphIndexStatus(AAIGraph.getInstance().getGraph(), freshIndexName).call();
			}
			Graph grTmp2 = engine.startTransaction();

			// For each node that has this property, update the new from the old
			// and then remove the
			// old property from that node
			Iterator<Vertex> verts = grTmp2.traversal().V().has(retiredName);
			int vtxCount = 0;
			while (verts.hasNext()) {
				vtxCount++;
				Vertex tmpVtx =  verts.next();
				String tmpVid = tmpVtx.id().toString();
				Object origVal = tmpVtx.<Object> property(retiredName).orElse(null);
				if (preserveData) {
					tmpVtx.property(propName, origVal);
					logAndPrint(logger,
								"INFO -- just did the add of the freshPropertyKey and updated it with the orig value ("
										+ origVal.toString() + ")");
				} else {
					// existing nodes just won't have that property anymore
					// Not sure if we'd ever actually want to do this -- maybe
					// we'd do this if the new
					// data type was not compatible with the old?
				}
				tmpVtx.property(retiredName).remove();
				logAndPrint(logger, "INFO -- just did the remove of the " + retiredName + " from this vertex. (vid="
						+ tmpVid + ")");
			}

			success = true;
		} catch (Exception ex) {
			logAndPrint(logger, "Threw a regular Exception: ");
			logAndPrint(logger, ex.getMessage());
		} finally {
			if (graphMgt != null && graphMgt.isOpen()) {
				// Any changes that worked correctly should have already done
				// their commits.
				graphMgt.rollback();
			}
			if (engine != null) {
				if (success) {
					engine.commit();
				} else {
					engine.rollback();
				}
			}
		}
	}

	/**
	 * Log and print.
	 *
	 * @param logger the logger
	 * @param msg the msg
	 */
	protected static void logAndPrint(Logger logger, String msg) {
		System.out.println(msg);
		logger.debug(msg);
	}

}
