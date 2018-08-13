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

import java.util.ArrayList;
import java.util.Iterator;
import java.util.UUID;

import org.apache.tinkerpop.gremlin.process.traversal.dsl.graph.GraphTraversal;
import org.apache.tinkerpop.gremlin.structure.Direction;
import org.apache.tinkerpop.gremlin.structure.Edge;
import org.apache.tinkerpop.gremlin.structure.Graph;
import org.apache.tinkerpop.gremlin.structure.Vertex;
import org.apache.tinkerpop.gremlin.structure.VertexProperty;
import org.onap.aai.serialization.engines.TransactionalGraphEngine;
import org.onap.aai.util.FormatDate;
import org.onap.aai.util.UniquePropertyCheck;

import com.att.eelf.configuration.EELFLogger;
import org.janusgraph.core.Cardinality;
import org.janusgraph.core.PropertyKey;
import org.janusgraph.core.schema.JanusGraphManagement;

public class SchemaModInternal {
	private static final String FROMAPPID = "AAI-UTILS";
	private final String TRANSID = UUID.randomUUID().toString();
	private final TransactionalGraphEngine engine;
	private final String propName;
	private final Class<?> type;
	private final String indexType;
	private final boolean preserveData;
	private final Cardinality cardinality;
	private final EELFLogger logger;
	
	public SchemaModInternal(TransactionalGraphEngine engine, EELFLogger logger, String propName, String type, String indexType, boolean preserveData) {
		this.engine = engine;
		this.propName = propName;
		this.type = determineClass(type);
		this.indexType = indexType;
		this.preserveData = preserveData;
		this.cardinality = determineCardinality(type);
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
	private Cardinality determineCardinality(String type) {
		if (type.equals("Set<String>")) {
			return Cardinality.SET;
		} else {
			return Cardinality.SINGLE;
		}
	}
	public void execute() {
		JanusGraphManagement graphMgt = null;
		boolean success = false;
		try {
			// Make sure this property is in the DB.
			graphMgt = engine.asAdmin().getManagementSystem();
			if (graphMgt == null) {
				String emsg = "Not able to get a graph Management object in SchemaMod.java\n";
				logAndPrint(logger, emsg);
				System.exit(1);
			}
			PropertyKey origPropKey = graphMgt.getPropertyKey(propName);
			if (origPropKey == null) {
				String emsg = "The propName = [" + propName + "] is not defined in our graph. ";
				logAndPrint(logger, emsg);
				System.exit(1);
			}
	
			if (indexType.equals("uniqueIndex")) {
				// Make sure the data in the property being changed can have a
				// unique-index put on it.
				// Ie. if there are duplicate values, we will not be able to
				// migrate the data back into the property.
				
				
				Graph grTmp = engine.tx();
				if( grTmp == null ){
					grTmp = engine.startTransaction();
				}
				// This is good to know in the logs
				logAndPrint(logger, "-- Starting UniquePropertyCheck. (this may take a loooong time) --");  
				
				Boolean foundDupesFlag = UniquePropertyCheck.runTheCheckForUniqueness(TRANSID, FROMAPPID,
						grTmp, propName, logger);
				if (foundDupesFlag) {
					logAndPrint(logger,
							"\n\n!!!!!! >> Cannot add a uniqueIndex for the property: [" + propName
									+ "] because duplicate values were found.  See the log for details on which"
									+ " nodes have this value.  \nThey will need to be resolved (by updating those values to new"
									+ " values or deleting unneeded nodes) using the standard REST-API \n");
					System.exit(1);
				}
				logAndPrint(logger, "-- Finished UniquePropertyCheck. ");  // This is good to know in the logs
			}
	
	
			// ---- If we made it to here - we must be OK with making this change
	
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
	
			// Create the appropriate index (if any)
			if (indexType.equals("uniqueIndex")) {
				String freshIndexName = propName + dteStr;
				graphMgt.buildIndex(freshIndexName, Vertex.class).addKey(freshPropKey).unique().buildCompositeIndex();
			} else if (indexType.equals("index")) {
				String freshIndexName = propName + dteStr;
				graphMgt.buildIndex(freshIndexName, Vertex.class).addKey(freshPropKey).buildCompositeIndex();
			}
	
			logAndPrint(logger, "Committing schema changes with graphMgt.commit()");
			graphMgt.commit();
			engine.commit();
			Graph grTmp2 = engine.startTransaction();
			
	
			// For each node that has this property, update the new from the old
			// and then remove the
			// old property from that node
			Iterator<Vertex> verts = grTmp2.traversal().V().has(retiredName);
			int vtxCount = 0;
			ArrayList<String> alreadySeenVals = new ArrayList<String>();
			while (verts.hasNext()) {
				vtxCount++;
				Vertex tmpVtx =  verts.next();
				String tmpVid = tmpVtx.id().toString();
				Object origVal = tmpVtx.<Object> property(retiredName).orElse(null);
				if (preserveData) {
					tmpVtx.property(propName, origVal);
					if (indexType.equals("uniqueIndex")) {
						// We're working on a property that is being used as a
						// unique index
						String origValStr = "";
						if (origVal != null) {
							origValStr = origVal.toString();
						}
						if (alreadySeenVals.contains(origValStr)) {
							// This property is supposed to be unique, but we've
							// already seen this value in this loop
							// This should have been caught up in the first part
							// of SchemaMod, but since it wasn't, we
							// will just log the problem.
							logAndPrint(logger,
									"\n\n ---------- ERROR - could not migrate the old data [" + origValStr
											+ "] for propertyName [" + propName
											+ "] because this property is having a unique index put on it.");
							showPropertiesAndEdges(TRANSID, FROMAPPID, tmpVtx, logger);
							logAndPrint(logger, "-----------------------------------\n");
						} else {
							// Ok to add this prop in as a unique value
							tmpVtx.property(propName, origVal);
							logAndPrint(logger,
									"INFO -- just did the add of the freshPropertyKey and updated it with the orig value ("
											+ origValStr + ")");
						}
						alreadySeenVals.add(origValStr);
					} else {
						// We are not working with a unique index
						tmpVtx.property(propName, origVal);
						logAndPrint(logger,
								"INFO -- just did the add of the freshPropertyKey and updated it with the orig value ("
										+ origVal.toString() + ")");
					}
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
	 * Show properties and edges.
	 *
	 * @param transId the trans id
	 * @param fromAppId the from app id
	 * @param tVert the t vert
	 * @param logger the logger
	 */
	private static void showPropertiesAndEdges(String transId, String fromAppId, Vertex tVert, EELFLogger logger) {

		if (tVert == null) {
			logAndPrint(logger, "Null node passed to showPropertiesAndEdges.");
		} else {
			String nodeType = "";
			Object ob = tVert.<String> property("aai-node-type");
			if (ob == null) {
				nodeType = "null";
			} else {
				nodeType = ob.toString();
			}

			logAndPrint(logger, " AAINodeType/VtxID for this Node = [" + nodeType + "/" + tVert.id() + "]");
			logAndPrint(logger, " Property Detail: ");
			Iterator<VertexProperty<Object>> pI = tVert.properties();
			while (pI.hasNext()) {
				VertexProperty<Object> tp = pI.next();
				Object val = tp.value();
				logAndPrint(logger, "Prop: [" + tp.key() + "], val = [" + val + "] ");
			}

			Iterator<Edge> eI = tVert.edges(Direction.BOTH);
			if (!eI.hasNext()) {
				logAndPrint(logger, "No edges were found for this vertex. ");
			}
			while (eI.hasNext()) {
				Edge ed = eI.next();
				String lab = ed.label();
				Vertex vtx;
				if (tVert.equals(ed.inVertex())) {
					vtx = ed.outVertex();
				} else {
					vtx = ed.inVertex();
				}
				if (vtx == null) {
					logAndPrint(logger,
							" >>> COULD NOT FIND VERTEX on the other side of this edge edgeId = " + ed.id() + " <<< ");
				} else {
					String nType = vtx.<String> property("aai-node-type").orElse(null);
					String vid = vtx.id().toString();
					logAndPrint(logger, "Found an edge (" + lab + ") from this vertex to a [" + nType
							+ "] node with VtxId = " + vid);
				}
			}
		}
	} // End of showPropertiesAndEdges()

	/**
	 * Log and print.
	 *
	 * @param logger the logger
	 * @param msg the msg
	 */
	protected static void logAndPrint(EELFLogger logger, String msg) {
		System.out.println(msg);
		logger.info(msg);
	}
	
}
