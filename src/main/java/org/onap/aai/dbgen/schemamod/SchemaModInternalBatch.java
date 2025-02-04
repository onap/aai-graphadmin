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
import java.util.HashMap;
import java.util.Iterator;
import java.util.Map;
import java.util.UUID;
import java.util.concurrent.TimeUnit;

import org.apache.tinkerpop.gremlin.structure.Graph;
import org.apache.tinkerpop.gremlin.structure.Vertex;
import org.janusgraph.core.schema.ConsistencyModifier;
import org.janusgraph.core.schema.JanusGraphIndex;
import org.onap.aai.serialization.engines.TransactionalGraphEngine;
import org.onap.aai.util.FormatDate;

import org.slf4j.Logger;
import org.janusgraph.core.Cardinality;
import org.janusgraph.core.PropertyKey;
import org.janusgraph.core.schema.JanusGraphManagement;

public class SchemaModInternalBatch {
	private static final String FROMAPPID = "AAI-UTILS";
	private final String TRANSID = UUID.randomUUID().toString();
	private final TransactionalGraphEngine engine;
	private final String propName;
	private final Class<?> type;
	private final String indexType;
	private final boolean preserveData;
	private final boolean consistencyLock;
	private final Cardinality cardinality;
	private final long commitBlockSize;
	private final Logger logger;

	public SchemaModInternalBatch(TransactionalGraphEngine engine, Logger logger, String propName,
				String type, String indexType, boolean preserveData, boolean consistencyLock, long commitBlockSize) {
		this.engine = engine;
		this.propName = propName;
		this.type = determineClass(type);
		this.indexType = indexType;
		this.preserveData = preserveData;
		this.consistencyLock = consistencyLock;
		this.cardinality = determineCardinality(type);
		this.commitBlockSize = commitBlockSize;
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
		String retiredName = "";
		boolean success = false;
		long timeStart = System.nanoTime();
		int batchCt = 0;
		int totalCount = 0;

		ArrayList<HashMap<String,Object>> allVerts = new ArrayList<HashMap<String,Object>>();
		HashMap<String,Object> batchVHash = new HashMap<String,Object>();

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

			// Collect the data that needs to be processed and
			// store as hashes of vertex-id's and the original property value
			long timeA = System.nanoTime();
			int msgEveryXCount = 1000;
			Graph grTmp1 = engine.startTransaction();
			Iterator<Vertex> allVtxItr = grTmp1.traversal().V().has(propName);
			// Will hold these in lists that are no bigger than our
			// allowed commitBatch size.
			logAndPrint(logger, "Collecting the data (this takes a little while).. ");
			int batchKey = 0;
			int batchVCount = 0;
			totalCount = 0;
			int msgCount = 0;
			logAndPrint(logger, "Collecting the data for batch # " + batchKey );
			Object origVal = null;
			while (allVtxItr.hasNext()) {
				Vertex v = allVtxItr.next();
				origVal = v.<Object>property(propName).orElse(null);
				batchVHash.put(v.id().toString(), origVal);
				batchVCount++;
				totalCount++;
				msgCount++;
				if (batchVCount >= commitBlockSize ) {
					// This was the last one for this batch
					allVerts.add(batchKey, batchVHash);
					batchKey++;
					logAndPrint(logger, "Collecting the data for batch # " + batchKey );
					batchVCount = 0;
					batchVHash = new HashMap<String,Object>();
				}
				if( msgCount > msgEveryXCount ) {
					msgCount = 0;
					logAndPrint(logger, " Initial processing running...  total so far = " + totalCount );
				}
			}

			if( batchVCount > 0 ) {
				// Add the last partial set if there is one.
				allVerts.add(batchKey, batchVHash);
			}
			logAndPrint(logger, "Found " + totalCount + " nodes that will be affected. ");

			batchCt = batchKey +1;

			if( totalCount == 0 ) {
				logAndPrint(logger, "INFO -- No data found to process.  ");
				System.exit(1);
			}

			logAndPrint(logger, "INFO -- Total of " + totalCount +
					" nodes to process.  Will use " + batchCt +
					" batches. " );

			long timeB = System.nanoTime();
			long diffTime =  timeB - timeA;
			long minCount = TimeUnit.NANOSECONDS.toMinutes(diffTime);
			long secCount = TimeUnit.NANOSECONDS.toSeconds(diffTime) - (60 * minCount);
			logAndPrint(logger, "    -- To collect all nodes took: " +
					minCount + " minutes, " + secCount + " seconds " );

			if (indexType.equals("uniqueIndex")) {
				// Make sure the data in the property being changed can have a
				// unique-index put on it.
				// Ie. if there are duplicate values, we will not be able to
				// migrate the data back into the property.
				Boolean foundDupesFlag = false;
				try {
					foundDupesFlag = doUniquenessCheck(allVerts, propName);
				} catch (Exception e) {
					logAndPrint(logger, "ERROR thrown in doUniquenessCheck(): [" +
							e.getMessage() + "]");
					System.exit(1);
				}
				if (foundDupesFlag) {
					logAndPrint(logger,
							"\n\n!!!!!! >> Cannot add a uniqueIndex for the property: [" + propName
									+ "] because duplicate values were found.  See the log for details on which"
									+ " nodes have this value.  \nThey will need to be resolved (by updating those values to"
									+ " new values or deleting unneeded nodes) using the standard REST-API \n");
					System.exit(1);
				}
				logAndPrint(logger, "-- Finished/Passed UniquePropertyCheck. ");
				logAndPrint(logger, "There are " + totalCount + " nodes that have this property. ");
			}

			// ---- If we made it to here - we must be OK with making this change

			// Rename this property to a backup name (old name with a dateString and
			//    "-RETIRED" appended)
			long timeE = System.nanoTime();
			FormatDate fd = new FormatDate("MMddHHmm", "GMT");
			String dteStr= fd.getDateTime();
			retiredName = propName + "-" + dteStr + "-RETIRED";
			graphMgt.changeName(origPropKey, retiredName);
			logAndPrint(logger, " -- Temporary property name will be: [" + retiredName + "]. ");

			// Create a new property using the original property name and the
			// targetDataType
			PropertyKey freshPropKey = graphMgt.makePropertyKey(propName).dataType(type)
					.cardinality(cardinality).make();
			if (consistencyLock) {
				logAndPrint(logger, " -- Consistency Lock is being set on the property ");
				graphMgt.setConsistency(freshPropKey, ConsistencyModifier.LOCK);
			}
			// Create the appropriate index (if any)
			JanusGraphIndex indexG = null;
			if (indexType.equals("uniqueIndex")) {
				String freshIndexName = propName + dteStr;
				indexG = graphMgt.buildIndex(freshIndexName, Vertex.class).addKey(freshPropKey).unique().buildCompositeIndex();
			} else if (indexType.equals("index")) {
				String freshIndexName = propName + dteStr;
				indexG = graphMgt.buildIndex(freshIndexName, Vertex.class).addKey(freshPropKey).buildCompositeIndex();
			}

			if(indexG != null && consistencyLock) {
				logAndPrint(logger, " -- Consistency Lock is being set on the index ");
				graphMgt.setConsistency(indexG, ConsistencyModifier.LOCK);
			}

			logAndPrint(logger, "Committing schema changes with graphMgt.commit()");
			graphMgt.commit();
			success = true;

			long timeF = System.nanoTime();
			diffTime =  timeF - timeE;
			minCount = TimeUnit.NANOSECONDS.toMinutes(diffTime);
			secCount = TimeUnit.NANOSECONDS.toSeconds(diffTime) - (60 * minCount);
			logAndPrint(logger, "    -- Temporary property Name Change took: " +
					minCount + " minutes, " + secCount + " seconds " );

		} catch (Exception ex) {
			logAndPrint(logger, "Threw a regular Exception: ");
			logAndPrint(logger, ex.getMessage());
			System.exit(1);
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


		// For each node that has this property, update the new from the old
		// and then remove the old property from that node
		// Note - do it in batches since there can be a LOT of updates.

		long timeE = System.nanoTime();
		ArrayList <String> emsgList = new ArrayList <String> ();
		for( int batNo=0; batNo < batchCt; batNo++ ) {
			try {
				logAndPrint(logger, "BEGIN -- Batch # " + batNo );
				processUpdateForBatch( 	allVerts.get(batNo), retiredName );
				logAndPrint(logger, "Completed Batch # " + batNo );
			} catch (Exception e) {
				String emsg = "ERROR -- Batch # " + batNo +
					" failed to process.  Please clean up manually. " +
					" data in [" + retiredName +
					"] will have to be moved to the original property.";
				logAndPrint(logger, emsg);
				emsgList.add(emsg);
			}
		}
		long timeF = System.nanoTime();
		long diffTime =  timeF - timeE;
		long minCount = TimeUnit.NANOSECONDS.toMinutes(diffTime);
		long secCount = TimeUnit.NANOSECONDS.toSeconds(diffTime) - (60 * minCount);
		logAndPrint(logger, "    -- Time to process all batches: " +
				minCount + " minutes, " + secCount + " seconds " );

		logAndPrint(logger, "\nINFO -- Total of " + totalCount +
				" nodes processed using: " + batchCt + " batches. " );

		if( !emsgList.isEmpty() ) {
			Iterator <String> eItr = emsgList.iterator();
			logAndPrint(logger, ">>> These will need to be taken care of: ");
			while( eItr.hasNext() ) {
				logAndPrint(logger, (String)eItr.next());
			}
		}

		long timeEnd = System.nanoTime();
		diffTime =  timeEnd - timeStart;
		minCount = TimeUnit.NANOSECONDS.toMinutes(diffTime);
		secCount = TimeUnit.NANOSECONDS.toSeconds(diffTime) - (60 * minCount);
		logAndPrint(logger, "    -- Total Processing time was: " +
				minCount + " minutes, " + secCount + " seconds " );

	}// End of Execute()


	private void processUpdateForBatch( HashMap<String,Object> vertHash,
			String retiredName ) throws Exception {

		Iterator<Map.Entry<String, Object>> vertHashItr = vertHash.entrySet().iterator();
		int vtxCount = 0;
		Boolean success = false;
		Graph grTmpBat = engine.startTransaction();
		try {
			while( vertHashItr.hasNext() ){
				Map.Entry<String, Object> entry = vertHashItr.next();
				String tmpVid = entry.getKey();
				Vertex tmpVtx = null;

				Iterator<Vertex> oneVItr = grTmpBat.traversal().V(tmpVid);
				while( oneVItr.hasNext() ) {
					// should never find more than one...
					tmpVtx = oneVItr.next();
					Object origVal = entry.getValue();
					if (preserveData) {
						tmpVtx.property(propName, origVal);
					} else {
						// existing nodes just won't have that property anymore
						// Might want to do this if the new
						// data type was not compatible with the old.
					}
					tmpVtx.property(retiredName).remove();
					logAndPrint(logger, "INFO -- update item: (vid= "
							+ tmpVid + ", val=[" + origVal + "])");
					vtxCount++;
				}
			}

			logAndPrint(logger, "INFO -- finished processing a batch with " + vtxCount + " nodes.");
			success = true;
		} catch (Exception ex) {
			logAndPrint(logger, "Threw a regular Exception: ");
			logAndPrint(logger, ex.getMessage());
		} finally {
			if (engine != null) {
				if (success) {
					logAndPrint(logger, "INFO -- committing node updates for this batch.");
					engine.commit();
				} else {
					logAndPrint(logger, "ERROR -- rolling back node updates for this batch.");
					engine.rollback();
				}
			}
		}
		if( ! success ) {
			throw new Exception ("ERROR - could not process this batch -- see the log for details.");
		}

	}// end of processUpdateForBatch()


	private Boolean doUniquenessCheck( ArrayList<HashMap<String,Object>> allVerts,
			String propertyName ){
		// Note - property can be found in more than one nodetype
		//   our uniqueness constraints are always across the entire db - so this
		//   tool looks across all nodeTypes that the property is found in.
		long timeStart = System.nanoTime();
		int batchCt = allVerts.size();
		HashMap <String,Object> bigSingleHash = new HashMap <String,Object> ();

		for( int batNo=0; batNo < batchCt; batNo++ ) {
			bigSingleHash.putAll(allVerts.get(batNo));
		}

		ArrayList <Object> dupeValues = new ArrayList<Object> ();
		int dupeCount = 0;

		Iterator bItr = bigSingleHash.entrySet().iterator();
		while( bItr.hasNext() ) {
			Map.Entry pair = (Map.Entry)bItr.next();
			Object thisVal = pair.getValue();
			bItr.remove();
			if( bigSingleHash.containsValue(thisVal) ) {
				// Found a dupe - because the value was still in the bigHash after
				//    we removed this pair from the bigHash
				logAndPrint(logger, "  Found a dupe node with val [" + thisVal + "]");
    			if( dupeCount == 0 ) {
    				dupeValues.add(thisVal);
    			}
    			else if( !dupeValues.contains(thisVal) ){
    				// Only record the first time we see it since we're just tracking
    				// the values, not the vids
    				dupeValues.add(thisVal);
    			}
    			dupeCount++;
    		}
		}

		long timeEnd = System.nanoTime();
		long diffTime =  timeEnd - timeStart;
		long minCount = TimeUnit.NANOSECONDS.toMinutes(diffTime);
		long secCount = TimeUnit.NANOSECONDS.toSeconds(diffTime) - (60 * minCount);
		logAndPrint(logger, "    -- Total Uniqueness Check took: " +
				minCount + " minutes, " + secCount + " seconds " );

		if( dupeValues.isEmpty() ){
			logAndPrint(logger, "\n ------------ No Duplicates Found -------- \n");
		}
		else {
			logAndPrint(logger, "\n -------------- Found " + dupeCount +
    			" cases of duplicate values for property [" + propertyName + "\n\n");
			logAndPrint(logger, "\n --- These values are in the db twice or more: ");
	    	Iterator <?> dupeValItr = dupeValues.iterator();
	    	while( dupeValItr.hasNext() ){
	    		logAndPrint(logger, " value = [" + dupeValItr.next() + "]");
	    	}
    	}

		if( dupeCount > 0 ) {
			return true;
		}else {
			return false;
		}

	}// end of doUniquenessCheck()



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
