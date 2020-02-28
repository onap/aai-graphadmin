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

import java.io.FileOutputStream;
import java.util.ArrayList;
import java.util.Iterator;

import org.apache.tinkerpop.gremlin.process.traversal.dsl.graph.GraphTraversal;
import org.apache.tinkerpop.gremlin.process.traversal.dsl.graph.GraphTraversalSource;
import org.apache.tinkerpop.gremlin.structure.Direction;
import org.apache.tinkerpop.gremlin.structure.Vertex;
import org.apache.tinkerpop.gremlin.structure.io.GraphWriter;
import org.apache.tinkerpop.gremlin.structure.io.IoCore;
import org.janusgraph.core.JanusGraph;
import org.onap.aai.aailog.logs.AaiScheduledTaskAuditLog;
import org.onap.aai.exceptions.AAIException;
import org.onap.aai.logging.ErrorLogHelper;
import org.onap.logging.filter.base.ONAPComponents;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;


public class PrintVertexDetails implements Runnable{

	private Logger LOGGER;

	private JanusGraph jg;
	private String fname;
	private ArrayList<Long> vtxIdList;
	private Boolean debugOn;
	private long debugDelayMs;
	private String snapshotType;

	static final byte[] newLineBytes = "\n".getBytes();
	
	private AaiScheduledTaskAuditLog auditLog;
	

	public PrintVertexDetails (JanusGraph graph, String fn, ArrayList<Long> vIdL, Boolean debugFlag, 
			long debugAddDelayTime, String snapshotType, Logger elfLog){
		jg = graph;
		fname = fn;
		vtxIdList = vIdL;
		debugOn = debugFlag;
		debugDelayMs = debugAddDelayTime;
		this.snapshotType = snapshotType;
		LOGGER = elfLog;
		this.auditLog = new AaiScheduledTaskAuditLog();
	}

	
	public void run(){
		LOGGER = LoggerFactory.getLogger(PrintVertexDetails.class);
		auditLog.logBefore("printVertexDetails", ONAPComponents.AAI.toString());
		try {
			if (debugOn) {
				// This is much slower, but sometimes we need to find out which single line is
				// causing a failure
				try {
					int okCount = 0;
					int failCount = 0;
					Long debugDelayMsL = new Long(debugDelayMs);
					FileOutputStream subFileStr = new FileOutputStream(fname);
					
					GraphWriter graphWriter = null;
					if ("gryo".equalsIgnoreCase(snapshotType)) {
						graphWriter = jg.io(IoCore.gryo()).writer().create();
					} else {
						graphWriter = jg.io(IoCore.graphson()).writer().create();
					}
					
					GraphTraversalSource gts = jg.traversal();
					ArrayList<Vertex> vtxList = new ArrayList<Vertex> ();
					GraphTraversal<Vertex, Vertex> gt = gts.V(vtxIdList);
					while( gt.hasNext() ) {
						vtxList.add(gt.next());
					}
					Iterator<Vertex> vSubItr = vtxList.iterator();
					while (vSubItr.hasNext()) {
						Long vertexIdL = 0L;
						String aaiNodeType = "";
						String aaiUri = "";
						String aaiUuid = "";
						try {
							Vertex tmpV = vSubItr.next();
							vertexIdL = (Long) tmpV.id();
							aaiNodeType = (String) tmpV.property("aai-node-type").orElse(null);
							aaiUri = (String) tmpV.property("aai-uri").orElse(null);
							aaiUuid = (String) tmpV.property("aai-uuid").orElse(null);

							Thread.sleep(debugDelayMsL); // Make sure it doesn't bump into itself
							graphWriter.writeVertex(subFileStr, tmpV, Direction.BOTH);
							subFileStr.write(newLineBytes);
							okCount++;
						} catch (Exception e) {
							failCount++;
							String fmsg = " >> DEBUG MODE >> Failed at:  VertexId = [" + vertexIdL
									+ "], aai-node-type = [" + aaiNodeType + "], aai-uuid = [" + aaiUuid
									+ "], aai-uri = [" + aaiUri + "]. ";
							System.out.println(fmsg);
							LOGGER.debug(" PrintVertexDetails " + fmsg);
							// e.printStackTrace();
						}
					}
					System.out.println(" -- Printed " + okCount + " vertexes out to " + fname + ", with " + failCount
							+ " failed.");
					subFileStr.close();
				} catch (Exception e) {
					AAIException ae = new AAIException("AAI_6128", e , "Error running PrintVertexDetails in debugon");
					ErrorLogHelper.logException(ae);
				}
			} else {
				// Not in DEBUG mode, so we'll do all the nodes in one group
				GraphTraversalSource gts = jg.traversal();
				ArrayList<Vertex> vtxList = new ArrayList<Vertex> ();
				GraphTraversal<Vertex, Vertex> gt = gts.V(vtxIdList);
				while( gt.hasNext() ) {
					vtxList.add(gt.next());
				}
				
				try {
					int count = vtxList.size();
					Iterator<Vertex> vSubItr = vtxList.iterator();
					FileOutputStream subFileStr = new FileOutputStream(fname);
					if ("gryo".equalsIgnoreCase(snapshotType)) {
						jg.io(IoCore.gryo()).writer().create().writeVertices(subFileStr, vSubItr, Direction.BOTH);
					} else {
						jg.io(IoCore.graphson()).writer().create().writeVertices(subFileStr, vSubItr, Direction.BOTH);
					}
					subFileStr.close();
					String pmsg = " -- Printed " + count + " vertexes out to " + fname;
					System.out.println(pmsg);
					LOGGER.debug(" PrintVertexDetails " + pmsg);
				} catch (Exception e) {
					AAIException ae = new AAIException("AAI_6128", e , "Error running PrintVertexDetails in else");
					ErrorLogHelper.logException(ae);
				}
			}
		}
		catch(Exception e){
			AAIException ae = new AAIException("AAI_6128", e , "Error running PrintVertexDetails");
			ErrorLogHelper.logException(ae);
		}
		finally {
			// Make sure the transaction this thread was using is freed up.
			jg.tx().commit();
			jg.tx().close();
		}
		auditLog.logAfter();
	}  
	
}	 