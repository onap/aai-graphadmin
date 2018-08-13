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
import org.apache.tinkerpop.gremlin.structure.Direction;
import org.apache.tinkerpop.gremlin.structure.Vertex;
import org.apache.tinkerpop.gremlin.structure.io.IoCore;
import org.janusgraph.core.JanusGraph;


public class PrintVertexDetails implements Runnable{
	
	//private static EELFLogger LOGGER;

	private JanusGraph jg;
	private String fname;
	private ArrayList<Vertex> vtxList;
	private Boolean debugOn;
	private int debugDelayMs;
		
	public PrintVertexDetails (JanusGraph graph, String fn, ArrayList<Vertex> vL, Boolean debugFlag, int debugDelay){
		jg = graph;
		fname = fn;
		vtxList = vL;
		debugOn = debugFlag;
		debugDelayMs = debugDelay;
	}
		
	public void run(){  
		if( debugOn ){
			// This is much slower, but sometimes we need to find out which single line is causing a failure
			try{
				int okCount = 0;
				int failCount = 0;
				Long debugDelayMsL = new Long(debugDelayMs);
				FileOutputStream subFileStr = new FileOutputStream(fname);
				Iterator <Vertex> vSubItr = vtxList.iterator();
				while( vSubItr.hasNext() ){
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
						jg.io(IoCore.graphson()).writer().create().writeVertex(subFileStr, tmpV, Direction.BOTH); 
						okCount++;
					}
					catch(Exception e) {
						failCount++;
						System.out.println(" >> DEBUG MODE >> Failed at:  VertexId = [" + vertexIdL + 
								"], aai-node-type = [" + aaiNodeType + 
								"], aai-uuid = [" + aaiUuid + 
								"], aai-uri = [" + aaiUri + "]. " );
						e.printStackTrace();
					}
				}
				System.out.println(" -- Printed " + okCount + " vertexes out to " + fname +
						", with " + failCount + " failed.");
				subFileStr.close();
			}
			catch(Exception e){
				e.printStackTrace();
			}  	
		}
		else {
			// Not in DEBUG mode, so we'll do all the nodes in one group
			try{
				int count = vtxList.size();
				Iterator <Vertex> vSubItr = vtxList.iterator();
				FileOutputStream subFileStr = new FileOutputStream(fname);
				jg.io(IoCore.graphson()).writer().create().writeVertices(subFileStr, vSubItr, Direction.BOTH);
				subFileStr.close();
				System.out.println(" -- Printed " + count + " vertexes out to " + fname);
			}
			catch(Exception e){
				e.printStackTrace();
			}  	
		}
	}  
	
}	 