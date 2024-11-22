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
package org.onap.aai.dbgen;

import java.io.FileInputStream;
import java.io.FileNotFoundException;
import java.io.InputStream;
import java.util.ArrayList;
import java.util.Iterator;
import java.util.Properties;
import java.util.Scanner;
import java.util.UUID;

import org.apache.tinkerpop.gremlin.process.traversal.dsl.graph.GraphTraversal;
import org.apache.tinkerpop.gremlin.process.traversal.dsl.graph.__;
import org.apache.tinkerpop.gremlin.structure.Direction;
import org.apache.tinkerpop.gremlin.structure.Edge;
import org.apache.tinkerpop.gremlin.structure.Vertex;
import org.apache.tinkerpop.gremlin.structure.VertexProperty;
import org.onap.aai.dbmap.AAIGraphConfig;
import org.onap.aai.dbmap.AAIGraph;
import org.onap.aai.exceptions.AAIException;
import org.onap.aai.logging.LogFormatTools;
import org.onap.aai.edges.enums.AAIDirection;
import org.onap.aai.edges.enums.EdgeProperty;
import org.onap.aai.util.AAIConfig;
import org.onap.aai.util.AAIConstants;
import org.slf4j.MDC;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.janusgraph.core.JanusGraphFactory;
import org.janusgraph.core.JanusGraph;

public class ForceDeleteTool {
	private static final String FROMAPPID = "AAI-DB";
	private static final String TRANSID = UUID.randomUUID().toString();

	private static String graphType = "realdb";

	public static boolean SHOULD_EXIT_VM = true;

	public static int EXIT_VM_STATUS_CODE = -1;

	public static void exit(int statusCode) {
		if (SHOULD_EXIT_VM) {
			System.exit(1);
		}
		EXIT_VM_STATUS_CODE = statusCode;
	}

	/*
	 * The main method.
	 *
	 * @param args the arguments
	 */
	public static void main(String[] args) {

		// SWGK 01/21/2016 - To suppress the warning message when the tool is run from
		// the Terminal.

		System.setProperty("aai.service.name", ForceDelete.class.getSimpleName());
		Logger logger = LoggerFactory.getLogger(ForceDeleteTool.class.getSimpleName());
		MDC.put("logFilenameAppender", ForceDeleteTool.class.getSimpleName());

		String actionVal = "";
		String userIdVal = "";
		String dataString = "";
		Boolean displayAllVidsFlag = false; // Note - This should rarely be needed
		Boolean overRideProtection = false; // This should rarely be used - it overrides all our new checking
		long vertexIdLong = 0;
		String edgeIdStr = "";
		String argStr4Msg = "";

		if (args != null && args.length > 0) {
			// They passed some arguments in that will affect processing
			for (int i = 0; i < args.length; i++) {
				String thisArg = args[i];
				argStr4Msg = argStr4Msg + " " + thisArg;

				if (thisArg.equals("-action")) {
					i++;
					if (i >= args.length) {
						logger.error(" No value passed with -action option.  ");
						exit(0);
					}
					actionVal = args[i];
					argStr4Msg = argStr4Msg + " " + actionVal;
				} else if (thisArg.equals("-userId")) {
					i++;
					if (i >= args.length) {
						logger.error(" No value passed with -userId option.  ");
						exit(0);
					}
					userIdVal = args[i];
					argStr4Msg = argStr4Msg + " " + userIdVal;
				} else if (thisArg.equals("-overRideProtection")) {
					overRideProtection = true;
				} else if (thisArg.equals("-DISPLAY_ALL_VIDS")) {
					displayAllVidsFlag = true;
				} else if (thisArg.equals("-vertexId")) {
					i++;
					if (i >= args.length) {
						logger.error(" No value passed with -vertexId option.  ");
						exit(0);
					}
					String nextArg = args[i];
					argStr4Msg = argStr4Msg + " " + nextArg;
					try {
						vertexIdLong = Long.parseLong(nextArg);
					} catch (Exception e) {
						logger.error("Bad value passed with -vertexId option: ["
								+ nextArg + "]");
						exit(0);
					}
				} else if (thisArg.equals("-params4Collect")) {
					i++;
					if (i >= args.length) {
						logger.error(" No value passed with -params4Collect option.  ");
						exit(0);
					}
					dataString = args[i];
					argStr4Msg = argStr4Msg + " " + dataString;
				} else if (thisArg.equals("-edgeId")) {
					i++;
					if (i >= args.length) {
						logger.error(" No value passed with -edgeId option.  ");
						exit(0);
					}
					String nextArg = args[i];
					argStr4Msg = argStr4Msg + " " + nextArg;
					edgeIdStr = nextArg;
				} else {
					logger.error(" Unrecognized argument passed to ForceDeleteTool: ["
							+ thisArg + "]. ");
					logger.error(
							" Valid values are: -action -userId -vertexId -edgeId -overRideProtection -params4Collect -DISPLAY_ALL_VIDS");
					exit(0);
				}
			}
		}

		if (!actionVal.equals("COLLECT_DATA") && !actionVal.equals("DELETE_NODE") && !actionVal.equals("DELETE_EDGE")) {
			String emsg = "Bad action parameter [" + actionVal
					+ "] passed to ForceDeleteTool().  Valid values = COLLECT_DATA or DELETE_NODE or DELETE_EDGE\n";
			System.out.println(emsg);
			logger.error(emsg);
			exit(0);
		}

		if (actionVal.equals("DELETE_NODE") && vertexIdLong == 0) {
			String emsg = "ERROR: No vertex ID passed on DELETE_NODE request. \n";
			System.out.println(emsg);
			logger.error(emsg);
			exit(0);
		} else if (actionVal.equals("DELETE_EDGE") && edgeIdStr.equals("")) {
			String emsg = "ERROR: No edge ID passed on DELETE_EDGE request. \n";
			System.out.println(emsg);
			logger.error(emsg);
			exit(0);
		}

		userIdVal = userIdVal.trim();
		if ((userIdVal.length() < 6) || userIdVal.toUpperCase().equals("AAIADMIN")) {
			String emsg = "Bad userId parameter [" + userIdVal
					+ "] passed to ForceDeleteTool(). must be not empty and not aaiadmin \n";
			System.out.println(emsg);
			logger.error(emsg);
			exit(0);
		}

		String msg = "";
		JanusGraph graph = null;
		try {
			AAIConfig.init();
			System.out.println("    ---- NOTE --- about to open graph (takes a little while)--------\n");
			graph = setupGraph(logger);
			if (graph == null) {
				String emsg = "could not get graph object in ForceDeleteTool() \n";
				System.out.println(emsg);
				logger.error(emsg);
				exit(0);
			}
		} catch (AAIException e1) {
			msg = e1.getErrorObject().toString();
			System.out.println(msg);
			logger.error(msg);
			exit(0);
		} catch (Exception e2) {
			msg = e2.toString();
			System.out.println(msg);
			logger.error(msg);
			exit(0);
		}

		msg = "ForceDelete called by: userId [" + userIdVal + "] with these params: [" + argStr4Msg + "]";
		System.out.println(msg);
		logger.debug(msg);

		ForceDelete fd = new ForceDelete(graph);
		if (actionVal.equals("COLLECT_DATA")) {
			// When doing COLLECT_DATA, we expect them to either pass the vertexId or
			// that the dataString string to be comma separated name value pairs like this:
			// "propName1|propVal1,propName2|propVal2" etc. We will look for a node or nodes
			// that have properties that ALL match what was passed in.
			GraphTraversal<Vertex, Vertex> g = null;
			String qStringForMsg = "";
			int resCount = 0;
			if (vertexIdLong > 0) {
				// They know which vertex they want to look at
				qStringForMsg = "graph.vertices(" + vertexIdLong + ")";
				Iterator<Vertex> vtxItr = graph.vertices(vertexIdLong);
				if (vtxItr != null && vtxItr.hasNext()) {
					Vertex vtx = vtxItr.next();
					fd.showNodeInfo(logger, vtx, displayAllVidsFlag);
					resCount++;
				}
			} else {
				// we need to find the node or nodes based on the dataString
				int firstPipeLoc = dataString.indexOf("|");
				if (firstPipeLoc <= 0) {
					msg = "Must use the -params4Collect option when collecting data with data string in a format like: 'propName1|propVal1,propName2|propVal2'";
					System.out.println(msg);
					logger.error(msg);
					exit(0);
				}
				g = graph.traversal().V();
				qStringForMsg = " graph.traversal().V()";
				// Note - if they're only passing one parameter, there won't be any commas
				String[] paramArr = dataString.split(",");
				for (int i = 0; i < paramArr.length; i++) {
					int pipeLoc = paramArr[i].indexOf("|");
					if (pipeLoc <= 0) {
						msg = "Must use the -params4Collect option when collecting data with data string in a format like: 'propName1|propVal1,propName2|propVal2'";
						System.out.println(msg);
						logger.error(msg);
						exit(0);
					} else {
						String propName = paramArr[i].substring(0, pipeLoc);
						String propVal = paramArr[i].substring(pipeLoc + 1);
						g = g.has(propName, propVal);
						qStringForMsg = qStringForMsg + ".has(" + propName + "," + propVal + ")";
					}
				}

				if ((g != null)) {
					Iterator<Vertex> vertItor = g;
					while (vertItor.hasNext()) {
						resCount++;
						Vertex v = vertItor.next();
						fd.showNodeInfo(logger, v, displayAllVidsFlag);
						int descendantCount = fd.countDescendants(logger, v, 0);
						String infMsg = " Found " + descendantCount + " descendant nodes \n";
						System.out.println(infMsg);
						logger.debug(infMsg);
					}
				} else {
					msg = "Bad JanusGraphQuery object.  ";
					System.out.println(msg);
					logger.error(msg);
					exit(0);
				}
			}

			String infMsg = "\n\n Found: " + resCount + " nodes for this query: [" + qStringForMsg + "]\n";
			System.out.println(infMsg);
			logger.debug(infMsg);
		} else if (actionVal.equals("DELETE_NODE")) {
			Iterator<Vertex> vtxItr = graph.vertices(vertexIdLong);
			if (vtxItr != null && vtxItr.hasNext()) {
				Vertex vtx = vtxItr.next();
				fd.showNodeInfo(logger, vtx, displayAllVidsFlag);
				int descendantCount = fd.countDescendants(logger, vtx, 0);
				String infMsg = " Found " + descendantCount + " descendant nodes.  Note - forceDelete does not cascade to " +
						" child nodes, but they may become unreachable after the delete. \n";
				System.out.println(infMsg);
				logger.debug(infMsg);

				int edgeCount = fd.countEdges(logger, vtx);

				infMsg = " Found total of " + edgeCount + " edges incident on this node.  \n";
				System.out.println(infMsg);
				logger.debug(infMsg);

				if (fd.getNodeDelConfirmation(logger, userIdVal, vtx, descendantCount, edgeCount, overRideProtection)) {
					vtx.remove();
					graph.tx().commit();
					infMsg = ">>>>>>>>>> Removed node with vertexId = " + vertexIdLong;
					logger.debug(infMsg);
					System.out.println(infMsg);
				} else {
					infMsg = " Delete Cancelled. ";
					System.out.println(infMsg);
					logger.debug(infMsg);
				}
			} else {
				String infMsg = ">>>>>>>>>> Vertex with vertexId = " + vertexIdLong + " not found.";
				System.out.println(infMsg);
				logger.debug(infMsg);
			}
		} else if (actionVal.equals("DELETE_EDGE")) {
			Edge thisEdge = null;
			Iterator<Edge> edItr = graph.edges(edgeIdStr);
			if (edItr != null && edItr.hasNext()) {
				thisEdge = edItr.next();
			}

			if (thisEdge == null) {
				String infMsg = ">>>>>>>>>> Edge with edgeId = " + edgeIdStr + " not found.";
				logger.debug(infMsg);
				System.out.println(infMsg);
				exit(0);
			}

			if (fd.getEdgeDelConfirmation(logger, userIdVal, thisEdge, overRideProtection)) {
				thisEdge.remove();
				graph.tx().commit();
				String infMsg = ">>>>>>>>>> Removed edge with edgeId = " + edgeIdStr;
				logger.debug(infMsg);
				System.out.println(infMsg);
			} else {
				String infMsg = " Delete Cancelled. ";
				System.out.println(infMsg);
				logger.debug(infMsg);
			}
			exit(0);
		} else {
			String emsg = "Unknown action parameter [" + actionVal
					+ "] passed to ForceDeleteTool().  Valid values = COLLECT_DATA, DELETE_NODE or DELETE_EDGE \n";
			System.out.println(emsg);
			logger.debug(emsg);
			exit(0);
		}

		closeGraph(graph, logger);
		exit(0);

	}// end of main()

	public static class ForceDelete {

		private final int MAXDESCENDENTDEPTH = 15;
		private final JanusGraph graph;

		public ForceDelete(JanusGraph graph) {
			this.graph = graph;
		}

		public void showNodeInfo(Logger logger, Vertex tVert, Boolean displayAllVidsFlag) {

			try {
				Iterator<VertexProperty<Object>> pI = tVert.properties();
				String infStr = ">>> Found Vertex with VertexId = " + tVert.id() + ", properties:    ";
				System.out.println(infStr);
				logger.debug(infStr);
				while (pI.hasNext()) {
					VertexProperty<Object> tp = pI.next();
					infStr = " [" + tp.key() + "|" + tp.value() + "] ";
					System.out.println(infStr);
					logger.debug(infStr);
				}

				ArrayList<String> retArr = collectEdgeInfoForNode(logger, tVert, displayAllVidsFlag);
				for (String infoStr : retArr) {
					System.out.println(infoStr);
					logger.debug(infoStr);
				}
			} catch (Exception e) {
				String warnMsg = " -- Error -- trying to display edge info. [" + e.getMessage() + "]";
				System.out.println(warnMsg);
				logger.warn(warnMsg);
			}

		}// End of showNodeInfo()

		public void showPropertiesForEdge(Logger logger, Edge tEd) {
			String infMsg = "";
			if (tEd == null) {
				infMsg = "null Edge object passed to showPropertiesForEdge()";
				System.out.print(infMsg);
				logger.debug(infMsg);
				return;
			}

			// Try to show the edge properties
			try {
				infMsg = " Label for this Edge = [" + tEd.label() + "] ";
				System.out.print(infMsg);
				logger.debug(infMsg);

				infMsg = " EDGE Properties for edgeId = " + tEd.id() + ": ";
				System.out.print(infMsg);
				logger.debug(infMsg);
				Iterator<String> pI = tEd.keys().iterator();
				while (pI.hasNext()) {
					String propKey = pI.next();
					infMsg = "Prop: [" + propKey + "], val = ["
							+ tEd.property(propKey) + "] ";
					System.out.print(infMsg);
					logger.debug(infMsg);
				}
			} catch (Exception ex) {
				infMsg = " Could not retrieve properties for this edge. exMsg = ["
						+ ex.getMessage() + "] ";
				System.out.println(infMsg);
				logger.debug(infMsg);
			}

			// Try to show what's connected to the IN side of this Edge
			try {
				infMsg = " Looking for the Vertex on the IN side of the edge:  ";
				System.out.print(infMsg);
				logger.debug(infMsg);
				Vertex inVtx = tEd.inVertex();
				Iterator<VertexProperty<Object>> pI = inVtx.properties();
				String infStr = ">>> Found Vertex with VertexId = " + inVtx.id()
						+ ", properties:    ";
				System.out.println(infStr);
				logger.debug(infStr);
				while (pI.hasNext()) {
					VertexProperty<Object> tp = pI.next();
					infStr = " [" + tp.key() + "|" + tp.value() + "] ";
					System.out.println(infStr);
					logger.debug(infStr);
				}
			} catch (Exception ex) {
				infMsg = " Could not retrieve vertex data for the IN side of "
						+ "the edge. exMsg = [" + ex.getMessage() + "] ";
				System.out.println(infMsg);
				logger.debug(infMsg);
			}

			// Try to show what's connected to the OUT side of this Edge
			try {
				infMsg = " Looking for the Vertex on the OUT side of the edge:  ";
				System.out.print(infMsg);
				logger.debug(infMsg);
				Vertex outVtx = tEd.outVertex();
				Iterator<VertexProperty<Object>> pI = outVtx.properties();
				String infStr = ">>> Found Vertex with VertexId = " + outVtx.id()
						+ ", properties:    ";
				System.out.println(infStr);
				logger.debug(infStr);
				while (pI.hasNext()) {
					VertexProperty<Object> tp = pI.next();
					infStr = " [" + tp.key() + "|" + tp.value() + "] ";
					System.out.println(infStr);
					logger.debug(infStr);
				}
			} catch (Exception ex) {
				infMsg = " Could not retrieve vertex data for the OUT side of "
						+ "the edge. exMsg = [" + ex.getMessage() + "] ";
				System.out.println(infMsg);
				logger.debug(infMsg);
			}

		}// end showPropertiesForEdge()

		public ArrayList<String> collectEdgeInfoForNode(Logger logger, Vertex tVert, boolean displayAllVidsFlag) {
			ArrayList<String> retArr = new ArrayList<String>();
			Direction dir = Direction.OUT;
			for (int i = 0; i <= 1; i++) {
				if (i == 1) {
					// Second time through we'll look at the IN edges.
					dir = Direction.IN;
				}
				Iterator<Edge> eI = tVert.edges(dir);
				if (!eI.hasNext()) {
					retArr.add("No " + dir + " edges were found for this vertex. ");
				}
				while (eI.hasNext()) {
					Edge ed = eI.next();
					String edId = ed.id().toString();
					String lab = ed.label();
					Vertex vtx = null;
					if (dir == Direction.OUT) {
						// get the vtx on the "other" side
						vtx = ed.inVertex();
					} else {
						// get the vtx on the "other" side
						vtx = ed.outVertex();
					}
					if (vtx == null) {
						retArr.add(" >>> COULD NOT FIND VERTEX on the other side of this edge edgeId = " + ed.id() + " <<< ");
					} else {
						String nType = vtx.<String>property("aai-node-type").orElse(null);
						if (displayAllVidsFlag) {
							// This should rarely be needed
							String vid = vtx.id().toString();
							retArr.add("Found an " + dir + " edge (" + lab + ") with EDGE-ID = " + edId +
									", between this vertex and a [" + nType + "] node with VtxId = " + vid);
						} else {
							// This is the normal case
							retArr.add("Found an " + dir + " edge (" + lab + ") between this vertex and a [" + nType + "] node. ");
						}
					}
				}
			}
			return retArr;

		}// end of collectEdgeInfoForNode()

		public int countEdges(Logger logger, Vertex vtx) {
			int edgeCount = 0;
			try {
				Iterator<Edge> edgesItr = vtx.edges(Direction.BOTH);
				while (edgesItr.hasNext()) {
					edgesItr.next();
					edgeCount++;
				}
			} catch (Exception e) {
				String wMsg = "-- ERROR -- Stopping the counting of edges because of Exception [" + e.getMessage() + "]";
				System.out.println(wMsg);
				logger.warn(wMsg);
			}
			return edgeCount;

		}// end of countEdges()

		public int countDescendants(Logger logger, Vertex vtx, int levelVal) {
			int totalCount = 0;
			int thisLevel = levelVal + 1;

			if (thisLevel > MAXDESCENDENTDEPTH) {
				String wMsg = "Warning -- Stopping the counting of descendents because we reached the max depth of "
						+ MAXDESCENDENTDEPTH;
				System.out.println(wMsg);
				logger.warn(wMsg);
				return totalCount;
			}

			try {
				Iterator<Vertex> vertI = graph.traversal().V(vtx).union(
						__.outE().has(EdgeProperty.CONTAINS.toString(), AAIDirection.OUT.toString()).inV(),
						__.inE().has(EdgeProperty.CONTAINS.toString(), AAIDirection.IN.toString()).outV());
				while (vertI != null && vertI.hasNext()) {
					totalCount++;
					Vertex childVtx = vertI.next();
					totalCount = totalCount + countDescendants(logger, childVtx, thisLevel);
				}
			} catch (Exception e) {
				String wMsg = "Error -- Stopping the counting of descendents because of Exception [" + e.getMessage() + "]";
				System.out.println(wMsg);
				logger.warn(wMsg);

			}

			return totalCount;
		}// end of countDescendants()

		public boolean getEdgeDelConfirmation(Logger logger, String uid, Edge ed,
				Boolean overRideProtection) {

			showPropertiesForEdge(logger, ed);
			System.out.print("\n Are you sure you want to delete this EDGE? (y/n): ");
			Scanner s = new Scanner(System.in);
			s.useDelimiter("");
			String confirm = s.next();
			s.close();

			if (!confirm.equalsIgnoreCase("y")) {
				String infMsg = " User [" + uid + "] has chosen to abandon this delete request. ";
				System.out.println("\n" + infMsg);
				logger.debug(infMsg);
				return false;
			} else {
				String infMsg = " User [" + uid + "] has confirmed this delete request. ";
				System.out.println("\n" + infMsg);
				logger.debug(infMsg);
				return true;
			}

		} // End of getEdgeDelConfirmation()

		public boolean getNodeDelConfirmation(Logger logger, String uid, Vertex vtx, int edgeCount,
				int descendantCount, Boolean overRideProtection) {
			String thisNodeType = "";
			try {
				thisNodeType = vtx.<String>property("aai-node-type").orElse(null);
			} catch (Exception nfe) {
				// Let the user know something is going on - but they can confirm the delete if
				// they want to.
				String infMsg = " -- WARNING -- could not get an aai-node-type for this vertex. -- WARNING -- ";
				System.out.println(infMsg);
				logger.warn(infMsg);
			}

			String ntListString = "";
			String maxDescString = "";
			String maxEdgeString = "";

			int maxDescCount = 10; // default value
			int maxEdgeCount = 10; // default value
			ArrayList<String> protectedNTypes = new ArrayList<String>();
			protectedNTypes.add("cloud-region"); // default value

			try {
				ntListString = AAIConfig.get("aai.forceDel.protected.nt.list");
				maxDescString = AAIConfig.get("aai.forceDel.protected.descendant.count");
				maxEdgeString = AAIConfig.get("aai.forceDel.protected.edge.count");
			} catch (Exception nfe) {
				// Don't worry, we will use default values
				String infMsg = "-- WARNING -- could not get aai.forceDel.protected values from aaiconfig.properties -- will use default values. ";
				System.out.println(infMsg);
				logger.warn(infMsg);
			}

			if (maxDescString != null && !maxDescString.equals("")) {
				try {
					maxDescCount = Integer.parseInt(maxDescString);
				} catch (Exception nfe) {
					// Don't worry, we will leave "maxDescCount" set to the default value
				}
			}

			if (maxEdgeString != null && !maxEdgeString.equals("")) {
				try {
					maxEdgeCount = Integer.parseInt(maxEdgeString);
				} catch (Exception nfe) {
					// Don't worry, we will leave "maxEdgeCount" set to the default value
				}
			}

			if (ntListString != null && !ntListString.trim().equals("")) {
				String[] nodeTypes = ntListString.split("\\|");
				for (int i = 0; i < nodeTypes.length; i++) {
					protectedNTypes.add(nodeTypes[i]);
				}
			}

			boolean giveProtOverRideMsg = false;
			boolean giveProtErrorMsg = false;
			if (descendantCount > maxDescCount) {
				// They are trying to delete a node with a lots of descendants
				String infMsg = " >> WARNING >> This node has more descendant edges than the max ProtectedDescendantCount: "
						+ edgeCount + ".  Max = " +
						maxEdgeCount + ".  It can be DANGEROUS to delete one of these. << WARNING << ";
				System.out.println(infMsg);
				logger.debug(infMsg);
				if (!overRideProtection) {
					// They cannot delete this kind of node without using the override option
					giveProtErrorMsg = true;
				} else {
					giveProtOverRideMsg = true;
				}
			}

			if (edgeCount > maxEdgeCount) {
				// They are trying to delete a node with a lot of edges
				String infMsg = " >> WARNING >> This node has more edges than the max ProtectedEdgeCount: " + edgeCount
						+ ".  Max = " +
						maxEdgeCount + ".  It can be DANGEROUS to delete one of these. << WARNING << ";
				System.out.println(infMsg);
				logger.debug(infMsg);
				if (!overRideProtection) {
					// They cannot delete this kind of node without using the override option
					giveProtErrorMsg = true;
				} else {
					giveProtOverRideMsg = true;
				}
			}

			if (thisNodeType != null && !thisNodeType.equals("") && protectedNTypes.contains(thisNodeType)) {
				// They are trying to delete a protected Node Type
				String infMsg = " >> WARNING >> This node is a PROTECTED NODE-TYPE (" + thisNodeType + "). " +
						" It can be DANGEROUS to delete one of these. << WARNING << ";
				System.out.println(infMsg);
				logger.debug(infMsg);
				if (!overRideProtection) {
					// They cannot delete this kind of node without using the override option
					giveProtErrorMsg = true;
				} else {
					giveProtOverRideMsg = true;
				}
			}

			if (giveProtOverRideMsg) {
				String infMsg = " !!>> WARNING >>!! you are using the overRideProtection parameter which will let you do this potentially dangerous delete.";
				System.out.println("\n" + infMsg);
				logger.debug(infMsg);
			} else if (giveProtErrorMsg) {
				String errMsg = " ERROR >> this kind of node can only be deleted if you pass the overRideProtection parameter.";
				System.out.println("\n" + errMsg);
				logger.error(errMsg);
				return false;
			}

			System.out.print("\n Are you sure you want to do this delete? (y/n): ");
			Scanner s = new Scanner(System.in);
			s.useDelimiter("");
			String confirm = s.next();
			s.close();

			if (!confirm.equalsIgnoreCase("y")) {
				String infMsg = " User [" + uid + "] has chosen to abandon this delete request. ";
				System.out.println("\n" + infMsg);
				logger.debug(infMsg);
				return false;
			} else {
				String infMsg = " User [" + uid + "] has confirmed this delete request. ";
				System.out.println("\n" + infMsg);
				logger.debug(infMsg);
				return true;
			}

		} // End of getNodeDelConfirmation()
	}

	public static JanusGraph setupGraph(Logger logger) {

		JanusGraph janusGraph = null;

		try (InputStream inputStream = new FileInputStream(AAIConstants.REALTIME_DB_CONFIG);) {

			Properties properties = new Properties();
			properties.load(inputStream);

			if ("inmemory".equals(properties.get("storage.backend"))) {
				janusGraph = AAIGraph.getInstance().getGraph();
				graphType = "inmemory";
			} else {
				janusGraph = JanusGraphFactory.open(
						new AAIGraphConfig.Builder(AAIConstants.REALTIME_DB_CONFIG)
								.forService(ForceDeleteTool.class.getSimpleName())
								.withGraphType("realtime1")
								.buildConfiguration());
			}
		} catch (Exception e) {
			logger.error("Unable to open the graph", LogFormatTools.getStackTop(e));
		}

		return janusGraph;
	}

	public static void closeGraph(JanusGraph graph, Logger logger) {

		try {
			if ("inmemory".equals(graphType)) {
				return;
			}
			if (graph != null && graph.isOpen()) {
				graph.tx().close();
				graph.close();
			}
		} catch (Exception ex) {
			// Don't throw anything because JanusGraph sometimes is just saying that the
			// graph is already closed{
			logger.warn("WARNING from final graph.shutdown()", ex);
		}
	}
}
