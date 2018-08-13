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


import java.util.HashMap;
import java.util.Iterator;
import java.util.List;
import java.util.Map;
import org.apache.tinkerpop.gremlin.structure.Edge;
import org.apache.tinkerpop.gremlin.structure.Property;
import org.apache.tinkerpop.gremlin.structure.Direction;
import org.apache.tinkerpop.gremlin.structure.Vertex;
import org.javatuples.Pair;
import org.onap.aai.db.props.AAIProperties;
import org.onap.aai.edges.EdgeIngestor;
import org.onap.aai.introspection.LoaderFactory;
import org.onap.aai.serialization.db.EdgeSerializer;
import org.onap.aai.serialization.engines.TransactionalGraphEngine;
import org.onap.aai.setup.SchemaVersions;

/**
 * A migration template for "swinging" edges that terminate on an old-node to a new target node. 
 *     That is, given an oldNode and a newNode we will swing edges that terminate on the
 *     oldNode and terminate them on the newNode (actually we drop the old edges and add new ones).
 *     
 *     
 *     We allow the passing of some parameters to restrict what edges get swung over: 
 *      > otherEndNodeTypeRestriction: only swing edges that terminate on the oldNode if the
 *                     node at the other end of the edge is of this nodeType.
 *      > edgeLabelRestriction: Only swing edges that have this edgeLabel
 *      > edgeDirectionRestriction: Only swing edges that go this direction (from the oldNode)
 *             this is a required parameter.  valid values are: BOTH, IN, OUT
 *     
 */
@MigrationPriority(0)
@MigrationDangerRating(1)
public abstract class EdgeSwingMigrator extends Migrator {

	private boolean success = true;
	private String nodeTypeRestriction = null;
	private String edgeLabelRestriction = null;  
	private String edgeDirRestriction = null;  
	private List<Pair<Vertex, Vertex>> nodePairList;
	
	
	public EdgeSwingMigrator(TransactionalGraphEngine engine , LoaderFactory loaderFactory, EdgeIngestor edgeIngestor, EdgeSerializer edgeSerializer, SchemaVersions schemaVersions) {
		super(engine, loaderFactory, edgeIngestor, edgeSerializer, schemaVersions);
	}
	

	/**
	 * Do not override this method as an inheritor of this class
	 */
	@Override
	public void run() {
		executeModifyOperation();
		cleanupAsAppropriate(this.nodePairList);
	}

	/**
	 * This is where inheritors should add their logic
	 */
	protected void executeModifyOperation() {
	
		try {
			this.nodeTypeRestriction = this.getNodeTypeRestriction();
			this.edgeLabelRestriction = this.getEdgeLabelRestriction();
			this.edgeDirRestriction = this.getEdgeDirRestriction();
			nodePairList = this.getAffectedNodePairs();
			for (Pair<Vertex, Vertex> nodePair : nodePairList) {
				Vertex fromNode = nodePair.getValue0();
				Vertex toNode = nodePair.getValue1();
				this.swingEdges(fromNode, toNode,
						this.nodeTypeRestriction,this.edgeLabelRestriction,this.edgeDirRestriction);
			}
		} catch (Exception e) {
			logger.error("error encountered", e);
			success = false;
		}
	}


	protected void swingEdges(Vertex oldNode, Vertex newNode, String nodeTypeRestr, String edgeLabelRestr, String edgeDirRestr) {
		try {
			// If the old and new Vertices aren't populated, throw an exception
			if( oldNode == null  ){
				logger.info ( "null oldNode passed to swingEdges() ");
				success = false;
				return;
			}
			else if( newNode == null ){
				logger.info ( "null newNode passed to swingEdges() ");
				success = false;
				return;
			}
			else if( edgeDirRestr == null ||
						(!edgeDirRestr.equals("BOTH") 
							&& !edgeDirRestr.equals("IN")  
							&& !edgeDirRestr.equals("OUT") )
						){
				logger.info ( "invalid direction passed to swingEdges(). valid values are BOTH/IN/OUT ");
				success = false;
				return;
			}
			else if( edgeLabelRestr != null 
					&& (edgeLabelRestr.trim().equals("none") || edgeLabelRestr.trim().equals("")) ){
				edgeLabelRestr = null;
			}
			else if( nodeTypeRestr == null || nodeTypeRestr.trim().equals("") ){
				nodeTypeRestr = "none";
			}
				
			String oldNodeType = oldNode.value(AAIProperties.NODE_TYPE);
			String oldUri = oldNode.<String> property("aai-uri").isPresent()  ? oldNode.<String> property("aai-uri").value() : "URI Not present"; 
			
			String newNodeType = newNode.value(AAIProperties.NODE_TYPE);
			String newUri = newNode.<String> property("aai-uri").isPresent()  ? newNode.<String> property("aai-uri").value() : "URI Not present"; 

			// If the nodeTypes don't match, throw an error 
			if( !oldNodeType.equals(newNodeType) ){
				logger.info ( "Can not swing edge from a [" + oldNodeType + "] node to a [" +
						newNodeType + "] node. ");
				success = false;
				return;
			}
			
			// Find and migrate any applicable OUT edges.
			if( edgeDirRestr.equals("BOTH") || edgeDirRestr.equals("OUT") ){
				Iterator <Edge> edgeOutIter = null;
				if( edgeLabelRestr == null ) {
					edgeOutIter = oldNode.edges(Direction.OUT);
				}
				else {
					edgeOutIter = oldNode.edges(Direction.OUT, edgeLabelRestr);
				}
				
				while( edgeOutIter.hasNext() ){
					Edge oldOutE = edgeOutIter.next();
					String eLabel = oldOutE.label();
					Vertex otherSideNode4ThisEdge = oldOutE.inVertex();
					String otherSideNodeType = otherSideNode4ThisEdge.value(AAIProperties.NODE_TYPE);
					if( nodeTypeRestr.equals("none") || nodeTypeRestr.toLowerCase().equals(otherSideNodeType) ){
						Iterator <Property<Object>> propsIter = oldOutE.properties();
						HashMap<String, String> propMap = new HashMap<String,String>();
						while( propsIter.hasNext() ){
							Property <Object> ep = propsIter.next();
							propMap.put(ep.key(), ep.value().toString());
						}
						
						String otherSideUri = otherSideNode4ThisEdge.<String> property("aai-uri").isPresent()  ? otherSideNode4ThisEdge.<String> property("aai-uri").value() : "URI Not present"; 
						logger.info ( "\nSwinging [" + eLabel + "] OUT edge.  \n    >> Unchanged side is [" 
								+ otherSideNodeType + "][" + otherSideUri + "] \n    >> Edge used to go to [" + oldNodeType 
								+ "][" + oldUri + "],\n    >> now swung to [" + newNodeType + "][" + newUri + "]. ");
						// remove the old edge
						oldOutE.remove();
						
						// add the new edge with properties that match the edge that was deleted.  We don't want to
						// change any edge properties - just swinging one end of the edge to a new node.
						// NOTE - addEdge adds an OUT edge to the vertex passed as a parameter, so we are 
						//       adding from the newNode side.
						Edge newOutE = newNode.addEdge(eLabel, otherSideNode4ThisEdge);
						
						Iterator it = propMap.entrySet().iterator();
					    while (it.hasNext()) {
					        Map.Entry pair = (Map.Entry)it.next();
					        newOutE.property(pair.getKey().toString(), pair.getValue().toString() );
					    }
					    
					}
				}
			}	
			
			// Find and migrate any applicable IN edges.
			if( edgeDirRestr.equals("BOTH") || edgeDirRestr.equals("IN") ){
				Iterator <Edge> edgeInIter = null;
				if( edgeLabelRestr == null ) {
					edgeInIter = oldNode.edges(Direction.IN);
				}
				else {
					edgeInIter = oldNode.edges(Direction.IN, edgeLabelRestr);
				}			
				
				while( edgeInIter.hasNext() ){
					Edge oldInE = edgeInIter.next();
					String eLabel = oldInE.label();
					Vertex otherSideNode4ThisEdge = oldInE.outVertex();
					String otherSideNodeType = otherSideNode4ThisEdge.value(AAIProperties.NODE_TYPE);
					if( nodeTypeRestr.equals("none") || nodeTypeRestr.toLowerCase().equals(otherSideNodeType) ){
						Iterator <Property<Object>> propsIter = oldInE.properties();
						HashMap<String, String> propMap = new HashMap<String,String>();
						while( propsIter.hasNext() ){
							Property <Object> ep = propsIter.next();
							propMap.put(ep.key(), ep.value().toString());
						}

						String otherSideUri = otherSideNode4ThisEdge.<String> property("aai-uri").isPresent()  ? otherSideNode4ThisEdge.<String> property("aai-uri").value() : "URI Not present"; 
						logger.info ( "\nSwinging [" + eLabel + "] IN edge.  \n    >> Unchanged side is  [" 
								+ otherSideNodeType + "][" + otherSideUri + "] \n    >>  Edge used to go to [" + oldNodeType 
								+ "][" + oldUri + "],\n    >>   now swung to [" + newNodeType + "][" + newUri + "]. ");
						
						// remove the old edge
						oldInE.remove();
						
						// add the new edge with properties that match the edge that was deleted.  We don't want to
						// change any edge properties - just swinging one end of the edge to a new node.
						// NOTE - addEdge adds an OUT edge to the vertex passed as a parameter, so we are 
						//       adding from the node on the other-end of the original edge so we'll get 
						//       an IN-edge to the newNode.
						Edge newInE = otherSideNode4ThisEdge.addEdge(eLabel, newNode);
						
						Iterator it = propMap.entrySet().iterator();
					    while (it.hasNext()) {
					        Map.Entry pair = (Map.Entry)it.next();
					        newInE.property(pair.getKey().toString(), pair.getValue().toString() );
					    } 
					}
				}
			}	
			
		} catch (Exception e) {
			logger.error("error encountered", e);
			success = false;
		}
	}
  
	@Override
	public Status getStatus() {
		if (success) {
			return Status.SUCCESS;
		} else {
			return Status.FAILURE;
		}
	}
	
	
	/**
	 * Get the List of node pairs("from" and "to"), you would like EdgeSwingMigrator to migrate from json files
	 * @return
	 */
	public abstract List<Pair<Vertex, Vertex>> getAffectedNodePairs() ;
	
	
	/**
	 * Get the nodeTypeRestriction that you want EdgeSwingMigrator to use
	 * @return
	 */
	public abstract String getNodeTypeRestriction() ;
	
	
	/**
	 * Get the nodeTypeRestriction that you want EdgeSwingMigrator to use
	 * @return
	 */
	public abstract String getEdgeLabelRestriction() ;
	
	/**
	 * Get the nodeTypeRestriction that you want EdgeSwingMigrator to use
	 * @return
	 */
	public abstract String getEdgeDirRestriction() ;
	

	
	/**
	 * Cleanup (remove) the nodes that edges were moved off of if appropriate
	 * @return
	 */
	public abstract void cleanupAsAppropriate(List<Pair<Vertex, Vertex>> nodePairL);

}