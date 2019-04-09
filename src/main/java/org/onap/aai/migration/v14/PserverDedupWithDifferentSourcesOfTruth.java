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
package org.onap.aai.migration.v14;

import org.apache.tinkerpop.gremlin.process.traversal.dsl.graph.GraphTraversalSource;
import org.apache.tinkerpop.gremlin.process.traversal.dsl.graph.__;
import org.apache.tinkerpop.gremlin.process.traversal.P;
import org.apache.tinkerpop.gremlin.structure.*;
import org.janusgraph.core.attribute.Text;
import org.javatuples.Pair;
import org.onap.aai.db.props.AAIProperties;
import org.onap.aai.edges.EdgeIngestor;
import org.onap.aai.edges.enums.AAIDirection;
import org.onap.aai.edges.enums.EdgeProperty;
import org.onap.aai.exceptions.AAIException;
import org.onap.aai.introspection.LoaderFactory;
import org.onap.aai.migration.*;
import org.onap.aai.serialization.engines.TransactionalGraphEngine;
import org.springframework.web.util.UriUtils;

import javax.ws.rs.core.UriBuilder;

import java.io.UnsupportedEncodingException;
import java.net.URI;
import java.util.*;
import java.util.stream.Collectors;

import org.onap.aai.serialization.db.EdgeSerializer;
import org.onap.aai.setup.SchemaVersions;
import org.onap.aai.introspection.Introspector;

//@Enabled
@MigrationPriority(10)
@MigrationDangerRating(100)
public class PserverDedupWithDifferentSourcesOfTruth extends EdgeSwingMigrator {
    /**
     * Instantiates a new migrator.
     *
     * @param engine
     */
    private final String PARENT_NODE_TYPE = "pserver";
    private boolean success = true;
    protected Set<Object> seen = new HashSet<>();
    private Map<String, UriBuilder> nodeTypeToUri;
    private Map<String, Set<String>> nodeTypeToKeys;
    private static List<String> dmaapMsgList = new ArrayList<String>();
    private static List<Introspector> dmaapDeleteList = new ArrayList<Introspector>();
    private static int pserversUpdatedCount = 0;
    private static int pserversDeletedCount = 0;
    
    
    private static String[] rctSourceOfTruth = new String[]{"AAIRctFeed", "RCT"};
    private static String[] roSourceOfTruth = new String[]{"AAI-EXTENSIONS", "RO"};

    List<Vertex> RemoveROList = new ArrayList<>();

    public PserverDedupWithDifferentSourcesOfTruth(TransactionalGraphEngine engine , LoaderFactory loaderFactory, EdgeIngestor edgeIngestor, EdgeSerializer edgeSerializer, SchemaVersions schemaVersions) {
        super(engine, loaderFactory, edgeIngestor, edgeSerializer, schemaVersions);
    }
    @Override
    public void commit() {
        engine.commit();
        createDmaapFiles(dmaapMsgList);
        createDmaapFilesForDelete(dmaapDeleteList);

    }

    @Override
    public Status getStatus() {
    	if (success) {
            return Status.SUCCESS;
        }
        else {
            return Status.FAILURE;
        }
    }

    @Override
    public List<Pair<Vertex, Vertex>> getAffectedNodePairs() {
        return null;
    }

    @Override
    public String getNodeTypeRestriction() {
        return null;
    }

    @Override
    public String getEdgeLabelRestriction() {
        return null;
    }

    @Override
    public String getEdgeDirRestriction() {
        return null;
    }

    @Override
    public void cleanupAsAppropriate(List<Pair<Vertex, Vertex>> nodePairL) {

    }

    @Override
    public Optional<String[]> getAffectedNodeTypes() {
        return null;
    }

    @Override
    public String getMigrationName() {
        return "PserverDedupWithDifferentSourcesOfTruth";
    }

    @Override
    public void run() {
    	
    	int dupCount = 0;
        nodeTypeToUri = loader.getAllObjects().entrySet().stream().filter(e -> e.getValue().getGenericURI().contains("{")).collect(
                Collectors.toMap(
                        e -> e.getKey(),
                        e -> UriBuilder.fromPath(e.getValue().getFullGenericURI().replaceAll("\\{"+ e.getKey() + "-", "{"))
                ));

        nodeTypeToKeys = loader.getAllObjects().entrySet().stream().filter(e -> e.getValue().getGenericURI().contains("{")).collect(
                Collectors.toMap(
                        e -> e.getKey(),
                        e -> e.getValue().getKeys()
                ));

        List<Vertex> rctList = graphTraversalSource().V().has("aai-node-type", "pserver").has("source-of-truth", P.within(rctSourceOfTruth)).toList();
        List<Vertex> roList =  graphTraversalSource().V().has("aai-node-type", "pserver").has("source-of-truth", P.within(roSourceOfTruth)).toList();
        
        logger.info("Total number of RCT sourced pservers in A&AI :" +rctList.size());
        logger.info("Total number of RO sourced pservers in A&AI :" +roList.size());
        
        for(int i=0;i<rctList.size();i++){
            Vertex currRct = rctList.get(i);
            Object currRctFqdn = null;
            if (currRct.property("fqdn").isPresent() && (currRct.property("fqdn").value() != null)){
            	currRctFqdn = currRct.property("fqdn").value();
            	logger.info("\n");
            	logger.info("Current RCT Pserver hostname: " + currRct.property("hostname").value().toString() + " fqdn: " +currRct.property("fqdn").value().toString());
	            for(int j=0;j<roList.size();j++){
	                Vertex currRo = roList.get(j);
	                Object currRoHostname = null;
	                if (currRo.property("hostname").isPresent()){
	                	currRoHostname = currRo.property("hostname").value();
	                }
	                if (currRoHostname != null){
		                String[] rctFqdnSplit = currRctFqdn.toString().split("\\.");
		                String[] roHostnameSplit = currRoHostname.toString().split("\\.");
		                if (rctFqdnSplit.length >0 && roHostnameSplit.length > 0){
			                if(!rctFqdnSplit[0].isEmpty() && !roHostnameSplit[0].isEmpty() && rctFqdnSplit[0].equals(roHostnameSplit[0])){
			                	logger.info("\tPserver match found - RO Pserver with hostname: "+currRo.property("hostname").value().toString());
			                	dupCount++;
			                    try {
			                        mergePservers(currRct,currRo);
			                        break;
			                    } catch (UnsupportedEncodingException e) {
			                        success = false;
			                    } catch (AAIException e) {
			                        success = false;
			                    }
			                }
		                }
	                }
	            }
            }
        }
        RemoveROList.forEach(v ->v.remove());
        logger.info ("\n \n ******* Migration Summary Counts for Dedup of RCT and RO sourced pservers ********* \n");
        logger.info(this.MIGRATION_SUMMARY_COUNT + "Total number of RCT: " +rctList.size());
        logger.info(this.MIGRATION_SUMMARY_COUNT + "Total number of RO: " +roList.size());
        logger.info(this.MIGRATION_SUMMARY_COUNT + "Duplicate pserver count: "+ dupCount);
        logger.info(this.MIGRATION_SUMMARY_COUNT + "Number of RCT updated: "+pserversUpdatedCount);
        logger.info(this.MIGRATION_SUMMARY_COUNT + "Number of RO deleted: "+ pserversDeletedCount +"\n");
    }
	private GraphTraversalSource graphTraversalSource() {
		return this.engine.asAdmin().getTraversalSource();
	}


    public void mergePservers(Vertex rct, Vertex ro) throws UnsupportedEncodingException, AAIException {
        Introspector obj = serializer.getLatestVersionView(ro);
        dmaapDeleteList.add(obj);
        rct.property("fqdn",ro.property("hostname").value().toString());
        dropComplexEdge(ro);
        dropMatchingROPInterfaces(ro, rct);
        dropMatchingROLagInterfaces(ro, rct);
        swingEdges(ro, rct, null, null, "BOTH");
        modifyChildrenUri(rct);
        if(!(rct.property("pserver-id").isPresent())){
            rct.property("pserver-id",UUID.randomUUID().toString());
        }
        String dmaapMsg = System.nanoTime() + "_" + rct.id().toString() + "_"	+ rct.value("resource-version").toString();
        dmaapMsgList.add(dmaapMsg);
        pserversUpdatedCount++;
        logger.info("\tAdding RO pserver to the delete list....");
        RemoveROList.add(ro);
        pserversDeletedCount++;
    }

    private void dropMatchingROPInterfaces(Vertex ro, Vertex rct) {
        Map<String, Vertex> removeROPIntMap = new HashMap<String, Vertex>();
    	List<Vertex> pIntList = graphTraversalSource().V(ro).in("tosca.relationships.network.BindsTo").has("aai-node-type","p-interface").toList();
    	if (pIntList != null && !pIntList.isEmpty()) {
	        Iterator<Vertex> pIntListItr = pIntList.iterator();
	        while(pIntListItr.hasNext()){
	        	Vertex pInt = pIntListItr.next();
	        	
	        	removeROPIntMap.put(pInt.property("interface-name").value().toString(), pInt);
	        }
	        Set<String> interfaceNameSet = removeROPIntMap.keySet();
	        List<Vertex> rctPIntList = graphTraversalSource().V(rct).in("tosca.relationships.network.BindsTo").has("aai-node-type","p-interface").toList();
	        if (rctPIntList != null && !rctPIntList.isEmpty()){
		        Iterator<Vertex> rctPIntListItr = rctPIntList.iterator();
		        while(rctPIntListItr.hasNext()){
		        	Vertex rctPInt = rctPIntListItr.next();
		        	String rctIntfName = rctPInt.property("interface-name").value().toString();
		        	if (interfaceNameSet.contains(rctIntfName)){
		        		Vertex pIntToRemoveFromROPserver = removeROPIntMap.get(rctIntfName);
		        		String roPIntUri = "roPIntUri";
		        		if (pIntToRemoveFromROPserver.property("aai-uri").isPresent()){
		        			roPIntUri = pIntToRemoveFromROPserver.property("aai-uri").value().toString();
		        		}
		        		Edge roPIntToPserverEdge = pIntToRemoveFromROPserver.edges(Direction.OUT, "tosca.relationships.network.BindsTo").next();
		        		roPIntToPserverEdge.remove();
		        		pIntToRemoveFromROPserver.remove();
		        		logger.info("\tRemoved p-interface "+roPIntUri + " and its edge to RO pserver, not swinging the p-interface to RCT pserver");
		        	}
		        }
	        }
    	} 
	}
    
    private void dropMatchingROLagInterfaces(Vertex ro, Vertex rct) {
        Map<String, Vertex> removeROLagIntMap = new HashMap<String, Vertex>();
    	List<Vertex> lagIntList = graphTraversalSource().V(ro).in("tosca.relationships.network.BindsTo").has("aai-node-type","lag-interface").toList();
    	if (lagIntList != null && !lagIntList.isEmpty()) {
	        Iterator<Vertex> lagIntListItr = lagIntList.iterator();
	        while(lagIntListItr.hasNext()){
	        	Vertex lagInt = lagIntListItr.next();
	        	
	        	removeROLagIntMap.put(lagInt.property("interface-name").value().toString(), lagInt);
	        }
	        Set<String> interfaceNameSet = removeROLagIntMap.keySet();
	        List<Vertex> rctLagIntList = graphTraversalSource().V(rct).in("tosca.relationships.network.BindsTo").has("aai-node-type","lag-interface").toList();
	        if (rctLagIntList != null && !rctLagIntList.isEmpty()){
		        Iterator<Vertex> rctLagIntListItr = rctLagIntList.iterator();
		        while(rctLagIntListItr.hasNext()){
		        	Vertex rctPInt = rctLagIntListItr.next();
		        	String rctIntfName = rctPInt.property("interface-name").value().toString();
		        	if (interfaceNameSet.contains(rctIntfName)){
		        		Vertex lagIntToRemoveFromROPserver = removeROLagIntMap.get(rctIntfName);
		        		String roLagIntUri = "roPIntUri";
		        		if (lagIntToRemoveFromROPserver.property("aai-uri").isPresent()){
		        			roLagIntUri = lagIntToRemoveFromROPserver.property("aai-uri").value().toString();
		        		}
		        		Edge roLagIntToPserverEdge = lagIntToRemoveFromROPserver.edges(Direction.OUT, "tosca.relationships.network.BindsTo").next();
		        		roLagIntToPserverEdge.remove();
		        		lagIntToRemoveFromROPserver.remove();
		        		logger.info("\tRemoved lag-interface "+roLagIntUri + " and its edge to RO pserver, not swinging the lag-interface to RCT pserver");
		        	}
		        }
	        }
    	} 
	}
    
	public void dropComplexEdge(Vertex ro){
    	List<Vertex> locatedInEdgeVertexList = graphTraversalSource().V(ro).has("aai-node-type", "pserver").out("org.onap.relationships.inventory.LocatedIn").has("aai-node-type","complex").toList();
    	if (locatedInEdgeVertexList != null && !locatedInEdgeVertexList.isEmpty()){
    		Iterator<Vertex> locatedInEdgeVertexListItr = locatedInEdgeVertexList.iterator();
    		while (locatedInEdgeVertexListItr.hasNext()){
    			Vertex v = locatedInEdgeVertexListItr.next();
    			if ("complex".equalsIgnoreCase(v.property("aai-node-type").value().toString())){
    				Edge pserverToComplexEdge = v.edges(Direction.IN, "org.onap.relationships.inventory.LocatedIn").next();
    				pserverToComplexEdge.remove();
    			}
    		}
    	}
    }


    private void modifyChildrenUri(Vertex v) throws UnsupportedEncodingException, AAIException {
        Set<Vertex> parentSet = new HashSet<>();
        parentSet.add(v);
        verifyOrAddUri("", parentSet);
    }


    protected void verifyOrAddUri(String parentUri, Set<Vertex> vertexSet) throws UnsupportedEncodingException, AAIException {


        String correctUri;
        for (Vertex v : vertexSet) {
            seen.add(v.id());
            //if there is an issue generating the uri catch, log and move on;
            try {
                correctUri = parentUri + this.getUriForVertex(v);
            } catch (Exception e) {
                logger.error("Vertex has issue generating uri " + e.getMessage() + "\n\t" + this.asString(v));
                continue;
            }
            try {
                v.property(AAIProperties.AAI_URI, correctUri);
            } catch (Exception e) {
                logger.info("\t" + e.getMessage() + "\n\t" + this.asString(v));
            }
            if (!v.property(AAIProperties.AAI_UUID).isPresent()) {
                v.property(AAIProperties.AAI_UUID, UUID.randomUUID().toString());
            }
            this.verifyOrAddUri(correctUri, getChildren(v));
        }
    }

    protected Set<Vertex> getChildren(Vertex v) {

        Set<Vertex> children = graphTraversalSource().V(v).bothE().not(__.has(EdgeProperty.CONTAINS.toString(), AAIDirection.NONE.toString())).otherV().toSet();

        return children.stream().filter(child -> !seen.contains(child.id())).collect(Collectors.toSet());
    }

    protected String getUriForVertex(Vertex v) {
        String aaiNodeType = v.property(AAIProperties.NODE_TYPE).value().toString();


        Map<String, String> parameters = this.nodeTypeToKeys.get(aaiNodeType).stream().collect(Collectors.toMap(
                key -> key,
                key -> encodeProp(v.property(key).value().toString())
        ));

        return this.nodeTypeToUri.get(aaiNodeType).buildFromEncodedMap(parameters).toString();
    }
    private static String encodeProp(String s) {
        try {
            return UriUtils.encode(s, "UTF-8");
        } catch (UnsupportedEncodingException e) {
            return "";
        }
    }

}
