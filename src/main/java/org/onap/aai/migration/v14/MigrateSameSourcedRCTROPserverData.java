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

import java.nio.charset.UnsupportedCharsetException;
import org.apache.tinkerpop.gremlin.process.traversal.dsl.graph.GraphTraversalSource;
import org.apache.tinkerpop.gremlin.process.traversal.dsl.graph.__;
import org.apache.tinkerpop.gremlin.structure.*;
import org.javatuples.Pair;
import org.onap.aai.db.props.AAIProperties;
import org.onap.aai.edges.EdgeIngestor;
import org.onap.aai.edges.enums.AAIDirection;
import org.onap.aai.edges.enums.EdgeProperty;
import org.onap.aai.exceptions.AAIException;
import org.onap.aai.introspection.LoaderFactory;
import org.onap.aai.migration.*;
import org.onap.aai.introspection.Introspector;
import org.onap.aai.serialization.db.EdgeSerializer;
import org.onap.aai.serialization.engines.TransactionalGraphEngine;
import org.apache.tinkerpop.gremlin.process.traversal.P;
import org.onap.aai.setup.SchemaVersions;
import org.springframework.web.util.UriUtils;

import javax.ws.rs.core.UriBuilder;

import java.io.UnsupportedEncodingException;
import java.util.*;
import java.util.stream.Collectors;

//@Enabled
@MigrationPriority(5)
@MigrationDangerRating(100)
public class MigrateSameSourcedRCTROPserverData extends EdgeSwingMigrator {
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
    Vertex complexFromOld;
    private static int dupROCount = 0;
    private static int roPserversUpdatedCount = 0;
    private static int roPserversDeletedCount = 0;
    private static int dupRctCount = 0;
    private static int rctPserversUpdatedCount = 0;
    private static int rctPserversDeletedCount = 0;
    
    public MigrateSameSourcedRCTROPserverData(TransactionalGraphEngine engine , LoaderFactory loaderFactory, EdgeIngestor edgeIngestor, EdgeSerializer edgeSerializer, SchemaVersions schemaVersions) {
        super(engine, loaderFactory, edgeIngestor, edgeSerializer, schemaVersions);
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
    public void commit() {
        engine.commit();
        createDmaapFiles(dmaapMsgList);
        createDmaapFilesForDelete(dmaapDeleteList);
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
        return Optional.of(new String[]{"lag-interface", "l-interface", "l3-interface-ipv4-address", "l3-interface-ipv6-address", "sriov-vf", "vlan", "p-interface", "sriov-pf"});
    }

    @Override
    public String getMigrationName() {
        return "MigrateCorrectRCTSourcedPserverData";
    }

    @Override
    public void run() {


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

        List<Vertex> pserverTraversalRCT = graphTraversalSource().V().has("aai-node-type", "pserver").has("source-of-truth", P.within("RCT", "AAIRctFeed")).toList();
        int rctCount = pserverTraversalRCT.size();
        
        try {
        	logger.info("RCT pserver count: "+rctCount);
            updateToLatestRCT(pserverTraversalRCT);
        } catch (UnsupportedEncodingException | AAIException e) {
          logger.info("UpdateToLatestRCT error: " + e.getMessage());
        }

      List<Vertex>  pserverTraversalRO = graphTraversalSource().V().has("aai-node-type", "pserver").has("source-of-truth", P.within("RO", "AAI-EXTENSIONS")).toList();
        int roCount = pserverTraversalRO.size();
        try {
        	logger.info("RO pserver count: "+roCount);
            updateToLatestRO(pserverTraversalRO);
        } catch (UnsupportedEncodingException | AAIException e) {
          logger.info("UpdateToLatestRO error: " + e.getMessage());
        }

      logger.info ("\n \n ******* Migration Summary Counts for RCT and RO sourced pservers in A&AI ********* \n");
        logger.info(this.MIGRATION_SUMMARY_COUNT + "Total number of RCT pservers: " +rctCount);
        logger.info(this.MIGRATION_SUMMARY_COUNT + "Duplicate RCT pserver count: "+ dupRctCount);
        logger.info(this.MIGRATION_SUMMARY_COUNT + "Number of RCT updated: "+ rctPserversUpdatedCount);
        logger.info(this.MIGRATION_SUMMARY_COUNT + "Number of RCT deleted: "+ rctPserversDeletedCount +"\n");
        
        logger.info(this.MIGRATION_SUMMARY_COUNT + "Total number of RO pservers: " +roCount);
        logger.info(this.MIGRATION_SUMMARY_COUNT + "Duplicate RO pserver count: "+ dupROCount);
        logger.info(this.MIGRATION_SUMMARY_COUNT + "Number of RO updated: "+ roPserversUpdatedCount);
        logger.info(this.MIGRATION_SUMMARY_COUNT + "Number of RO deleted: "+ roPserversDeletedCount +"\n");
    }

    public void updateToLatestRO(List<Vertex> list)  throws UnsupportedEncodingException, AAIException {
        List<Vertex> removeROList = new ArrayList<>();

        Vertex latestV = null;

        for(int i=0;i<list.size();i++){
            Vertex currV = list.get(i);
            
            if (removeROList.contains(currV)){
            	logger.info("RO Pserver: "+currV.property("hostname").value().toString() + "was already added to delete list. No further processing needed for this.");
            	continue;
            }
            logger.info("RO Pserver: "+currV.property("hostname").value().toString());

            for(int j=i+1; j<list.size();j++) {

                Vertex temp = list.get(j);

                String[] currentVHostname = currV.property("hostname").value().toString().split("\\.");
                String[] tempHostname = temp.property("hostname").value().toString().split("\\.");
                
                if (currentVHostname.length >0 && tempHostname.length > 0){
	                if (!currentVHostname[0].isEmpty() && !tempHostname[0].isEmpty() && currentVHostname[0].equals(tempHostname[0])) {
	                	dupROCount++;
	                	logger.info("\tTemp RO Pserver: "+temp.property("hostname").value().toString());
	                    if (temp.property("hostname").value().toString().length() > currV.property("hostname").value().toString().length()) {
	                        //temp is the latest vertex swing everything from currV to temp
	                        latestV = temp;
	                        movePlink(currV, latestV);
	                        moveLagInterfaces(currV, latestV);
	                        swingEdges(currV, latestV, null, null, "BOTH");
	                        modifyChildrenUri(latestV);
	                        String dmaapMsg = System.nanoTime() + "_" + temp.id().toString() + "_"	+ temp.value("resource-version").toString();
	                        dmaapMsgList.add(dmaapMsg);
	                        roPserversUpdatedCount++;
	                        logger.info("\tAdding pserver "+latestV.property("hostname").value().toString() + " to updated list");
	                        if (!removeROList.contains(list.get(i))) {
	                        	removeROList.add(list.get(i));
	                        	Introspector obj = serializer.getLatestVersionView(currV);//currV
	                        	logger.info("\tAdding pserver "+currV.property("hostname").value().toString() + " to delete list");
		                        dmaapDeleteList.add(obj);
		                        roPserversDeletedCount++;
	                        }
	                        currV = latestV;
	                    } else {
	                        //currV is the latest temp is the old vertex swing everything from temp to currV
	                        latestV = currV;
	                        movePlink(temp, latestV);
	                        moveLagInterfaces(temp, latestV);
	                        swingEdges(temp, latestV, null, null, "BOTH");
	                        modifyChildrenUri(latestV);
	                        String dmaapMsg = System.nanoTime() + "_" + currV.id().toString() + "_"	+ currV.value("resource-version").toString();
	                        dmaapMsgList.add(dmaapMsg);
	                        logger.info("\tAdding pserver "+latestV.property("hostname").value().toString() + " to updated list");
	                        roPserversUpdatedCount++;
	                        
	                        if (!removeROList.contains(list.get(j))) {
	                        	removeROList.add(list.get(j));
	                        	Introspector obj = serializer.getLatestVersionView(temp);//temp
	                        	logger.info("\tAdding pserver "+temp.property("hostname").value().toString() + " to delete list");
		                        dmaapDeleteList.add(obj);
		                        roPserversDeletedCount++;
	                        }
	                    }
	                }
	            }
            }
        }
        logger.info("\tCount of RO Pservers removed = "+removeROList.size()+"\n");
        removeROList.forEach(v ->v.remove());

    }

//    public void addComplexEdge(Vertex Latest) throws AAIException {
//
//        if(!(graphTraversalSource().V(Latest).has("aai-node-type", "pserver").out("org.onap.relationships.inventory.LocatedIn").has("aai-node-type","complex").hasNext())){
//        	if (complexFromOld != null)
//            createCousinEdge(Latest,complexFromOld);
//
//        }
//    }


//    public void dropComplexEdge(Vertex old){
//    	List<Vertex> locatedInEdgeVertexList = graphTraversalSource().V(old).has("aai-node-type", "pserver").out("org.onap.relationships.inventory.LocatedIn").has("aai-node-type","complex").toList();
//    	if (locatedInEdgeVertexList != null && !locatedInEdgeVertexList.isEmpty()){
//    		Iterator<Vertex> locatedInEdgeVertexListItr = locatedInEdgeVertexList.iterator();
//    		while (locatedInEdgeVertexListItr.hasNext()){
//    			complexFromOld = locatedInEdgeVertexListItr.next();
//    			if ("complex".equalsIgnoreCase(complexFromOld.property("aai-node-type").value().toString())){
//    				Edge pserverToComplexEdge = complexFromOld.edges(Direction.IN, "org.onap.relationships.inventory.LocatedIn").next();
//    				pserverToComplexEdge.remove();
//    			}
//    		}
//    	}
//    }


    private GraphTraversalSource graphTraversalSource() {
		return this.engine.asAdmin().getTraversalSource();
	}

	public void updateToLatestRCT(List<Vertex> list) throws UnsupportedEncodingException, AAIException {
        List<Vertex>removeRCTList = new ArrayList<>();

        Vertex latestV = null;
        for(int i=0;i<list.size();i++) {
            Vertex currV = list.get(i);
            if (!currV.property("fqdn").isPresent()){
            	continue;
            }
            
            if (removeRCTList.contains(currV)){
            	logger.info("RCT Pserver: "+currV.property("hostname").value().toString() + "was already added to delete list. No further processing needed for this.");
            	continue;
            }
            logger.info("RCT Pserver: "+currV.property("hostname").value().toString());
            for(int j=i+1;j<list.size();j++) {

                Vertex temp = list.get(j);
                if (temp.property("fqdn").isPresent()) {
                    String[] currentVFqdn = currV.property("fqdn").value().toString().split("\\.");
                    String[] tempFqdn = temp.property("fqdn").value().toString().split("\\.");
                    if (currentVFqdn.length >0 && tempFqdn.length > 0){
	                    String currentFqdnFirstToken = currentVFqdn[0];
	                    String tempFqdnFirstToken = tempFqdn[0];
	                    if (!currentFqdnFirstToken.isEmpty() && !tempFqdnFirstToken.isEmpty() && currentFqdnFirstToken.equals(tempFqdnFirstToken)) {
	                    	dupRctCount++;
	                    	logger.info("\tMatching Temp RCT Pserver: "+temp.property("hostname").value().toString());
	                    	long tempRV = Long.parseLong(temp.value("resource-version"));
	                    	long currRV = Long.parseLong(currV.value("resource-version"));
	                    	logger.info("\tcurrRV: "+currRV+ ", tempRV: "+tempRV);
	                        if (Long.parseLong(temp.value("resource-version")) > Long.parseLong(currV.value("resource-version"))) {
	                            //currv is old, temp vertex found in traversal is the latest
	                            latestV = temp;
	                            movePlink(currV, latestV);
	                            moveLagInterfaces(currV, latestV);
	                            swingEdges(currV, latestV, null, null, "BOTH");
	                            modifyChildrenUri(latestV);
	                            String dmaapMsg = System.nanoTime() + "_" + temp.id().toString() + "_"	+ temp.value("resource-version").toString();
	                            dmaapMsgList.add(dmaapMsg);
	                            rctPserversUpdatedCount++;
	                            logger.info("\tAdding pserver "+latestV.property("hostname").value().toString() + " to updated list");
	                            if (!removeRCTList.contains(list.get(i))) {
	                            	removeRCTList.add(list.get(i));
	                            	Introspector obj = serializer.getLatestVersionView(currV);
	                            	logger.info("\tAdding pserver "+currV.property("hostname").value().toString() + " to delete list");
		                            dmaapDeleteList.add(obj);
		                            rctPserversDeletedCount++;
	                            }
	                            currV = latestV;
	                        } else {
	                            //currv Is the latest, temp vertex found is an older version
	                            latestV = currV;
	                            movePlink(temp, latestV);
	                            moveLagInterfaces(temp, latestV);
	                            swingEdges(temp, latestV, null, null, "BOTH");
	                            modifyChildrenUri(latestV);
	                            String dmaapMsg = System.nanoTime() + "_" + currV.id().toString() + "_"	+ currV.value("resource-version").toString();
	                            dmaapMsgList.add(dmaapMsg);
	                            rctPserversUpdatedCount++;
	                            logger.info("\tAdding pserver "+latestV.property("hostname").value().toString() + " to updated list");
	                            if (!removeRCTList.contains(list.get(j))) {
	                            	removeRCTList.add(list.get(j));
	                            	Introspector obj = serializer.getLatestVersionView(temp);
	                            	logger.info("\tAdding pserver "+temp.property("hostname").value().toString() + " to delete list");
		                            dmaapDeleteList.add(obj);
		                            rctPserversDeletedCount++;
	                            }
	                        }
	
	                    }
                    }
                }
            }
        }
        logger.info("\tCount of RCT Pservers removed = "+removeRCTList.size() +"\n");
        removeRCTList.forEach((r)-> r.remove());

    }


    public void movePlink(Vertex old, Vertex latest) throws AAIException {

        List<Vertex> pInterfacesOnOldPserver = graphTraversalSource().V(old).has("aai-node-type","pserver").in("tosca.relationships.network.BindsTo").has("aai-node-type","p-interface").toList();
        List<Vertex> pInterfacesOnLatestPserver = graphTraversalSource().V(latest).has("aai-node-type","pserver").in("tosca.relationships.network.BindsTo").has("aai-node-type","p-interface").toList();
        //                SCENARIO 1 = no match found move everything from pserver old to new in swing edges call outside this fcn

        if(pInterfacesOnLatestPserver.size() == 0){
        	logger.info("\tNo P-interfaces found on "+latest.property("hostname").value().toString()+ "...");
            if(pInterfacesOnOldPserver.size() != 0) {
            	logger.info("\tP-interfaces found on "+old.property("hostname").value().toString()+ ". Update plink name and move the p-interfaces to latest pserver.");
                for (int i = 0; i < pInterfacesOnOldPserver.size(); i++) {
                    if (graphTraversalSource().V(pInterfacesOnOldPserver.get(i)).has("aai-node-type", "p-interface").out("tosca.relationships.network.LinksTo").hasNext()) {
                        Vertex oldPlink = graphTraversalSource().V(pInterfacesOnOldPserver.get(i)).has("aai-node-type", "p-interface").out("tosca.relationships.network.LinksTo").next();
                        String linkName = oldPlink.property("link-name").value().toString();
                        logger.info("\tPhysical-link "+linkName+ " found on "+graphTraversalSource().V(pInterfacesOnOldPserver.get(i).property("interface-name").value().toString()));
                        linkName = linkName.replaceAll(old.property("hostname").value().toString(), latest.property("hostname").value().toString());
                        String[] PlinkBarSplit = linkName.split("\\|");
                        if (PlinkBarSplit.length > 1) {
                            modifyPlinkName(oldPlink, linkName, old);
                        }
                    }
                }
            }

            return;
        }

        for(int i=0; i<pInterfacesOnOldPserver.size();i++){
            for(int j=0; j<pInterfacesOnLatestPserver.size(); j++){
            	Vertex oldPinterface = graphTraversalSource().V(pInterfacesOnOldPserver.get(i)).has("aai-node-type","p-interface").next();
                //pinterfaces are the same
                if(pInterfacesOnOldPserver.get(i).property("interface-name").value().toString().equals(pInterfacesOnLatestPserver.get(j).property("interface-name").value().toString())){
                    Vertex newPinterface = graphTraversalSource().V(pInterfacesOnLatestPserver.get(j)).has("aai-node-type","p-interface").next();
                    logger.info("\tMatching P-interface "+newPinterface.property("interface-name").value().toString()+ " found on pservers");
//                  SCENARIO 3 there already exists a plink in the new pinterface need to move all other pinterfaces and nodes in swing edges after the fcn no need for plink name change
                    List<Vertex> oldPlinkList = graphTraversalSource().V(pInterfacesOnOldPserver.get(i)).has("aai-node-type","p-interface").out("tosca.relationships.network.LinksTo").toList();
                    if(graphTraversalSource().V(pInterfacesOnLatestPserver.get(j)).has("aai-node-type","p-interface").out("tosca.relationships.network.LinksTo").hasNext()){
                    	logger.info("\tPhysical-link exists on new pserver's p-interface also... So, don't move this p-interface to new pserver...");
                    	if (!oldPlinkList.isEmpty()) {	
                    		//drop edge b/w oldPInterface and oldPlink
                    		String oldPlinkName = ""; 
                    		Edge oldPIntToPlinkEdge = oldPinterface.edges(Direction.OUT, "tosca.relationships.network.LinksTo").next();
                    		oldPIntToPlinkEdge.remove();
	
                    		//remove physical link vertex also
                    		Vertex oldPlink = null;
	                     
	                     	oldPlink = oldPlinkList.get(0);
	                     	oldPlinkName = oldPlink.property("link-name").value().toString();
	                     	oldPlink.remove();
	                     	logger.info("\tDropped edge b/w old P-interface and Physical-link, and deleted old physical-link "+oldPlinkName);
	                     }
                    	moveChildrenOfMatchingPInterfaceToNewPserver(pInterfacesOnOldPserver, i, oldPinterface,	newPinterface);
                    }
//                  SCENARIO 2 = there is no  plink in new  pinterface and move old plink to new
                    else{
                    	logger.info("\tNo Physical-link exists on new pserver's p-interface... Move old plink to new pserver's p-interface");
                        Vertex oldPlink = null;
                        if (!oldPlinkList.isEmpty()) {
                        	oldPlink = oldPlinkList.get(0);
                        	String linkName = oldPlink.property("link-name").value().toString();
	                        createCousinEdge(newPinterface,oldPlink);
	                        logger.info("\tCreated edge b/w new P-interface and old physical-link "+linkName);
	                        //drop edge b/w oldPInterface and oldPlink
	                        Edge oldPIntToPlinkEdge = oldPinterface.edges(Direction.OUT, "tosca.relationships.network.LinksTo").next();
	                        oldPIntToPlinkEdge.remove();
	                        logger.info("\tDropped edge b/w old P-interface and Physical-link "+linkName);
	                        linkName =  linkName.replaceAll(old.property("hostname").value().toString(),latest.property("hostname").value().toString());
	
	                        String[] PlinkBarSplit = linkName.split("\\|");
	                        if(PlinkBarSplit.length>1) {
	                            modifyPlinkName(oldPlink,linkName,old);
	                        }
	                        else{
	                            logger.info("\t" +oldPlink.property("link-name").value().toString()+ " does not comply with naming conventions related to pserver hostname:" + old.property("hostname").value().toString());
	                        }
	                        moveChildrenOfMatchingPInterfaceToNewPserver(pInterfacesOnOldPserver, i, oldPinterface,	newPinterface);
                        } else {
                        	moveChildrenOfMatchingPInterfaceToNewPserver(pInterfacesOnOldPserver, i, oldPinterface,	newPinterface);
                        }
                    }
                  //delete the oldPInterface
                    oldPinterface.remove();
                    break;
                }
            }
        }
    }

	private void moveChildrenOfMatchingPInterfaceToNewPserver(List<Vertex> pInterfacesOnOldPserver, int i, Vertex oldPinterface, Vertex newPinterface) {
		// Check if there are children under old pserver's p-int and move them to new pserver's matching p-int
		List<Vertex> oldPIntChildren = graphTraversalSource().V(pInterfacesOnOldPserver.get(i)).has("aai-node-type","p-interface").in().has("aai-node-type", P.within("l-interface","sriov-pf")).toList();
		if (oldPIntChildren != null && !oldPIntChildren.isEmpty()){
			oldPIntChildren.forEach((c)-> { swingEdges(oldPinterface, newPinterface, null, null, "IN");
//											c.remove();
			});
			logger.info("\t"+"Child vertices of p-interface on old pserver have been moved to p-interface on new pserver");
			
		}
	}

	public void modifyPlinkName(Vertex oldPlink,String linkName,Vertex old ){

        String[] PlinkBarSplit = linkName.split("\\|");
        if(PlinkBarSplit.length>1) {
            String[] pserv1Connection = PlinkBarSplit[0].split(":");
            String[] pserv2Connection = PlinkBarSplit[1].split(":");

            HashMap<String, String> map = new HashMap<>();
            map.put(pserv1Connection[0], pserv1Connection[1]);
            map.put(pserv2Connection[0], pserv2Connection[1]);

            String[] temp = new String[2];
            temp[0] = pserv1Connection[0];
            temp[1] = pserv2Connection[0];
            Arrays.sort(temp);
            String linkNameNew = temp[0] + ":" + map.get(temp[0]).toString() + "|" + temp[1] + ":" + map.get(temp[1]).toString();
            oldPlink.property("link-name", linkNameNew);
            logger.info("\tUpdate physical-link name from "+linkName+ " to "+linkNameNew);
        }
        else{
            logger.info("\t" +oldPlink.property("link-name").value().toString()+ "Does not comply with naming conventions related to pserver hostname:" + old.property("hostname").value().toString());

        }
    }

    public void moveLagInterfaces(Vertex old, Vertex latest) throws AAIException {

        List<Vertex> lagInterfacesOnOldPserver = graphTraversalSource().V(old).has("aai-node-type","pserver").in("tosca.relationships.network.BindsTo").has("aai-node-type","lag-interface").toList();
        List<Vertex> lagInterfacesOnLatestPserver = graphTraversalSource().V(latest).has("aai-node-type","pserver").in("tosca.relationships.network.BindsTo").has("aai-node-type","lag-interface").toList();
        //                SCENARIO 1 = no match found move everything from pserver old to new in swing edges call outside this fcn

        if(lagInterfacesOnLatestPserver.size() == 0){
            return;
        }

        for(int i=0; i<lagInterfacesOnOldPserver.size();i++){

            for(int j=0; j<lagInterfacesOnLatestPserver.size(); j++){
                //lag interface-name matches on both
                if(lagInterfacesOnOldPserver.get(i).property("interface-name").value().toString().equals(lagInterfacesOnLatestPserver.get(j).property("interface-name").value().toString())){
                    Vertex oldLaginterface = graphTraversalSource().V(lagInterfacesOnOldPserver.get(i)).has("aai-node-type","lag-interface").next();
                    Vertex newLaginterface = graphTraversalSource().V(lagInterfacesOnLatestPserver.get(j)).has("aai-node-type","lag-interface").next();
                    //Check if there are any children on the old lag-interface and move them to new
                 // Check if there are children under old pserver's p-int and move them to new pserver's matching p-int
                	List<Vertex> oldPIntChildren = graphTraversalSource().V(lagInterfacesOnOldPserver.get(i)).has("aai-node-type","lag-interface").in().has("aai-node-type", P.within("l-interface")).toList();
                	if (oldPIntChildren != null && !oldPIntChildren.isEmpty()){
                		oldPIntChildren.forEach((c)-> swingEdges(oldLaginterface, newLaginterface, null, null, "BOTH"));
                	}
                	logger.info("\t"+"Child vertices of lag-interface on old pserver have been moved to lag-interface on new pserver");
                    //delete the oldLagInterface
                    oldLaginterface.remove();
                    break;
                }
            }
        }
    }


    private void modifyChildrenUri(Vertex v) throws UnsupportedEncodingException, AAIException {
    	logger.info("\tModifying children uri for all levels.....");
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
                logger.error("\tVertex has issue generating uri " + e.getMessage() + "\n\t" + this.asString(v));
                continue;
            }
            try {
                v.property(AAIProperties.AAI_URI, correctUri);
            } catch (Exception e) {
                logger.info(e.getMessage() + "\n\t" + this.asString(v));
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
        } catch (UnsupportedCharsetException e) {
            return "";
        }
    }

}