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

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.apache.tinkerpop.gremlin.process.traversal.dsl.graph.GraphTraversal;
import org.apache.tinkerpop.gremlin.process.traversal.dsl.graph.GraphTraversalSource;
import org.apache.tinkerpop.gremlin.process.traversal.dsl.graph.__;
import org.apache.tinkerpop.gremlin.structure.*;
import org.janusgraph.core.JanusGraph;
import org.janusgraph.core.JanusGraphFactory;
import org.onap.aai.dbmap.AAIGraph;
import org.onap.aai.dbmap.AAIGraphConfig;
import org.onap.aai.edges.enums.AAIDirection;
import org.onap.aai.edges.enums.EdgeProperty;
import org.onap.aai.exceptions.AAIException;
import org.onap.aai.introspection.Introspector;
import org.onap.aai.introspection.Loader;
import org.onap.aai.introspection.LoaderFactory;
import org.onap.aai.introspection.ModelType;
import org.onap.aai.logging.ErrorLogHelper;
import org.onap.aai.logging.LogFormatTools;
import org.onap.aai.setup.SchemaVersions;
import org.onap.aai.util.AAIConfig;
import org.onap.aai.util.AAIConstants;
import org.onap.aai.util.ExceptionTranslator;
import org.onap.aai.util.GraphAdminConstants;
import org.slf4j.MDC;
import org.springframework.context.annotation.AnnotationConfigApplicationContext;

import java.io.FileInputStream;
import java.io.InputStream;
import java.util.*;
import java.util.Map.Entry;

public class DupeTool {

    private static final Logger logger = LoggerFactory.getLogger(DupeTool.class.getSimpleName());
    private static final String FROMAPPID = "AAI-DB";
    private static final String TRANSID = UUID.randomUUID().toString();

    private static String graphType = "realdb";
    private final SchemaVersions schemaVersions;

    private boolean shouldExitVm = true;

    public void exit(int statusCode) {
        if (this.shouldExitVm) {
            System.exit(1);
        }
    }

    private LoaderFactory loaderFactory;
    private int dupeGroupCount = 0;

    public DupeTool(LoaderFactory loaderFactory, SchemaVersions schemaVersions){
        this(loaderFactory, schemaVersions, true);
    }

    public DupeTool(LoaderFactory loaderFactory, SchemaVersions schemaVersions, boolean shouldExitVm){
        this.loaderFactory = loaderFactory;
        this.schemaVersions = schemaVersions;
        this.shouldExitVm = shouldExitVm;
    }

    public void execute(String[] args){

        String defVersion = "v18";
        try {
            defVersion = AAIConfig.get(AAIConstants.AAI_DEFAULT_API_VERSION_PROP);
        } catch (AAIException ae) {
            String emsg = "Error trying to get default API Version property \n";
            System.out.println(emsg);
            logger.error(emsg);
            exit(0);
        }

        dupeGroupCount = 0;
        Loader loader = null;
        try {
            loader = loaderFactory.createLoaderForVersion(ModelType.MOXY, schemaVersions.getDefaultVersion());
        } catch (Exception ex) {
            logger.error("ERROR - Could not do the moxyMod.init() " + LogFormatTools.getStackTop(ex));
            exit(1);
        }
        JanusGraph graph1 = null;
        JanusGraph graph2 = null;
        Graph gt1 = null;
        Graph gt2 = null;

        boolean specialTenantRule = false;

        try {
            AAIConfig.init();
            int maxRecordsToFix = GraphAdminConstants.AAI_DUPETOOL_DEFAULT_MAX_FIX;
            int sleepMinutes = GraphAdminConstants.AAI_DUPETOOL_DEFAULT_SLEEP_MINUTES;
            int timeWindowMinutes = 0;   // A value of 0 means that we will not have a time-window -- we will look
            // at all nodes of the passed-in nodeType.
            long windowStartTime = 0;  // Translation of the window into a starting timestamp

            try {
                String maxFixStr = AAIConfig.get("aai.dupeTool.default.max.fix");
                if (maxFixStr != null && !maxFixStr.equals("")) {
                    maxRecordsToFix = Integer.parseInt(maxFixStr);
                }
                String sleepStr = AAIConfig.get("aai.dupeTool.default.sleep.minutes");
                if (sleepStr != null && !sleepStr.equals("")) {
                    sleepMinutes = Integer.parseInt(sleepStr);
                }
            } catch (Exception e) {
                // Don't worry, we'll just use the defaults that we got from AAIConstants
                logger.warn("WARNING - could not pick up aai.dupeTool values from aaiconfig.properties file.  Will use defaults. " + e.getMessage());
            }

            String nodeTypeVal = "";
            String userIdVal = "";
            String filterParams = "";
            Boolean skipHostCheck = false;
            Boolean autoFix = false;
            String argStr4Msg = "";
            Introspector obj = null;

            if (args != null && args.length > 0) {
                // They passed some arguments in that will affect processing
                for (int i = 0; i < args.length; i++) {
                    String thisArg = args[i];
                    argStr4Msg = argStr4Msg + " " + thisArg;

                    if (thisArg.equals("-nodeType")) {
                        i++;
                        if (i >= args.length) {
                            logger.error(" No value passed with -nodeType option.  ");
                            exit(0);
                        }
                        nodeTypeVal = args[i];
                        argStr4Msg = argStr4Msg + " " + nodeTypeVal;
                    } else if (thisArg.equals("-sleepMinutes")) {
                        i++;
                        if (i >= args.length) {
                            logger.error("No value passed with -sleepMinutes option.");
                            exit(0);
                        }
                        String nextArg = args[i];
                        try {
                            sleepMinutes = Integer.parseInt(nextArg);
                        } catch (Exception e) {
                            logger.error("Bad value passed with -sleepMinutes option: ["
                                    + nextArg + "]");
                            exit(0);
                        }
                        argStr4Msg = argStr4Msg + " " + sleepMinutes;
                    } else if (thisArg.equals("-maxFix")) {
                        i++;
                        if (i >= args.length) {
                            logger.error("No value passed with -maxFix option.");
                            exit(0);
                        }
                        String nextArg = args[i];
                        try {
                            maxRecordsToFix = Integer.parseInt(nextArg);
                        } catch (Exception e) {
                            logger.error("Bad value passed with -maxFix option: ["
                                    + nextArg + "]");
                            exit(0);
                        }
                        argStr4Msg = argStr4Msg + " " + maxRecordsToFix;
                    } else if (thisArg.equals("-timeWindowMinutes")) {
                        i++;
                        if (i >= args.length) {
                            logger.error("No value passed with -timeWindowMinutes option.");
                            exit(0);
                        }
                        String nextArg = args[i];
                        try {
                            timeWindowMinutes = Integer.parseInt(nextArg);
                        } catch (Exception e) {
                            logger.error("Bad value passed with -timeWindowMinutes option: ["
                                    + nextArg + "]");
                            exit(0);
                        }
                        argStr4Msg = argStr4Msg + " " + timeWindowMinutes;
                    } else if (thisArg.equals("-skipHostCheck")) {
                        skipHostCheck = true;
                    } else if (thisArg.equals("-specialTenantRule")) {
                        specialTenantRule = true;
                    } else if (thisArg.equals("-autoFix")) {
                        autoFix = true;
                    } else if (thisArg.equals("-userId")) {
                        i++;
                        if (i >= args.length) {
                            logger.error(" No value passed with -userId option.  ");
                            exit(0);
                        }
                        userIdVal = args[i];
                        argStr4Msg = argStr4Msg + " " + userIdVal;
                    } else if (thisArg.equals("-params4Collect")) {
                        i++;
                        if (i >= args.length) {
                            logger.error(" No value passed with -params4Collect option.  ");
                            exit(0);
                        }
                        filterParams = args[i];
                        argStr4Msg = argStr4Msg + " " + filterParams;
                    } else {
                        logger.error(" Unrecognized argument passed to DupeTool: ["
                                + thisArg + "]. ");
                        logger.error(" Valid values are: -action -userId -vertexId -edgeId -overRideProtection ");
                        exit(0);
                    }
                }
            }

            userIdVal = userIdVal.trim();
            if ((userIdVal.length() < 6) || userIdVal.toUpperCase().equals("AAIADMIN")) {
                String emsg = "userId parameter is required.  [" + userIdVal + "] passed to DupeTool(). userId must be not empty and not aaiadmin \n";
                System.out.println(emsg);
                logger.error(emsg);
                exit(0);
            }

            nodeTypeVal = nodeTypeVal.trim();
            if (nodeTypeVal.equals("")) {
                String emsg = " nodeType is a required parameter for DupeTool().\n";
                System.out.println(emsg);
                logger.error(emsg);
                exit(0);
            } else {
                obj = loader.introspectorFromName(nodeTypeVal);
            }

            if (timeWindowMinutes > 0) {
                // Translate the window value (ie. 30 minutes) into a unix timestamp like
                //    we use in the db - so we can select data created after that time.
                windowStartTime = figureWindowStartTime(timeWindowMinutes);
            }

            String msg = "";
            msg = "DupeTool called with these params: [" + argStr4Msg + "]";
            System.out.println(msg);
            logger.debug(msg);

            // Determine what the key fields are for this nodeType (and we want them ordered)
            ArrayList<String> keyPropNamesArr = new ArrayList<>(obj.getKeys());

            // Determine what kinds of nodes (if any) this nodeType is dependent on for uniqueness
            ArrayList<String> depNodeTypeList = new ArrayList<>();
            Collection<String> depNTColl = obj.getDependentOn();
            Iterator<String> ntItr = depNTColl.iterator();
            while (ntItr.hasNext()) {
                depNodeTypeList.add(ntItr.next());
            }

            // Based on the nodeType, window and filterData, figure out the vertices that we will be checking
            System.out.println("    ---- NOTE --- about to open graph (takes a little while)--------\n");
            graph1 = setupGraph(logger);
            gt1 = getGraphTransaction(graph1, logger);
            ArrayList<Vertex> verts2Check = new ArrayList<>();
            try {
                verts2Check = figureOutNodes2Check(TRANSID, FROMAPPID, gt1,
                        nodeTypeVal, windowStartTime, filterParams, logger);
            } catch (AAIException ae) {
                String emsg = "Error trying to get initial set of nodes to check. \n";
                System.out.println(emsg);
                logger.error(emsg);
                exit(0);
            }

            if (verts2Check == null || verts2Check.size() == 0) {
                msg = " No vertices found to check.  Used nodeType = [" + nodeTypeVal
                        + "], windowMinutes = " + timeWindowMinutes
                        + ", filterData = [" + filterParams + "].";
                logger.debug(msg);
                System.out.println(msg);
                exit(0);
            } else {
                msg = " Found " + verts2Check.size() + " nodes of type " + nodeTypeVal
                        + " to check using passed filterParams and windowStartTime. ";
                logger.debug(msg);
                System.out.println(msg);
            }

            ArrayList<String> firstPassDupeSets = new ArrayList<>();
            ArrayList<String> secondPassDupeSets = new ArrayList<>();
            Boolean isDependentOnParent = false;
            if (!obj.getDependentOn().isEmpty()) {
                isDependentOnParent = true;
            }

            if (isDependentOnParent) {
                firstPassDupeSets = getDupeSets4DependentNodes(TRANSID, FROMAPPID, gt1,
                        defVersion, nodeTypeVal, verts2Check, keyPropNamesArr, loader,
                        specialTenantRule, logger);
            } else {
                firstPassDupeSets = getDupeSets4NonDepNodes(TRANSID, FROMAPPID, gt1,
                        defVersion, nodeTypeVal, verts2Check, keyPropNamesArr,
                        specialTenantRule, loader, logger);
            }

            msg = " Found " + firstPassDupeSets.size() + " sets of duplicates for this request. ";
            logger.debug(msg);
            System.out.println(msg);
            if (firstPassDupeSets.size() > 0) {
                msg = " Here is what they look like: ";
                logger.debug(msg);
                System.out.println(msg);
                for (int x = 0; x < firstPassDupeSets.size(); x++) {
                    msg = " Set " + x + ": [" + firstPassDupeSets.get(x) + "] ";
                    logger.debug(msg);
                    System.out.println(msg);
                    showNodeDetailsForADupeSet(gt1, firstPassDupeSets.get(x), logger);
                }
            }
            dupeGroupCount = firstPassDupeSets.size();
            boolean didSomeDeletesFlag = false;
            ArrayList<String> dupeSetsToFix = new ArrayList<>();
            if (autoFix && firstPassDupeSets.size() == 0) {
                msg = "AutoFix option is on, but no dupes were found on the first pass.  Nothing to fix.";
                logger.debug(msg);
                System.out.println(msg);
            } else if (autoFix) {
                // We will try to fix any dupes that we can - but only after sleeping for a
                // time and re-checking the list of duplicates using a seperate transaction.
                try {
                    msg = "\n\n-----------  About to sleep for " + sleepMinutes + " minutes."
                            + "  -----------\n\n";
                    logger.debug(msg);
                    System.out.println(msg);
                    int sleepMsec = sleepMinutes * 60 * 1000;
                    Thread.sleep(sleepMsec);
                } catch (InterruptedException ie) {
                    msg = "\n >>> Sleep Thread has been Interrupted <<< ";
                    logger.debug(msg);
                    System.out.println(msg);
                    exit(0);
                }

                graph2 = setupGraph(logger);
                gt2 = getGraphTransaction(graph2, logger);
                if (isDependentOnParent) {
                    secondPassDupeSets = getDupeSets4DependentNodes(TRANSID, FROMAPPID, gt2,
                            defVersion, nodeTypeVal, verts2Check, keyPropNamesArr, loader,
                            specialTenantRule, logger);
                } else {
                    secondPassDupeSets = getDupeSets4NonDepNodes(TRANSID, FROMAPPID, gt2,
                            defVersion, nodeTypeVal, verts2Check, keyPropNamesArr,
                            specialTenantRule, loader, logger);
                }

                dupeSetsToFix = figureWhichDupesStillNeedFixing(firstPassDupeSets, secondPassDupeSets, logger);
                msg = "\nAfter running a second pass, there were " + dupeSetsToFix.size()
                        + " sets of duplicates that we think can be deleted. ";
                logger.debug(msg);
                System.out.println(msg);

                if (dupeSetsToFix.size() > 0) {
                    msg = " Here is what the sets look like: ";
                    logger.debug(msg);
                    System.out.println(msg);
                    for (int x = 0; x < dupeSetsToFix.size(); x++) {
                        msg = " Set " + x + ": [" + dupeSetsToFix.get(x) + "] ";
                        logger.debug(msg);
                        System.out.println(msg);
                        showNodeDetailsForADupeSet(gt2, dupeSetsToFix.get(x), logger);
                    }
                }

                if (dupeSetsToFix.size() > 0) {
                    if (dupeSetsToFix.size() > maxRecordsToFix) {
                        String infMsg = " >> WARNING >>  Dupe list size ("
                                + dupeSetsToFix.size()
                                + ") is too big.  The maxFix we are using is: "
                                + maxRecordsToFix
                                + ".  No nodes will be deleted. (use the"
                                + " -maxFix option to override this limit.)";
                        System.out.println(infMsg);
                       logger.debug(infMsg);
                    } else {
                        // Call the routine that fixes known dupes
                        didSomeDeletesFlag = deleteNonKeepers(gt2, dupeSetsToFix, logger);
                    }
                }
                if (didSomeDeletesFlag) {
                    gt2.tx().commit();
                }
            }

        } catch (AAIException e) {
            logger.error("Caught AAIException while running the dupeTool: " + LogFormatTools.getStackTop(e));
            ErrorLogHelper.logException(e);
        } catch (Exception ex) {
            logger.error("Caught exception while running the dupeTool: " + LogFormatTools.getStackTop(ex));
            ErrorLogHelper.logError("AAI_6128", ex.getMessage() + ", resolve and rerun the dupeTool. ");
        } finally {
            if (gt1 != null && gt1.tx().isOpen()) {
                // We don't change any data with gt1 - so just roll it back so it knows we're done.
                try {
                    gt1.tx().rollback();
                } catch (Exception ex) {
                    // Don't throw anything because JanusGraph sometimes is just saying that the graph is already closed
                    logger.warn("WARNING from final gt1.rollback() " + LogFormatTools.getStackTop(ex));
                }
            }

            if (gt2 != null && gt2.tx().isOpen()) {
                // Any changes that worked correctly should have already done
                // their commits.
                try {
                    gt2.tx().rollback();
                } catch (Exception ex) {
                    // Don't throw anything because JanusGraph sometimes is just saying that the graph is already closed
                    logger.warn("WARNING from final gt2.rollback() " + LogFormatTools.getStackTop(ex));
                }
            }

            try {
                if (graph1 != null && graph1.isOpen()) {
                    closeGraph(graph1, logger);
                }
            } catch (Exception ex) {
                // Don't throw anything because JanusGraph sometimes is just saying that the graph is already closed{
                logger.warn("WARNING from final graph1.shutdown() " + LogFormatTools.getStackTop(ex));
            }

            try {
                if (graph2 != null && graph2.isOpen()) {
                    closeGraph(graph2, logger);
                }
            } catch (Exception ex) {
                // Don't throw anything because JanusGraph sometimes is just saying that the graph is already closed{
                logger.warn("WARNING from final graph2.shutdown() " + LogFormatTools.getStackTop(ex));
            }
        }

        exit(0);
    }

    /**
     * The main method.
     *
     * @param args the arguments
     */
    public static void main(String[] args) throws AAIException {

        System.setProperty("aai.service.name", DupeTool.class.getSimpleName());
        MDC.put("logFilenameAppender", DupeTool.class.getSimpleName());


        AnnotationConfigApplicationContext ctx = new AnnotationConfigApplicationContext();
        try {
            ctx.scan(
                    "org.onap.aai"
            );
            ctx.refresh();
        } catch (Exception e) {
            AAIException aai = ExceptionTranslator.schemaServiceExceptionTranslator(e);
            logger.error("Problems running DupeTool "+aai.getMessage());
            ErrorLogHelper.logError(aai.getCode(), e.getMessage() + ", resolve and retry");
            throw aai;
        }
        LoaderFactory loaderFactory = ctx.getBean(LoaderFactory.class);
        SchemaVersions schemaVersions = (SchemaVersions) ctx.getBean("schemaVersions");
        DupeTool dupeTool = new DupeTool(loaderFactory, schemaVersions);
        dupeTool.execute(args);
    }// end of main()


    /**
     * Collect Duplicate Sets for nodes that are NOT dependent on parent nodes.
     *
     * @param transId        the trans id
     * @param fromAppId      the from app id
     * @param g              the g
     * @param version        the version
     * @param nType          the n type
     * @param passedVertList the passed vert list
     * @param loader         the loader
     * @param logger         the logger
     * @return the array list
     */
    private ArrayList<String> getDupeSets4NonDepNodes(String transId,
                                                             String fromAppId, Graph g, String version, String nType,
                                                             ArrayList<Vertex> passedVertList,
                                                             ArrayList<String> keyPropNamesArr,
                                                             Boolean specialTenantRule, Loader loader, Logger logger) {

        ArrayList<String> returnList = new ArrayList<>();

        // We've been passed a set of nodes that we want to check.
        // They are all NON-DEPENDENT nodes meaning that they should be
        // unique in the DB based on their KEY DATA alone.  So, if
        // we group them by their key data - if any key has more than one
        // vertex mapped to it, those vertices are dupes.
        //
        // When we find duplicates, we return then as a String (there can be
        //     more than one duplicate for one set of key data):
        // Each element in the returned arrayList might look like this:
        // "1234|5678|keepVid=UNDETERMINED" (if there were 2 dupes, and we
        // couldn't figure out which one to keep)
        // or, "100017|200027|30037|keepVid=30037" (if there were 3 dupes and we
        // thought the third one was the one that should survive)

        HashMap<String, ArrayList<String>> keyVals2VidHash = new HashMap<>();
        HashMap<String, Vertex> vtxHash = new HashMap<>();
        Iterator<Vertex> pItr = passedVertList.iterator();
        while (pItr.hasNext()) {
            try {
                Vertex tvx = pItr.next();
                String thisVid = tvx.id().toString();
                vtxHash.put(thisVid, tvx);

                // if there are more than one vertexId mapping to the same keyProps -- they are dupes
                String hKey = getNodeKeyValString(tvx, keyPropNamesArr, logger);
                if (keyVals2VidHash.containsKey(hKey)) {
                    // We've already seen this key
                    ArrayList<String> tmpVL = keyVals2VidHash.get(hKey);
                    tmpVL.add(thisVid);
                    keyVals2VidHash.put(hKey, tmpVL);
                } else {
                    // First time for this key
                    ArrayList<String> tmpVL = new ArrayList<>();
                    tmpVL.add(thisVid);
                    keyVals2VidHash.put(hKey, tmpVL);
                }
            } catch (Exception e) {
                logger.warn(" >>> Threw an error in getDupeSets4NonDepNodes - just absorb this error and move on. " + LogFormatTools.getStackTop(e));
            }
        }

        for (Map.Entry<String, ArrayList<String>> entry : keyVals2VidHash.entrySet()) {
            ArrayList<String> vidList = entry.getValue();
            try {
                if (!vidList.isEmpty() && vidList.size() > 1) {
                    // There are more than one vertex id's using the same key info
                    StringBuilder dupesStr = new StringBuilder();
                    ArrayList<Vertex> vertList = new ArrayList<>();
                    for (String tmpVid : vidList) {
                        dupesStr.append(tmpVid).append("|");
                        vertList.add(vtxHash.get(tmpVid));
                    }

                    if (dupesStr.length() > 0) {
                        Vertex prefV = getPreferredDupe(transId, fromAppId,
                                g, vertList, version, specialTenantRule, loader, logger);
                        if (prefV == null) {
                            // We could not determine which duplicate to keep
                            dupesStr.append("KeepVid=UNDETERMINED");
                            returnList.add(dupesStr.toString());
                        } else {
                            dupesStr.append("KeepVid=").append(prefV.id());
                            returnList.add(dupesStr.toString());
                        }
                    }
                }
            } catch (Exception e) {
                logger.warn(" >>> Threw an error in getDupeSets4NonDepNodes - just absorb this error and move on. " + LogFormatTools.getStackTop(e));
            }

        }
        return returnList;

    }// End of getDupeSets4NonDepNodes()


    /**
     * Collect Duplicate Sets for nodes that are dependent on parent nodes.
     *
     * @param transId           the trans id
     * @param fromAppId         the from app id
     * @param g                 the g
     * @param version           the version
     * @param nType             the n type
     * @param passedVertList    the passed vert list
     * @param keyPropNamesArr   Array (ordered) of keyProperty names
     * @param specialTenantRule flag
     * @param logger        the logger
     * @return the array list
     */
    private ArrayList<String> getDupeSets4DependentNodes(String transId,
                                                                String fromAppId, Graph g, String version, String nType,
                                                                ArrayList<Vertex> passedVertList,
                                                                ArrayList<String> keyPropNamesArr, Loader loader,
                                                                Boolean specialTenantRule, Logger logger) {

        // This is for nodeTypes that DEPEND ON A PARENT NODE FOR UNIQUNESS

        ArrayList<String> returnList = new ArrayList<>();
        ArrayList<String> alreadyFoundDupeVidArr = new ArrayList<>();

        // We've been passed a set of nodes that we want to check.  These are
        // all nodes that ARE DEPENDENT on a PARENT Node for uniqueness.
        // The first thing to do is to identify the key properties for the node-type
        // and pull from the db just using those properties.
        // Then, we'll check those nodes with their parent nodes to see if there
        // are any duplicates.
        //
        // When we find duplicates, we return then as a String (there can be
        //     more than one duplicate for one set of key data):
        // Each element in the returned arrayList might look like this:
        // "1234|5678|keepVid=UNDETERMINED" (if there were 2 dupes, and we
        // couldn't figure out which one to keep)
        // or, "100017|200027|30037|keepVid=30037" (if there were 3 dupes and we
        // thought the third one was the one that should survive)
        HashMap<String, Object> checkVertHash = new HashMap<>();
        try {
            Iterator<Vertex> pItr = passedVertList.iterator();
            while (pItr.hasNext()) {
                Vertex tvx = pItr.next();
                String passedId = tvx.id().toString();
                if (!alreadyFoundDupeVidArr.contains(passedId)) {
                    // We haven't seen this one before - so we should check it.
                    HashMap<String, Object> keyPropValsHash = getNodeKeyVals(tvx, keyPropNamesArr, logger);
                    ArrayList<Vertex> tmpVertList = getNodeJustUsingKeyParams(transId, fromAppId, g,
                            nType, keyPropValsHash, version, logger);

                    if (tmpVertList.size() <= 1) {
                        // Even without a parent node, this thing is unique so don't worry about it.
                    } else {
                        for (int i = 0; i < tmpVertList.size(); i++) {
                            Vertex tmpVtx = (tmpVertList.get(i));
                            String tmpVid = tmpVtx.id().toString();
                            alreadyFoundDupeVidArr.add(tmpVid);

                            String hKey = getNodeKeyValString(tmpVtx, keyPropNamesArr, logger);
                            if (checkVertHash.containsKey(hKey)) {
                                // add it to an existing list
                                ArrayList<Vertex> tmpVL = (ArrayList<Vertex>) checkVertHash.get(hKey);
                                tmpVL.add(tmpVtx);
                                checkVertHash.put(hKey, tmpVL);
                            } else {
                                // First time for this key
                                ArrayList<Vertex> tmpVL = new ArrayList<Vertex>();
                                tmpVL.add(tmpVtx);
                                checkVertHash.put(hKey, tmpVL);
                            }
                        }
                    }
                }
            }

            // More than one node have the same key fields since they may
            // depend on a parent node for uniqueness. Since we're finding
            // more than one, we want to check to see if any of the
            // vertices that have this set of keys are also pointing at the
            // same 'parent' node.
            // Note: for a given set of key data, it is possible that there
            // could be more than one set of duplicates.
            for (Entry<String, Object> lentry : checkVertHash.entrySet()) {
                ArrayList<Vertex> thisIdSetList = (ArrayList<Vertex>) lentry.getValue();
                if (thisIdSetList == null || thisIdSetList.size() < 2) {
                    // Nothing to check for this set.
                    continue;
                }

                HashMap<String, ArrayList<Vertex>> vertsGroupedByParentHash = groupVertsByDepNodes(
                        transId, fromAppId, g, version, nType,
                        thisIdSetList, loader);
                for (Map.Entry<String, ArrayList<Vertex>> entry : vertsGroupedByParentHash
                        .entrySet()) {
                    ArrayList<Vertex> thisParentsVertList = entry
                            .getValue();
                    if (thisParentsVertList.size() > 1) {
                        // More than one vertex found with the same key info
                        // hanging off the same parent/dependent node
                        StringBuilder dupesStr = new StringBuilder();
                        for (Vertex vertex : thisParentsVertList) {
                            dupesStr.append(vertex.id()).append("|");
                        }
                        if (dupesStr.toString().length() > 0) {
                            Vertex prefV = getPreferredDupe(transId,
                                    fromAppId, g, thisParentsVertList,
                                    version, specialTenantRule, loader, logger);

                            if (prefV == null) {
                                // We could not determine which duplicate to keep
                                dupesStr.append("KeepVid=UNDETERMINED");
                                returnList.add(dupesStr.toString());
                            } else {
                                dupesStr.append("KeepVid=").append(prefV.id().toString());
                                returnList.add(dupesStr.toString());
                            }
                        }
                    }
                }
            }

        } catch (Exception e) {
            logger.warn(" >>> Threw an error in checkAndProcessDupes - just absorb this error and move on. " + LogFormatTools.getStackTop(e));
        }

        return returnList;

    }// End of getDupeSets4DependentNodes()


    private Graph getGraphTransaction(JanusGraph graph, Logger logger) {

        Graph gt = null;
        try {
            if (graph == null) {
                String emsg = "could not get graph object in DupeTool.  \n";
                System.out.println(emsg);
                logger.error(emsg);
                exit(0);
            }
            gt = graph.newTransaction();
            if (gt == null) {
                String emsg = "null graphTransaction object in DupeTool. \n";
                throw new AAIException("AAI_6101", emsg);
            }

        } catch (AAIException e1) {
            String msg = e1.getErrorObject().toString();
            System.out.println(msg);
            logger.error(msg);
            exit(0);
        } catch (Exception e2) {
            String msg = e2.toString();
            System.out.println(msg);
            logger.error(msg);
            exit(0);
        }

        return gt;

    }// End of getGraphTransaction()


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


    public ArrayList<String> collectEdgeInfoForNode(Logger logger, Vertex tVert, boolean displayAllVidsFlag) {
        ArrayList<String> retArr = new ArrayList<>();
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
                    retArr.add(" >>> COULD NOT FIND VERTEX on the other side of this edge edgeId = %s <<< ".formatted(ed.id()));
                } else {
                    String nType = vtx.<String>property("aai-node-type").orElse(null);
                    if (displayAllVidsFlag) {
                        // This should rarely be needed
                        String vid = vtx.id().toString();
                        retArr.add("Found an " + dir + " edge (" + lab + ") between this vertex and a [" + nType + "] node with VtxId = " + vid);
                    } else {
                        // This is the normal case
                        retArr.add("Found an " + dir + " edge (" + lab + ") between this vertex and a [" + nType + "] node. ");
                    }
                }
            }
        }
        return retArr;

    }// end of collectEdgeInfoForNode()


    private long figureWindowStartTime(int timeWindowMinutes) {
        // Given a window size, calculate what the start-timestamp would be.

        if (timeWindowMinutes <= 0) {
            // This just means that there is no window...
            return 0;
        }
        long unixTimeNow = System.currentTimeMillis();
        long windowInMillis = timeWindowMinutes * 60 * 1000;

        long startTimeStamp = unixTimeNow - windowInMillis;

        return startTimeStamp;
    } // End of figureWindowStartTime()


    /**
     * Gets the node(s) just using key params.
     *
     * @param transId      the trans id
     * @param fromAppId    the from app id
     * @param graph        the graph
     * @param nodeType     the node type
     * @param keyPropsHash the key props hash
     * @param apiVersion   the api version
     * @return the node just using key params
     * @throws AAIException the AAI exception
     */
    public ArrayList<Vertex> getNodeJustUsingKeyParams(String transId, String fromAppId, Graph graph, String nodeType,
                                                              HashMap<String, Object> keyPropsHash, String apiVersion, Logger logger) throws AAIException {

        ArrayList<Vertex> retVertList = new ArrayList<>();

        // We assume that all NodeTypes have at least one key-property defined.
        // Note - instead of key-properties (the primary key properties), a user could pass
        //        alternate-key values if they are defined for the nodeType.
        ArrayList<String> kName = new ArrayList<>();
        ArrayList<Object> kVal = new ArrayList<>();
        if (keyPropsHash == null || keyPropsHash.isEmpty()) {
            throw new AAIException("AAI_6120", " NO key properties passed for this getNodeJustUsingKeyParams() request.  NodeType = [" + nodeType + "]. ");
        }

        int i = -1;
        for (Map.Entry<String, Object> entry : keyPropsHash.entrySet()) {
            i++;
            kName.add(i, entry.getKey());
            kVal.add(i, entry.getValue());
        }
        int topPropIndex = i;
        Vertex tiV = null;
        String propsAndValuesForMsg = "";
        Iterator<Vertex> verts = null;
        GraphTraversalSource g = graph.traversal();
        try {
            if (topPropIndex == 0) {
                propsAndValuesForMsg = " (" + kName.get(0) + " = " + kVal.get(0) + ") ";
                verts = g.V().has(kName.get(0), kVal.get(0)).has("aai-node-type", nodeType);
            } else if (topPropIndex == 1) {
                propsAndValuesForMsg = " (" + kName.get(0) + " = " + kVal.get(0) + ", "
                        + kName.get(1) + " = " + kVal.get(1) + ") ";
                verts = g.V().has(kName.get(0), kVal.get(0)).has(kName.get(1), kVal.get(1)).has("aai-node-type", nodeType);
            } else if (topPropIndex == 2) {
                propsAndValuesForMsg = " (" + kName.get(0) + " = " + kVal.get(0) + ", "
                        + kName.get(1) + " = " + kVal.get(1) + ", "
                        + kName.get(2) + " = " + kVal.get(2) + ") ";
                verts = g.V().has(kName.get(0), kVal.get(0)).has(kName.get(1), kVal.get(1)).has(kName.get(2), kVal.get(2)).has("aai-node-type", nodeType);
            } else if (topPropIndex == 3) {
                propsAndValuesForMsg = " (" + kName.get(0) + " = " + kVal.get(0) + ", "
                        + kName.get(1) + " = " + kVal.get(1) + ", "
                        + kName.get(2) + " = " + kVal.get(2) + ", "
                        + kName.get(3) + " = " + kVal.get(3) + ") ";
                verts = g.V().has(kName.get(0), kVal.get(0)).has(kName.get(1), kVal.get(1)).has(kName.get(2), kVal.get(2)).has(kName.get(3), kVal.get(3)).has("aai-node-type", nodeType);
            } else {
                throw new AAIException("AAI_6114", " We only support 4 keys per nodeType for now \n");
            }
        } catch (Exception ex) {
            logger.error(" ERROR trying to get node for: [" + propsAndValuesForMsg + "] " + LogFormatTools.getStackTop(ex));
        }

        if (verts != null) {
            while (verts.hasNext()) {
                tiV = verts.next();
                retVertList.add(tiV);
            }
        }

        if (retVertList.size() == 0) {
            logger.debug("DEBUG No node found for nodeType = [%s], propsAndVal = %s".formatted(nodeType, propsAndValuesForMsg));
        }

        return retVertList;

    }// End of getNodeJustUsingKeyParams()


    /**
     * Gets the node(s) just using key params.
     *
     * @param transId         the trans id
     * @param fromAppId       the from app id
     * @param graph           the graph
     * @param nodeType        the node type
     * @param windowStartTime the window start time
     * @param propsString     the props hash
     * @param logger          the logger
     * @return the nodes
     * @throws AAIException the AAI exception
     */
    public ArrayList<Vertex> figureOutNodes2Check(String transId, String fromAppId,
                                                         Graph graph, String nodeType, long windowStartTime,
                                                         String propsString, Logger logger) throws AAIException {

        ArrayList<Vertex> retVertList = new ArrayList<>();
        String msg = "";
        GraphTraversal<Vertex, Vertex> tgQ = graph.traversal().V().has("aai-node-type", nodeType);
        String qStringForMsg = "graph.traversal().V().has(\"aai-node-type\"," + nodeType + ")";

        if (propsString != null && !propsString.trim().equals("")) {
            propsString = propsString.trim();
            int firstPipeLoc = propsString.indexOf("|");
            if (firstPipeLoc <= 0) {
                msg = "Bad props4Collect passed: [" + propsString + "].  \n Expecting a format like, 'propName1|propVal1,propName2|propVal2'";
                System.out.println(msg);
                logger.error(msg);
                exit(0);
            }

            // Note - if they're only passing on parameter, there won't be any commas
            String[] paramArr = propsString.split(",");
            for (int i = 0; i < paramArr.length; i++) {
                int pipeLoc = paramArr[i].indexOf("|");
                if (pipeLoc <= 0) {
                    msg = "Bad propsString passed: [" + propsString + "].  \n Expecting a format like, 'propName1|propVal1,propName2|propVal2'";
                    System.out.println(msg);
                    logger.error(msg);
                    exit(0);
                } else {
                    String propName = paramArr[i].substring(0, pipeLoc);
                    String propVal = paramArr[i].substring(pipeLoc + 1);
                    tgQ = tgQ.has(propName, propVal);
                    qStringForMsg = qStringForMsg + ".has(" + propName + "," + propVal + ")";
                }
            }
        }

        if (tgQ == null) {
            msg = "Bad JanusGraphQuery object.  ";
            System.out.println(msg);
            logger.error(msg);
            exit(0);
        } else {
            Iterator<Vertex> vertItor = tgQ;
            while (vertItor.hasNext()) {
                Vertex tiV = vertItor.next();
                if (windowStartTime <= 0) {
                    // We're not applying a time-window
                    retVertList.add(tiV);
                } else {
                    Object objTimeStamp = tiV.property("aai-created-ts").orElse(null);
                    if (objTimeStamp == null) {
                        // No timestamp - so just take it
                        retVertList.add(tiV);
                    } else {
                        long thisNodeCreateTime = (long) objTimeStamp;
                        if (thisNodeCreateTime > windowStartTime) {
                            // It is in our window, so we can take it
                            retVertList.add(tiV);
                        }
                    }
                }
            }
        }

        if (retVertList.size() == 0) {
            logger.debug("DEBUG No node found for: [%s, with aai-created-ts > %d".formatted(qStringForMsg, windowStartTime));
        }

        return retVertList;

    }// End of figureOutNodes2Check()


    /**
     * Gets the preferred dupe.
     *
     * @param transId        the trans id
     * @param fromAppId      the from app id
     * @param g              the g
     * @param dupeVertexList the dupe vertex list
     * @param ver            the ver
     * @param loader         the loader
     * @param logger         the logger
     * @return Vertex
     * @throws AAIException the AAI exception
     */
    public Vertex getPreferredDupe(String transId,
                                          String fromAppId, Graph g,
                                          ArrayList<Vertex> dupeVertexList, String ver,
                                          Boolean specialTenantRule, Loader loader, Logger logger)
            throws AAIException {

		// This method assumes that it is being passed a List of
		// vertex objects which violate our uniqueness constraints.
		// Note - returning a null vertex means we could not
		//   safely pick one to keep (Ie. safely know which to delete.)
        Vertex nullVtx = null;
        GraphTraversalSource gts = g.traversal();

        if (dupeVertexList == null) {
            return nullVtx;
        }
        int listSize = dupeVertexList.size();
        if (listSize == 0) {
            return nullVtx;
        }
        if (listSize == 1) {
            return (dupeVertexList.get(0));
        }

		// If they don't all have the same aai-uri, then we will not
		// choose between them - we'll need someone to manually
		// check to pick which one makes sense to keep.
		Object uriOb = dupeVertexList.get(0).<Object>property("aai-uri").orElse(null);
		if( uriOb == null || uriOb.toString().equals("") ){
			// this is a bad node - hopefully will be picked up by phantom checker
			return nullVtx;
		}
		String thisUri = uriOb.toString();
		for (int i = 1; i < listSize; i++) {
			uriOb = dupeVertexList.get(i).<Object>property("aai-uri").orElse(null);
			if( uriOb == null || uriOb.toString().equals("") ){
				// this is a bad node - hopefully will be picked up by phantom checker
				return nullVtx;
			}
			String nextUri = uriOb.toString();
			if( !thisUri.equals(nextUri)){
				// there are different URI's on these - so we can't pick
				// a dupe to keep.  Someone will need to look at it.
				return nullVtx;
			}
		}

        Vertex vtxPreferred = null;
        Vertex currentFaveVtx = dupeVertexList.get(0);
        for (int i = 1; i < listSize; i++) {
            Vertex vtxB = dupeVertexList.get(i);
            vtxPreferred = pickOneOfTwoDupes(transId, fromAppId, gts,
                    currentFaveVtx, vtxB, ver, specialTenantRule, loader, logger);
            if (vtxPreferred == null) {
                // We couldn't choose one
                return nullVtx;
            } else {
                currentFaveVtx = vtxPreferred;
            }
        }

        if( currentFaveVtx != null && checkAaiUriOk(gts, currentFaveVtx, logger) ){
			return (currentFaveVtx);
		}
		else {
			// We had a preferred vertex, but its aai-uri was bad, so
			// we will not recommend one to keep.
			return nullVtx;
		}

    } // end of getPreferredDupe()


    /**
     * Pick one of two dupes.
     *
     * @param transId               the trans id
     * @param fromAppId             the from app id
     * @param gts                   the graphTraversalSource
     * @param vtxA                  the vtx A
     * @param vtxB                  the vtx B
     * @param ver                   the ver
     * @param specialTenantRule     specialTenantRuleFlag flag
     * @param loader                the loader
     * @param logger                the logger
     * @return Vertex
     * @throws AAIException the AAI exception
     */
    public Vertex pickOneOfTwoDupes(String transId,
                                           String fromAppId, GraphTraversalSource gts, Vertex vtxA,
                                           Vertex vtxB, String ver, Boolean specialTenantRule, Loader loader, Logger logger) throws AAIException {

        Vertex nullVtx = null;
        Vertex preferredVtx = null;

        Long vidA = Long.valueOf(vtxA.id().toString());
        Long vidB = Long.valueOf(vtxB.id().toString());

        String vtxANodeType = "";
        String vtxBNodeType = "";
        Object obj = vtxA.<Object>property("aai-node-type").orElse(null);
        if (obj != null) {
            vtxANodeType = obj.toString();
        }
        obj = vtxB.<Object>property("aai-node-type").orElse(null);
        if (obj != null) {
            vtxBNodeType = obj.toString();
        }

        if (vtxANodeType.equals("") || (!vtxANodeType.equals(vtxBNodeType))) {
            // Either they're not really dupes or there's some bad data - so
            // don't pick one
            return nullVtx;
        }

        // Check that node A and B both have the same key values (or else they
        // are not dupes)
        // (We'll check dep-node later)
        Collection<String> keyProps = loader.introspectorFromName(vtxANodeType).getKeys();
        Iterator<String> keyPropI = keyProps.iterator();
        while (keyPropI.hasNext()) {
            String propName = keyPropI.next();
            String vtxAKeyPropVal = "";
            obj = vtxA.<Object>property(propName).orElse(null);
            if (obj != null) {
                vtxAKeyPropVal = obj.toString();
            }
            String vtxBKeyPropVal = "";
            obj = vtxB.<Object>property(propName).orElse(null);
            if (obj != null) {
                vtxBKeyPropVal = obj.toString();
            }

            if (vtxAKeyPropVal.equals("")
                    || (!vtxAKeyPropVal.equals(vtxBKeyPropVal))) {
                // Either they're not really dupes or they are missing some key
                // data - so don't pick one
                return nullVtx;
            }
        }

        // Collect the vid's and aai-node-types of the vertices that each vertex
        // (A and B) is connected to.
        ArrayList<String> vtxIdsConn2A = new ArrayList<>();
        ArrayList<String> vtxIdsConn2B = new ArrayList<>();
        HashMap<String, String> nodeTypesConn2A = new HashMap<>();
        HashMap<String, String> nodeTypesConn2B = new HashMap<>();

        ArrayList<String> retArr = new ArrayList<>();
        Iterator<Edge> eAI = vtxA.edges(Direction.BOTH);
        while (eAI.hasNext()) {
            Edge ed = eAI.next();
            Vertex tmpVtx;
            if (vtxA.equals(ed.inVertex())) {
                tmpVtx = ed.outVertex();
            } else {
                tmpVtx = ed.inVertex();
            }
            if (tmpVtx == null) {
                retArr.add(" >>> COULD NOT FIND VERTEX on the other side of this edge edgeId = " + ed.id() + " <<< ");
            } else {
                String conVid = tmpVtx.id().toString();
                String nt = "";
                obj = tmpVtx.<Object>property("aai-node-type").orElse(null);
                if (obj != null) {
                    nt = obj.toString();
                }
                nodeTypesConn2A.put(nt, conVid);
                vtxIdsConn2A.add(conVid);
            }
        }

        Iterator<Edge> eBI = vtxB.edges(Direction.BOTH);
        while (eBI.hasNext()) {
            Edge ed = eBI.next();
            Vertex tmpVtx;

            if (vtxB.equals(ed.inVertex())) {
                tmpVtx = ed.outVertex();
            } else {
                tmpVtx = ed.inVertex();
            }
            if (tmpVtx == null) {
                retArr.add(" >>> COULD NOT FIND VERTEX on the other side of this edge edgeId = " + ed.id() + " <<< ");
            } else {
                String conVid = tmpVtx.id().toString();
                String nt = "";
                obj = tmpVtx.<Object>property("aai-node-type").orElse(null);
                if (obj != null) {
                    nt = obj.toString();
                }
                nodeTypesConn2B.put(nt, conVid);
                vtxIdsConn2B.add(conVid);
            }
        }

        // 1 - If this kind of node needs a dependent node for uniqueness, then
        //    verify that they both nodes point to the same dependent
        //    node (otherwise they're not really duplicates)
        // Note - there are sometimes more than one dependent node type since
        //    one nodeType can be used in different ways. But for a
        //    particular node, it will only have one dependent node that
        //    it's connected to.
        Collection<String> depNodeTypes = loader.introspectorFromName(vtxANodeType).getDependentOn();
        if (depNodeTypes.isEmpty()) {
            // This kind of node is not dependent on any other. That is ok.
        } else {
            String depNodeVtxId4A = "";
            String depNodeVtxId4B = "";
            Iterator<String> iter = depNodeTypes.iterator();
            while (iter.hasNext()) {
                String depNodeType = iter.next();
                if (nodeTypesConn2A.containsKey(depNodeType)) {
                    // This is the dependent node type that vertex A is using
                    depNodeVtxId4A = nodeTypesConn2A.get(depNodeType);
                }
                if (nodeTypesConn2B.containsKey(depNodeType)) {
                    // This is the dependent node type that vertex B is using
                    depNodeVtxId4B = nodeTypesConn2B.get(depNodeType);
                }
            }
            if (depNodeVtxId4A.equals("")
                    || (!depNodeVtxId4A.equals(depNodeVtxId4B))) {
                // Either they're not really dupes or there's some bad data - so
                // don't pick either one
                return nullVtx;
            }
        }

        if (vtxIdsConn2A.size() == vtxIdsConn2B.size()) {
            // 2 - If they both have edges to all the same vertices, then return
            // the one with the lower vertexId.

            // OR (2b)-- if this is the SPECIAL case -- of
            //  "tenant|vserver vs. tenant|service-subscription"
            //   then we pick/prefer the one that's connected to
            //   the service-subscription.  AAI-8172
            boolean allTheSame = true;
            Iterator<String> iter = vtxIdsConn2A.iterator();
            while (iter.hasNext()) {
                String vtxIdConn2A = iter.next();
                if (!vtxIdsConn2B.contains(vtxIdConn2A)) {
                    allTheSame = false;
                    break;
                }
            }

            if (allTheSame) {
                if ( checkAaiUriOk(gts, vtxA, logger) ) {
                	preferredVtx = vtxA;
            	}
            	else if ( checkAaiUriOk(gts, vtxB, logger) ) {
            		preferredVtx = vtxB;
            	}
            	// else we're picking neither because neither one had a working aai-uri index property
            } else if (specialTenantRule) {
                // They asked us to apply a special rule if it applies
                if (vtxIdsConn2A.size() == 2 && vtxANodeType.equals("tenant")) {
                    // We're dealing with two tenant nodes which each just have
                    // two connections.  One must be the parent (cloud-region)
                    // which we check in step 1 above.   If one connects to
                    // a vserver and the other connects to a service-subscription,
                    // our special rule is to keep the one connected
                    // to the
                    if (nodeTypesConn2A.containsKey("vserver") && nodeTypesConn2B.containsKey("service-subscription")) {
                        String infMsg = " WARNING >>> we are using the special tenant rule to choose to " +
                                " delete tenant vtxId = " + vidA + ", and keep tenant vtxId = " + vidB;
                        System.out.println(infMsg);
                        logger.debug(infMsg);
                        preferredVtx = vtxB;
                    } else if (nodeTypesConn2B.containsKey("vserver") && nodeTypesConn2A.containsKey("service-subscription")) {
                        String infMsg = " WARNING >>> we are using the special tenant rule to choose to " +
                                " delete tenant vtxId = " + vidB + ", and keep tenant vtxId = " + vidA;
                        System.out.println(infMsg);
                        logger.debug(infMsg);
                        preferredVtx = vtxA;
                    }
                }
            }
        } else if (vtxIdsConn2A.size() > vtxIdsConn2B.size()) {
            // 3 - VertexA is connected to more things than vtxB.
            // We'll pick VtxA if its edges are a superset of vtxB's edges.
            boolean missingOne = false;
            Iterator<String> iter = vtxIdsConn2B.iterator();
            while (iter.hasNext()) {
                String vtxIdConn2B = iter.next();
                if (!vtxIdsConn2A.contains(vtxIdConn2B)) {
                    missingOne = true;
                    break;
                }
            }
            if (!missingOne) {
                preferredVtx = vtxA;
            }
        } else if (vtxIdsConn2B.size() > vtxIdsConn2A.size()) {
            // 4 - VertexB is connected to more things than vtxA.
            // We'll pick VtxB if its edges are a superset of vtxA's edges.
            boolean missingOne = false;
            Iterator<String> iter = vtxIdsConn2A.iterator();
            while (iter.hasNext()) {
                String vtxIdConn2A = iter.next();
                if (!vtxIdsConn2B.contains(vtxIdConn2A)) {
                    missingOne = true;
                    break;
                }
            }
            if (!missingOne) {
                preferredVtx = vtxB;
            }
        } else {
            preferredVtx = nullVtx;
        }

        return (preferredVtx);

    } // end of pickOneOfTwoDupes()


    /**
     * Group verts by dep nodes.
     *
     * @param transId        the trans id
     * @param fromAppId      the from app id
     * @param g              the g
     * @param version        the version
     * @param nType          the n type
     * @param passedVertList the passed vert list
     * @param loader the loader
     * @return the hash map
     * @throws AAIException the AAI exception
     */
    private HashMap<String, ArrayList<Vertex>> groupVertsByDepNodes(
            String transId, String fromAppId, Graph g, String version,
            String nType, ArrayList<Vertex> passedVertList, Loader loader)
            throws AAIException {

        // Given a list of JanusGraph Vertices, group them together by dependent
        // nodes. Ie. if given a list of ip address nodes (assumed to all
        // have the same key info) they might sit under several different
        // parent vertices.
        // Under Normal conditions, there would only be one per parent -- but
        // we're trying to find duplicates - so we allow for the case
        // where more than one is under the same parent node.

        HashMap<String, ArrayList<Vertex>> retHash = new HashMap<String, ArrayList<Vertex>>();
        GraphTraversalSource gts = g.traversal();
        if (passedVertList != null) {
            Iterator<Vertex> iter = passedVertList.iterator();
            while (iter.hasNext()) {
                Vertex thisVert = iter.next();
                Vertex parentVtx = getConnectedParent(gts, thisVert);
                if (parentVtx != null) {
                    String parentVid = parentVtx.id().toString();
                    if (retHash.containsKey(parentVid)) {
                        // add this vert to the list for this parent key
                        retHash.get(parentVid).add(thisVert);
                    } else {
                        // This is the first one we found on this parent
                        ArrayList<Vertex> vList = new ArrayList<Vertex>();
                        vList.add(thisVert);
                        retHash.put(parentVid, vList);
                    }
                }
            }
        }
        return retHash;

    }// end of groupVertsByDepNodes()


    private Vertex getConnectedParent(GraphTraversalSource g, Vertex startVtx) {

        Vertex parentVtx = null;
        // This traversal does not assume a parent/child edge direction
        Iterator<Vertex> vertI = g.V(startVtx).union(__.inE().has(EdgeProperty.CONTAINS.toString(), AAIDirection.OUT.toString()).outV(), __.outE().has(EdgeProperty.CONTAINS.toString(), AAIDirection.IN.toString()).inV());
        while (vertI != null && vertI.hasNext()) {
            // Note - there better only be one!
            parentVtx = vertI.next();
        }
        return parentVtx;

    }// End of getConnectedParent()


    /**
     * Delete non keepers if appropriate.
     *
     * @param g            the g
     * @param dupeInfoList the dupe info string
     * @param logger       the Logger
     * @return the boolean
     */
    private Boolean deleteNonKeepers(Graph g,
                                            ArrayList<String> dupeInfoList, Logger logger) {

        // This assumes that each dupeInfoString is in the format of
        // pipe-delimited vid's followed by either "keepVid=xyz" or "keepVid=UNDETERMINED"
        // ie. "3456|9880|keepVid=3456"

        boolean didADelFlag = false;
        for (String dupeInfoString : dupeInfoList) {
            didADelFlag |= deleteNonKeeperForOneSet(g, dupeInfoString, logger);
        }

        return didADelFlag;

    }// end of deleteNonKeepers()


    /**
     * Delete non keepers if appropriate.
     *
     * @param g                 the g
     * @param dupeInfoString    the dupe string
     * @param logger            the Logger
     * @return the boolean
     */
    private Boolean deleteNonKeeperForOneSet(Graph g,
                                                    String dupeInfoString, Logger logger) {

        Boolean deletedSomething = false;
        // This assumes that each dupeInfoString is in the format of
        // pipe-delimited vid's followed by either "keepVid=xyz" or "keepVid=UNDETERMINED"
        // ie. "3456|9880|keepVid=3456"


        String[] dupeArr = dupeInfoString.split("\\|");
        ArrayList<String> idArr = new ArrayList<>();
        int lastIndex = dupeArr.length - 1;
        for (int i = 0; i <= lastIndex; i++) {
            if (i < lastIndex) {
                // This is not the last entry, it is one of the dupes,
                String vidString = dupeArr[i];
                idArr.add(vidString);
            } else {
                // This is the last entry which should tell us if we have a
                // preferred keeper
                String prefString = dupeArr[i];
                if (prefString.equals("KeepVid=UNDETERMINED")) {
                    // They sent us a bad string -- nothing should be deleted if
                    // no dupe could be tagged as preferred.
                    return false;
                } else {
                    // If we know which to keep, then the prefString should look
                    // like, "KeepVid=12345"
                    String[] prefArr = prefString.split("=");
                    if (prefArr.length != 2 || (!prefArr[0].equals("KeepVid"))) {
                        String emsg = "Bad format. Expecting KeepVid=999999";
                        System.out.println(emsg);
                        logger.error(emsg);
                        return false;
                    } else {
                        String keepVidStr = prefArr[1];
                        if (idArr.contains(keepVidStr)) {
                            idArr.remove(keepVidStr);
                            // So now, the idArr should just contain the vid's
                            // that we want to remove.
                            for (int x = 0; x < idArr.size(); x++) {
                                boolean okFlag = true;
                                String thisVid = idArr.get(x);
                                try {
                                    long longVertId = Long.parseLong(thisVid);
                                    Vertex vtx = g.traversal().V(longVertId).next();
                                    String msg = "--->>>   We will delete node with VID = " + thisVid + " <<<---";
                                    System.out.println(msg);
                                    logger.debug(msg);
                                    vtx.remove();
                                } catch (Exception e) {
                                    okFlag = false;
                                    String emsg = "ERROR trying to delete VID = " + thisVid + ", [" + e + "]";
                                    System.out.println(emsg);
                                    logger.error(emsg);
                                }
                                if (okFlag) {
                                    String infMsg = " DELETED VID = " + thisVid;
                                    logger.debug(infMsg);
                                    System.out.println(infMsg);
                                    deletedSomething = true;
                                }
                            }
                        } else {
                            String emsg = "ERROR - Vertex Id to keep not found in list of dupes.  dupeInfoString = ["
                                    + dupeInfoString + "]";
                            logger.error(emsg);
                            System.out.println(emsg);
                            return false;
                        }
                    }
                }// else we know which one to keep
            }// else last entry
        }// for each vertex in a group

        return deletedSomething;

    }// end of deleteNonKeeperForOneSet()


    /**
     * Get values of the key properties for a node.
     *
     * @param tvx              the vertex to pull the properties from
     * @param keyPropNamesArr  ArrayList (ordered) of key prop names
     * @param logger           the Logger
     * @return a hashMap of the propertyNames/values
     */
    private HashMap<String, Object> getNodeKeyVals(Vertex tvx,
                                                          ArrayList<String> keyPropNamesArr, Logger logger) {

        HashMap<String, Object> retHash = new HashMap<>();
        Iterator<String> propItr = keyPropNamesArr.iterator();
        while (propItr.hasNext()) {
            String propName = propItr.next();
            if (tvx != null) {
                Object propValObj = tvx.property(propName).orElse(null);
                retHash.put(propName, propValObj);
            }
        }
        return retHash;

    }// End of getNodeKeyVals()



	/**
	 * makes sure aai-uri exists and can be used to get this node back
     *
	 * @param graph the graph
	 * @param origVtx
	 * @param eLogger
	 * @return true if aai-uri is populated and the aai-uri-index points to this vtx
	 * @throws AAIException the AAI exception
	 */
	private Boolean checkAaiUriOk( GraphTraversalSource graph, Vertex origVtx, Logger eLogger ) {
		String aaiUriStr = "";
		try {
			Object ob = origVtx.<Object>property("aai-uri").orElse(null);
			String origVid = origVtx.id().toString();
			if (ob == null || ob.toString().equals("")) {
				// It is missing its aai-uri
				eLogger.debug("DEBUG No [aai-uri] property found for vid = [%s] ".formatted(origVid));
				return false;
			}
			else {
				aaiUriStr = ob.toString();
				Iterator <Vertex> verts = graph.V().has("aai-uri",aaiUriStr);
				int count = 0;
				while( verts.hasNext() ){
					count++;
					Vertex foundV = verts.next();
					String foundVid = foundV.id().toString();
					if( !origVid.equals(foundVid) ){
						eLogger.debug("DEBUG aai-uri key property [%s] for vid = [%s] brought back different vertex with vid = [%s].".formatted(aaiUriStr, origVid, foundVid));
						return false;
					}
				}
				if( count == 0 ){
					eLogger.debug("DEBUG aai-uri key property [%s] for vid = [%s] could not be used to query for that vertex. ".formatted(aaiUriStr, origVid));
					return false;
				}
				else if( count > 1 ){
					eLogger.debug("DEBUG aai-uri key property [%s] for vid = [%s] brought back multiple (%d) vertices instead of just one. ".formatted(aaiUriStr, origVid, count));
					return false;
				}
			}
		}
		catch( Exception ex ){
			eLogger.error(" ERROR trying to get node with aai-uri: [" + aaiUriStr + "]" + LogFormatTools.getStackTop(ex));
		}
		return true;

	}// End of checkAaiUriOk()


    /**
     * Get values of the key properties for a node as a single string
     *
     * @param tvx              the vertex to pull the properties from
     * @param keyPropNamesArr  collection of key prop names
     * @param logger           the Logger
     * @return a String of concatenated values
     */
    private String getNodeKeyValString(Vertex tvx,
                                              ArrayList<String> keyPropNamesArr, Logger logger) {

        // -- NOTE -- for what we're using this for, we would need to
        // guarantee that the properties are always in the same order

        String retString = "";
        Iterator<String> propItr = keyPropNamesArr.iterator();
        while (propItr.hasNext()) {
            String propName = propItr.next();
            if (tvx != null) {
                Object propValObj = tvx.property(propName).orElse(null);
                retString = " " + retString + propValObj.toString();
            }
        }
        return retString;

    }// End of getNodeKeyValString()


    /**
     * Find duplicate sets from two dupe runs.
     *
     * @param firstPassDupeSets  from the first pass
     * @param secondPassDupeSets from the second pass
     * @param logger         logger
     * @return commonDupeSets that are common to both passes and have a determined keeper
     */
    private ArrayList<String> figureWhichDupesStillNeedFixing(ArrayList<String> firstPassDupeSets,
                                                                     ArrayList<String> secondPassDupeSets, Logger logger) {

        ArrayList<String> common2BothSet = new ArrayList<>();

        // We just want to look for entries from the first set which have identical (almost)
        //    entries in the secondary set.  I say "almost" because the order of the
        //    vid's to delete may be in a different order, but we only want to use it if
        //    they have all the same values.   Note also - we're just looking for
        //    the sets where we have a candidate to delete.

        // The duplicate-set Strings are in this format:
        // "1234|5678|keepVid=UNDETERMINED" (if there were 2 dupes, and we
        // couldn't figure out which one to keep)
        // or, "100017|200027|30037|keepVid=30037" (if there were 3 dupes and we
        // thought the third one was the one that should survive)

        if (firstPassDupeSets == null || firstPassDupeSets.isEmpty()
                || secondPassDupeSets == null || secondPassDupeSets.isEmpty()) {
            // If either set is empty, then our return list has to be empty too
            return common2BothSet;
        }

        boolean needToParse = false;
        for (int x = 0; x < secondPassDupeSets.size(); x++) {
            String secPassDupeSetStr = secondPassDupeSets.get(x);
            if (secPassDupeSetStr.endsWith("UNDETERMINED")) {
                // This is a set of dupes where we could not pick one
                // to delete - so don't include it on our list for
                // fixing.
            } else if (firstPassDupeSets.contains(secPassDupeSetStr)) {
                // We have lucked out and do not even need to parse this since
                // it was in the other array with any dupes listed in the same order
                // This is actually the most common scenario since there is
                // usually only one dupe, so order doesn't matter.
                common2BothSet.add(secPassDupeSetStr);
            } else {
                // We'll need to do some parsing to check this one
                needToParse = true;
            }
        }

        if (needToParse) {
            // Make a hash from the first and second Pass data
            //	where the key is the vid to KEEP and the value is an
            // 	array of (String) vids that would get deleted.
            HashMap<String, ArrayList<String>> firstPassHash = makeKeeperHashOfDupeStrings(firstPassDupeSets, common2BothSet, logger);

            HashMap<String, ArrayList<String>> secPassHash = makeKeeperHashOfDupeStrings(secondPassDupeSets, common2BothSet, logger);

            // Loop through the secondPass data and keep the ones
            //       that check out against the firstPass set.
            for (Map.Entry<String, ArrayList<String>> entry : secPassHash.entrySet()) {
                boolean skipThisOne = false;
                String secKey = entry.getKey();
                ArrayList<String> secList = entry.getValue();
                if (!firstPassHash.containsKey(secKey)) {
                    // The second pass found this delete candidate, but not the first pass
                    skipThisOne = true;
                } else {
                    // They both think they should keep this VID, check the associated deletes for it.
                    ArrayList<String> firstList = firstPassHash.get(secKey);
                    for (int z = 0; z < secList.size(); z++) {
                        if (!firstList.contains(secList.get(z))) {
                            // The first pass did not think this needed to be deleted
                            skipThisOne = true;
                        }
                    }
                }
                if (!skipThisOne) {
                    // Put the string back together and pass it back
                    // Not beautiful, but no time to make it nice right now...
                    // Put it back in the format: "3456|9880|keepVid=3456"
                    String thisDelSetStr = "";
                    for (int z = 0; z < secList.size(); z++) {
                        if (z == 0) {
                            thisDelSetStr = secList.get(z);
                        } else {
                            thisDelSetStr = thisDelSetStr + "|" + secList.get(z);
                        }
                    }
                    thisDelSetStr = thisDelSetStr + "|keepVid=" + secKey;
                    common2BothSet.add(thisDelSetStr);
                }
            }

        }
        return common2BothSet;

    }// figureWhichDupesStillNeedFixing


    private HashMap<String, ArrayList<String>> makeKeeperHashOfDupeStrings(ArrayList<String> dupeSets,
                                                                                  ArrayList<String> excludeSets, Logger logger) {

        HashMap<String, ArrayList<String>> keeperHash = new HashMap<>();

        for (int x = 0; x < dupeSets.size(); x++) {
            String tmpSetStr = dupeSets.get(x);
            if (excludeSets.contains(tmpSetStr)) {
                // This isn't one of the ones we needed to parse.
                continue;
            }

            String[] dupeArr = tmpSetStr.split("\\|");
            ArrayList<String> delIdArr = new ArrayList<>();
            int lastIndex = dupeArr.length - 1;
            for (int i = 0; i <= lastIndex; i++) {
                if (i < lastIndex) {
                    // This is not the last entry, it is one of the dupes
                    delIdArr.add(dupeArr[i]);
                } else {
                    // This is the last entry which should tell us if we
                    // have a preferred keeper and how many dupes we had
                    String prefString = dupeArr[i];
                    if (i == 1) {
                        // There was only one dupe, so if we were gonna find
                        // it, we would have found it above with no parsing.
                    } else if (prefString.equals("KeepVid=UNDETERMINED")) {
                        // This one had no determined keeper, so we don't
                        // want it.
                    } else {
                        // If we know which to keep, then the prefString
                        // should look like, "KeepVid=12345"
                        String[] prefArr = prefString.split("=");
                        if (prefArr.length != 2
                                || (!prefArr[0].equals("KeepVid"))) {
                            String infMsg = "Bad format in figureWhichDupesStillNeedFixing(). Expecting " +
                                    " KeepVid=999999 but string looks like: [" + tmpSetStr + "]";
                            System.out.println(infMsg);
                            logger.debug(infMsg);
                        } else {
                            keeperHash.put(prefArr[0], delIdArr);
                        }
                    }
                }
            }
        }

        return keeperHash;

    }// End makeHashOfDupeStrings()


    /**
     * Get values of the key properties for a node.
     *
     * @param g              the g
     * @param dupeInfoString
     * @param logger         the Logger
     * @return void
     */
    private void showNodeDetailsForADupeSet(Graph g, String dupeInfoString, Logger logger) {

        // dang...   parsing this string once again...

        String[] dupeArr = dupeInfoString.split("\\|");
        int lastIndex = dupeArr.length - 1;
        for (int i = 0; i <= lastIndex; i++) {
            if (i < lastIndex) {
                // This is not the last entry, it is one of the dupes,
                String vidString = dupeArr[i];
                long longVertId = Long.parseLong(vidString);
                Vertex vtx = g.traversal().V(longVertId).next();
                showNodeInfo(logger, vtx, false);
            } else {
                // This is the last entry which should tell us if we have a
                // preferred keeper
                String prefString = dupeArr[i];
                if (prefString.equals("KeepVid=UNDETERMINED")) {
                    String msg = " Our algorithm cannot choose from among these, so they will all be kept. -------\n";
                    System.out.println(msg);
                    logger.debug(msg);
                } else {
                    // If we know which to keep, then the prefString should look
                    // like, "KeepVid=12345"
                    String[] prefArr = prefString.split("=");
                    if (prefArr.length != 2 || (!prefArr[0].equals("KeepVid"))) {
                        String emsg = "Bad format. Expecting KeepVid=999999";
                        System.out.println(emsg);
                        logger.error(emsg);
                    } else {
                        String keepVidStr = prefArr[1];
                        String msg = " vid = " + keepVidStr + " is the one that we would KEEP. ------\n";
                        System.out.println(msg);
                        logger.debug(msg);
                    }
                }
            }
        }

    }// End of showNodeDetailsForADupeSet()

    private int graphIndex = 1;

    public JanusGraph setupGraph(Logger logger) {

        JanusGraph janusGraph = null;


        try (InputStream inputStream = new FileInputStream(AAIConstants.REALTIME_DB_CONFIG);) {

            Properties properties = new Properties();
            properties.load(inputStream);

            if ("inmemory".equals(properties.get("storage.backend"))) {
                janusGraph = AAIGraph.getInstance().getGraph();
                graphType = "inmemory";
            } else {
                janusGraph = JanusGraphFactory.open(new AAIGraphConfig.Builder(AAIConstants.REALTIME_DB_CONFIG).forService(DupeTool.class.getSimpleName()).withGraphType("realtime" + graphIndex).buildConfiguration());
                graphIndex++;
            }
        } catch (Exception e) {
            logger.error("Unable to open the graph", e);
        }

        return janusGraph;
    }

    public void closeGraph(JanusGraph graph, Logger logger) {

        try {
            if ("inmemory".equals(graphType)) {
                return;
            }
            if (graph != null && graph.isOpen()) {
                graph.tx().close();
                graph.close();
            }
        } catch (Exception ex) {
            // Don't throw anything because JanusGraph sometimes is just saying that the graph is already closed{
            logger.warn("WARNING from final graph.shutdown()", ex);
        }
    }

	public int getDupeGroupCount() {
		return dupeGroupCount;
	}

	public void setDupeGroupCount(int dgCount) {
		this.dupeGroupCount = dgCount;
	}

}
