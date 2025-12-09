/**
 * ============LICENSE_START=======================================================
 * org.onap.aai
 * ================================================================================
 * Copyright © 2017-2018 AT&T Intellectual Property. All rights reserved.
 * ================================================================================
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 * <p>
 * http://www.apache.org/licenses/LICENSE-2.0
 * <p>
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 * ============LICENSE_END=========================================================
 */
package org.onap.aai.dbgen;

import com.beust.jcommander.JCommander;
import jakarta.validation.ValidationException;
import org.onap.aai.schema.enums.ObjectMetadata;
import org.onap.aai.util.AAISystemExitUtil;
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
import org.slf4j.MDC;
import org.springframework.context.annotation.AnnotationConfigApplicationContext;

import java.io.FileInputStream;
import java.io.InputStream;
import java.util.*;
import java.util.Map.Entry;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.stream.Collectors;

public class DupeTool {

    private static final Logger logger = LoggerFactory.getLogger(DupeTool.class.getSimpleName());
    private static final String FROMAPPID = "AAI-DB";
    private static final String TRANSID = UUID.randomUUID().toString();
    public static final String AAI_NODE_TYPE = "aai-node-type";
    public static final String NAME = "-name";
    public static final String DETAILS = "details";
    public static final String AAI_URI = "aai-uri";
    public static final String KEEP_VID = "KeepVid";
    public static final String INMEMORY = "inmemory";

    private static String graphType = "realdb";
    private final SchemaVersions schemaVersions;

    private boolean shouldExitVm = true;

    private DupeToolCommandLineArgs cArgs;

    public void exit(int statusCode) {
        if (this.shouldExitVm) {
            System.exit(statusCode);
        }
    }

    private LoaderFactory loaderFactory;
    private int dupeGroupCount = 0;

    public DupeTool(LoaderFactory loaderFactory, SchemaVersions schemaVersions) {
        this(loaderFactory, schemaVersions, true);
    }

    public DupeTool(LoaderFactory loaderFactory, SchemaVersions schemaVersions, boolean shouldExitVm) {
        this.loaderFactory = loaderFactory;
        this.schemaVersions = schemaVersions;
        this.shouldExitVm = shouldExitVm;
    }

    public void execute(String[] args) throws AAIException {
        String defVersion = getDefVersion();

        dupeGroupCount = 0;
        Loader loader = getLoader();
        JanusGraph janusGraph1 = null;
        JanusGraph janusGraph2 = null;
        Graph gt1 = null;
        Graph gt2 = null;
        try {
            AAIConfig.init();

            cArgs = new DupeToolCommandLineArgs();
            JCommander jCommander = new JCommander(cArgs, args);
            jCommander.setProgramName(DupeTool.class.getSimpleName());

            boolean autoFix = cArgs.doAutoFix;
            int maxRecordsToFix = cArgs.maxRecordsToFix;
            int timeWindowMinutes = cArgs.timeWindowMinutes;
            int sleepMinutes = cArgs.sleepMinutes;
            boolean skipHostCheck = cArgs.skipHostCheck;
            final boolean specialTenantRule = cArgs.specialTenantRule;
            String nodeTypes = cArgs.nodeTypes;
            String filterParams = cArgs.filterParams;
            String userIdVal = cArgs.userId.trim();
            validateUserId(userIdVal);
            boolean allNodeTypes = cArgs.forAllNodeTypes;

            boolean multipleNodeTypes = false;
            String[] nodeTypesArr = null;

            if (allNodeTypes) {
                // run for defined set of nodes
                String nodeTypesProp = AAIConfig.get("aai.dupeTool.nodeTypes");
                if (nodeTypesProp.contains(",") && nodeTypesProp.split(",").length > 0) {
                    nodeTypesArr = nodeTypesProp.split(",");
                    processMultipleNodeTypes(nodeTypesArr, janusGraph1, filterParams, timeWindowMinutes, loader,
                            defVersion, specialTenantRule, autoFix, sleepMinutes, maxRecordsToFix);
                }
            } else {
                // Validate if nodeTypes is passed & is not empty
                validateNodeType(nodeTypes);

                if (nodeTypes.contains(",")) {
                    multipleNodeTypes = true;
                    nodeTypesArr = nodeTypes.split(",");
                }

                if (multipleNodeTypes) {
                    // Run in threads
                    processMultipleNodeTypes(nodeTypesArr, janusGraph1, filterParams, timeWindowMinutes, loader,
                            defVersion, specialTenantRule, autoFix, sleepMinutes, maxRecordsToFix);
                } else {
                    processMultipleNodeTypes(new String[]{nodeTypes}, janusGraph1, filterParams, timeWindowMinutes, loader,
                            defVersion, specialTenantRule, autoFix, sleepMinutes, maxRecordsToFix);
                }
            }

        } catch (AAIException e) {
            logger.error("Caught AAIException while running the dupeTool: " + LogFormatTools.getStackTop(e));
            ErrorLogHelper.logException(e);
            throw new AAIException(e.getMessage());
        } catch (Exception ex) {
            logger.error("Caught exception while running the dupeTool: " + LogFormatTools.getStackTop(ex));
            ErrorLogHelper.logError("AAI_6128", ex.getMessage() + ", resolve and rerun the dupeTool. ");
            throw new AAIException(ex.getMessage());
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
                if (janusGraph1 != null && janusGraph1.isOpen()) {
                    closeGraph(janusGraph1, logger);
                }
            } catch (Exception ex) {
                // Don't throw anything because JanusGraph sometimes is just saying that the graph is already closed{
                logger.warn("WARNING from final janusGraph1.shutdown() " + LogFormatTools.getStackTop(ex));
            }

            try {
                if (janusGraph2 != null && janusGraph2.isOpen()) {
                    closeGraph(janusGraph2, logger);
                }
            } catch (Exception ex) {
                // Don't throw anything because JanusGraph sometimes is just saying that the graph is already closed{
                logger.warn("WARNING from final janusGraph2.shutdown() " + LogFormatTools.getStackTop(ex));
            }
        }
    }

    private void processMultipleNodeTypes(String[] nodeTypes, JanusGraph janusGraph, String filterParams,
                                          int timeWindowMinutes, Loader loader, String defVersion,
                                          boolean specialTenantRule, boolean autoFix, int sleepMinutes, int maxRecordsToFix) throws AAIException {
        if (janusGraph == null || !janusGraph.isOpen()) {
            janusGraph = setupGraph(logger);
        }
        int threadCount = Math.min(nodeTypes.length, 5); // limit to 5 threads max
        ExecutorService executor = Executors.newFixedThreadPool(threadCount);
        for (String nodeType : nodeTypes) {
            Graph graph1 = getGraphTransaction(janusGraph);
            Graph graph2 = getGraphTransaction(janusGraph);
            executor.submit(() -> {
                try {
                    processNodeType(graph1, graph2, nodeType,
                            filterParams, timeWindowMinutes, loader, defVersion, specialTenantRule, autoFix,
                            sleepMinutes, maxRecordsToFix);
                } catch (InterruptedException | AAIException e) {
                    throw new RuntimeException(e);
                }
            });
        }
    }

    private void processNodeType(Graph graph1, Graph graph2,
                                 String nodeTypeVal, String filterParams,
                                 int timeWindowMinutes, Loader loader, String defVersion, boolean specialTenantRule,
                                 boolean autoFix, int sleepMinutes, int maxRecordsToFix) throws InterruptedException, AAIException {
        long windowStartTime = 0;
        if (timeWindowMinutes > 0) {
            // Translate the window value (ie. 30 minutes) into a unix timestamp like
            //    we use in the db - so we can select data created after that time.
            windowStartTime = figureWindowStartTime(timeWindowMinutes);
        }
        logger.info("DupeTool called with these params: [{}]", getParamString(nodeTypeVal));

        final Introspector obj = loader.introspectorFromName(nodeTypeVal);
        // Determine what the key fields are for this nodeType (and we want them ordered)
        ArrayList<String> keyPropNamesArr = new ArrayList<>(obj.getKeys());

        // Based on the nodeType, window and filterData, figure out the vertices that we will be checking
        logger.info("    ---- NOTE --- about to open graph (takes a little while)--------\n");

        nodeTypeVal = nodeTypeVal.trim();

        List<Vertex> vertsToCheck = getVertices(graph1, nodeTypeVal, windowStartTime, filterParams, timeWindowMinutes);

        ArrayList<String> firstPassDupeSets = new ArrayList<>();
        ArrayList<String> secondPassDupeSets = new ArrayList<>();
        boolean isDependentOnParent = false;
        if (!obj.getDependentOn().isEmpty()) {
            isDependentOnParent = true;
        }
        boolean hasName = false;
        String name = "";
        List<String> nameProps = getNameProps(loader, nodeTypeVal);
        for (String entry : nameProps) {
            if (entry.contains(NAME)) {
                name = entry;
                hasName = true;
                break;
            }
        }

        if (isDependentOnParent) {
            firstPassDupeSets = getDupeSetsForDependentNodes(graph1,
                    defVersion, nodeTypeVal, vertsToCheck, keyPropNamesArr, loader,
                    specialTenantRule, hasName, name);
            logger.info("First pass dupe sets: {}", firstPassDupeSets);
        } else {
            firstPassDupeSets = getDupeSetsForNonDepNodes(graph1,
                    defVersion, vertsToCheck, keyPropNamesArr,
                    specialTenantRule, loader);
            logger.info("Else First pass dupe sets: {}", firstPassDupeSets);
        }

        logger.info(" Found {} sets of duplicates for this request. ", firstPassDupeSets.size());
        if (!firstPassDupeSets.isEmpty()) {
            logger.info(" Here is what they look like: ");
            for (int x = 0; x < firstPassDupeSets.size(); x++) {
                if (logger.isInfoEnabled())
                    logger.info(" Set {}: [{}] ", x, firstPassDupeSets.get(x));
                showNodeDetailsForADupeSet(graph1, firstPassDupeSets.get(x));
            }
        }
        dupeGroupCount = firstPassDupeSets.size();
        boolean didSomeDeletesFlag = false;
        if (autoFix && firstPassDupeSets.isEmpty()) {
            logger.info("AutoFix option is on, but no dupes were found on the first pass.  Nothing to fix.");
        } else if (autoFix) {
            // We will try to fix any dupes that we can - but only after sleeping for a
            // time and re-checking the list of duplicates using a seperate transaction.
            sleep(sleepMinutes);

            if (isDependentOnParent) {
                secondPassDupeSets = getDupeSetsForDependentNodes(graph2,
                        defVersion, nodeTypeVal, vertsToCheck, keyPropNamesArr, loader,
                        specialTenantRule, hasName, name);
            } else {
                secondPassDupeSets = getDupeSetsForNonDepNodes(graph2,
                        defVersion, vertsToCheck, keyPropNamesArr,
                        specialTenantRule, loader);
            }

            didSomeDeletesFlag = isDidSomeDeletesFlag(graph2, maxRecordsToFix,
                    didSomeDeletesFlag, firstPassDupeSets, secondPassDupeSets);
            if (didSomeDeletesFlag) {
                graph2.tx().commit();
                // Run reindexing
                ReindexingTool reindexingTool = new ReindexingTool();
                reindexingTool.reindexByName(nodeTypeVal + "-id");
            }
        }
    }

    private String getParamString(String nodeType) {
        return "doAutoFix=" + cArgs.doAutoFix +
                ", maxRecordsToFix=" + cArgs.maxRecordsToFix +
                ", sleepMinutes=" + cArgs.sleepMinutes +
                ", userId='" + cArgs.userId + '\'' +
                ", nodeType='" + nodeType + '\'' +
                ", timeWindowMinutes=" + cArgs.timeWindowMinutes +
                ", skipHostCheck=" + cArgs.skipHostCheck +
                ", specialTenantRule=" + cArgs.specialTenantRule +
                ", filterParams='" + cArgs.filterParams + '\'' +
                ", forAllNodeTypes=" + cArgs.forAllNodeTypes;
    }

    private void sleep(int sleepMinutes) {
        try {
            logger.info("\n\n-----------  About to sleep for {} minutes. -----------\n\n", sleepMinutes);
            int sleepMsec = sleepMinutes * 60 * 1000;
            Thread.sleep(sleepMsec);
        } catch (InterruptedException ie) {
            logger.error("\n >>> Sleep Thread has been Interrupted <<< ");
            AAISystemExitUtil.systemExitCloseAAIGraph(0);
        }
    }

    private boolean isDidSomeDeletesFlag(Graph gt2, int maxRecordsToFix,
                                         boolean didSomeDeletesFlag,
                                         ArrayList<String> firstPassDupeSets,
                                         ArrayList<String> secondPassDupeSets) throws AAIException {
        ArrayList<String> dupeSetsToFix = figureWhichDupesStillNeedFixing(firstPassDupeSets, secondPassDupeSets);
        logger.info("\nAfter running a second pass, there were {} sets of duplicates that we think can be deleted. ", dupeSetsToFix.size());
        if (!dupeSetsToFix.isEmpty()) {
            logger.info(" Here is what the sets look like: ");
            for (int x = 0; x < dupeSetsToFix.size(); x++) {
                if (logger.isInfoEnabled())
                    logger.info(" Set {}: [{}] ", x, dupeSetsToFix.get(x));
                showNodeDetailsForADupeSet(gt2, dupeSetsToFix.get(x));
            }
        }

        if (!dupeSetsToFix.isEmpty()) {
            if (dupeSetsToFix.size() > maxRecordsToFix) {
                logger.info(" >> WARNING >>  Dupe list size ({}) is too big.  The maxFix we are using is: {}.  No nodes will be deleted. (use the"
                        + " -maxFix option to override this limit.)", dupeSetsToFix.size(), maxRecordsToFix);
            } else {
                // Call the routine that fixes known dupes
                didSomeDeletesFlag = deleteNonKeepers(gt2, dupeSetsToFix);
            }
        }
        return didSomeDeletesFlag;
    }


    private List<Vertex> getVertices(Graph gt1,
                                          String nodeTypeVal, long windowStartTime, String filterParams,
                                          int timeWindowMinutes) {
        List<Vertex> vertsToCheck = new ArrayList<>();
        try {
            vertsToCheck = figureOutNodesToCheck(gt1, nodeTypeVal, windowStartTime, filterParams);
        } catch (AAIException ae) {
            logger.error("Error trying to get initial set of nodes to check. \n");
            throw new ValidationException("Error trying to get initial set of nodes to check. \n");
        }

        if (vertsToCheck == null || vertsToCheck.isEmpty()) {
            logger.info(" No vertices found to check.  Used nodeType = [{}], windowMinutes = {}, filterData = [{}].", nodeTypeVal, timeWindowMinutes, filterParams);
        } else {
            logger.info(" Found {} nodes of type {} to check using passed filterParams and windowStartTime. ", vertsToCheck.size(), nodeTypeVal);
        }
        return vertsToCheck;
    }

    private void validateNodeType(String nodeTypeVal) {
        if (null == nodeTypeVal || nodeTypeVal.isEmpty()) {
            logger.error(" nodeTypes is a required parameter for DupeTool().\n");
            throw new ValidationException(" nodeTypes is a required parameter for DupeTool().\n");
        }
    }

    private void validateUserId(String userIdVal) {
        if ((userIdVal.length() < 6) || userIdVal.equalsIgnoreCase("AAIADMIN")) {
            logger.error("userId parameter is required.  [{}] passed to DupeTool(). userId must be not empty and not aaiadmin \n", userIdVal);
            throw new ValidationException("userId parameter is required.  [" +
                    userIdVal + "] passed to DupeTool(). userId must be not empty and not aaiadmin \n");
        }
    }

    private Loader getLoader() {
        Loader loader = null;
        try {
            loader = loaderFactory.createLoaderForVersion(ModelType.MOXY, schemaVersions.getDefaultVersion());
        } catch (Exception ex) {
            logger.error("ERROR - Could not do the moxyMod.init() {}", LogFormatTools.getStackTop(ex));
            throw new ValidationException(ex.getMessage());
        }
        return loader;
    }

    private String getDefVersion() throws AAIException {
        String defVersion = null;
        try {
            defVersion = AAIConfig.get(AAIConstants.AAI_DEFAULT_API_VERSION_PROP) == null
                    || AAIConfig.get(AAIConstants.AAI_DEFAULT_API_VERSION_PROP).isEmpty()
                    ? "v18"
                    : AAIConfig.get(AAIConstants.AAI_DEFAULT_API_VERSION_PROP);
        } catch (AAIException ae) {
            logger.error("Error trying to get default API Version property \n");
            throw new AAIException("Error trying to get default API Version property \n");
        }
        return defVersion;
    }

    private List<String> getNameProps(Loader loader, String nodeType) {
        Map<String, Introspector> allObjects = loader.getAllObjects();

        Object model = allObjects.get(nodeType);
        if (model == null) {
            return Collections.emptyList(); // node type not found
        }

        Object meta = ((Introspector) model).getMetadata(ObjectMetadata.NAME_PROPS);
        if (meta == null) {
            return Collections.emptyList(); // no nameProps defined
        }

        // Split comma-separated values, trim whitespace
        return Arrays.stream(meta.toString().split(","))
                .map(String::trim)
                .filter(s -> !s.isEmpty())
                .toList();
    }

    /**
     * The main method.
     *
     * @param args the arguments
     */
    public static void main(String[] args) {

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
            logger.error("Problems running DupeTool {}", aai.getMessage());
            ErrorLogHelper.logError(aai.getCode(), e.getMessage() + ", resolve and retry");
        }
        LoaderFactory loaderFactory = ctx.getBean(LoaderFactory.class);
        SchemaVersions schemaVersions = (SchemaVersions) ctx.getBean("schemaVersions");
        DupeTool dupeTool = new DupeTool(loaderFactory, schemaVersions);
        try {
            dupeTool.execute(args);
        } catch (AAIException e) {
            logger.error("Exception occurred in running DupeTool: {}", e.getMessage());
            throw new RuntimeException(e);
        }
    }// end of main()


    /**
     * Collect Duplicate Sets for nodes that are NOT dependent on parent nodes.
     *
     * @param g              the g
     * @param version        the version
     * @param passedVertList the passed vert list
     * @param loader         the loader
     * @return the array list
     */
    private ArrayList<String> getDupeSetsForNonDepNodes(Graph g, String version,
                                                        List<Vertex> passedVertList,
                                                        ArrayList<String> keyPropNamesArr,
                                                        Boolean specialTenantRule, Loader loader) {
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

        HashMap<String, ArrayList<String>> keyValsToVidHash = new HashMap<>();
        HashMap<String, Vertex> vtxHash = new HashMap<>();
        Iterator<Vertex> pItr = passedVertList.iterator();
        while (pItr.hasNext()) {
            try {
                Vertex tvx = pItr.next();
                String thisVid = tvx.id().toString();
                vtxHash.put(thisVid, tvx);

                // if there are more than one vertexId mapping to the same keyProps -- they are dupes
                String hKey = getNodeKeyValString(tvx, keyPropNamesArr);
                if (keyValsToVidHash.containsKey(hKey)) {
                    // We've already seen this key
                    ArrayList<String> tmpVL = keyValsToVidHash.get(hKey);
                    tmpVL.add(thisVid);
                    keyValsToVidHash.put(hKey, tmpVL);
                } else {
                    // First time for this key
                    ArrayList<String> tmpVL = new ArrayList<>();
                    tmpVL.add(thisVid);
                    keyValsToVidHash.put(hKey, tmpVL);
                }
            } catch (Exception e) {
                logger.warn(" >>> Threw an error in getDupeSets4NonDepNodes - just absorb this error and move on. " + LogFormatTools.getStackTop(e));
            }
        }

        for (Map.Entry<String, ArrayList<String>> entry : keyValsToVidHash.entrySet()) {
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

                    if (!dupesStr.isEmpty()) {
                        Vertex prefV = getPreferredDupe(DupeTool.TRANSID, DupeTool.FROMAPPID,
                                g, vertList, version, specialTenantRule, loader);
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
     * @param g                 the g
     * @param version           the version
     * @param nType             the n type
     * @param passedVertList    the passed vert list
     * @param keyPropNamesArr   Array (ordered) of keyProperty names
     * @param specialTenantRule flag
     * @return the array list
     */
    private ArrayList<String> getDupeSetsForDependentNodes(Graph g, String version, String nType,
                                                           List<Vertex> passedVertList,
                                                           ArrayList<String> keyPropNamesArr, Loader loader,
                                                           Boolean specialTenantRule, boolean hasName, String nameProp) {

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
            for (Vertex tvx : passedVertList) {
                String passedId = tvx.id().toString();

                if (!alreadyFoundDupeVidArr.contains(passedId)) {

                    Map<String, Object> keyPropValsHash = new HashMap<>();
                    if (hasName) {
                        Object namePropValue = tvx.property(nameProp).orElse(null);
                        keyPropValsHash = getNodeKeyVals(tvx, keyPropNamesArr, nameProp, namePropValue.toString());
                    } else {
                        keyPropValsHash = getNodeKeyVals(tvx, keyPropNamesArr, null, null);
                    }
                    // We haven't seen this one before - so we should check it.
                    List<Vertex> tmpVertList = getNodeJustUsingKeyParams(g,
                            nType, keyPropValsHash);

                    if (tmpVertList.size() <= 1) {
                        // Even without a parent node, this thing is unique so don't worry about it.
                    } else {
                        for (Vertex tmpVtx : tmpVertList) {
                            String tmpVid = tmpVtx.id().toString();
                            alreadyFoundDupeVidArr.add(tmpVid);

                            String hKey = getNodeKeyValString(tmpVtx, keyPropNamesArr);
                            if (checkVertHash.containsKey(hKey)) {
                                // add it to an existing list
                                ArrayList<Vertex> tmpVL = (ArrayList<Vertex>) checkVertHash.get(hKey);
                                tmpVL.add(tmpVtx);
                                checkVertHash.put(hKey, tmpVL);
                            } else {
                                // First time for this key
                                ArrayList<Vertex> tmpVL = new ArrayList<>();
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

                Map<String, ArrayList<Vertex>> vertsGroupedByParentHash = groupVertsByDepNodes(g,
                        thisIdSetList);
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
                        if (!dupesStr.isEmpty()) {
                            Vertex prefV = getPreferredDupe(DupeTool.TRANSID,
                                    DupeTool.FROMAPPID, g, thisParentsVertList,
                                    version, specialTenantRule, loader);
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
            logger.warn(" >>> Threw an error in checkAndProcessDupes - just absorb this error and move on. {}", LogFormatTools.getStackTop(e));
        }

        return returnList;

    }// End of getDupeSets4DependentNodes()


    private Graph getGraphTransaction(JanusGraph janusGraph) throws AAIException {

        Graph graph = null;
        try {
            if (janusGraph == null) {
                logger.error("could not get graph object in DupeTool.  \n");
                throw new AAIException("could not get graph object in DupeTool.  \n");
            }
            graph = janusGraph.newTransaction();
            if (graph == null) {
                throw new AAIException("AAI_6101", "null graphTransaction object in DupeTool. \n");
            }

        } catch (AAIException e1) {
            logger.error(e1.getErrorObject().toString());
            throw new AAIException(e1.getErrorObject().toString());
        } catch (Exception e2) {
            logger.error(e2.toString());
            throw new AAIException(e2.toString());
        }

        return graph;

    }// End of getGraphTransaction()


    public void showNodeInfo(Vertex tVert, Boolean displayAllVidsFlag) {

        try {
            Iterator<VertexProperty<Object>> pI = tVert.properties();
            String infStr = ">>> Found Vertex with VertexId = " + tVert.id() + ", properties:    ";
            logger.info(infStr);
            while (pI.hasNext()) {
                VertexProperty<Object> tp = pI.next();
                infStr = " [" + tp.key() + "|" + tp.value() + "] ";
                logger.info(infStr);
            }

            List<String> retArr = collectEdgeInfoForNode(tVert, displayAllVidsFlag);
            for (String infoStr : retArr) {
                logger.info(infoStr);
            }
        } catch (Exception e) {
            logger.warn(" -- Error -- trying to display edge info. [{}]", e.getMessage());
        }

    }// End of showNodeInfo()


    public List<String> collectEdgeInfoForNode(Vertex tVert, boolean displayAllVidsFlag) {
        List<String> retArr = new ArrayList<>();

        for (Direction dir : new Direction[]{Direction.OUT, Direction.IN}) {
            Iterator<Edge> edgeIterator = tVert.edges(dir);

            if (!edgeIterator.hasNext()) {
                retArr.add("No " + dir + " edges were found for this vertex. ");
                continue;
            }

            while (edgeIterator.hasNext()) {
                Edge edge = edgeIterator.next();
                Vertex otherVertex = getOtherVertex(edge, dir);

                if (otherVertex == null) {
                    retArr.add(" >>> COULD NOT FIND VERTEX on the other side of this edge edgeId = %s <<< "
                            .formatted(edge.id()));
                    continue;
                }

                retArr.add(buildEdgeMessage(edge, otherVertex, dir, displayAllVidsFlag));
            }
        }

        return retArr;
    }

    private Vertex getOtherVertex(Edge edge, Direction direction) {
        return (direction == Direction.OUT) ? edge.inVertex() : edge.outVertex();
    }

    private String buildEdgeMessage(Edge edge,
                                    Vertex otherVertex,
                                    Direction direction,
                                    boolean displayAllVidsFlag) {

        String lab = edge.label();
        String nType = otherVertex.<String>property(AAI_NODE_TYPE).orElse(null);

        if (displayAllVidsFlag) {
            String vid = otherVertex.id().toString();
            return "Found an " + direction + " edge (" + lab + ") between this vertex and a ["
                    + nType + "] node with VtxId = " + vid;
        } else {
            return "Found an " + direction + " edge (" + lab + ") between this vertex and a ["
                    + nType + "] node. ";
        }
    }

    private long figureWindowStartTime(int timeWindowMinutes) {
        // Given a window size, calculate what the start-timestamp would be.

        if (timeWindowMinutes <= 0) {
            // This just means that there is no window...
            return 0;
        }
        long unixTimeNow = System.currentTimeMillis();
        long windowInMillis = (long) timeWindowMinutes * 60 * 1000;

        return unixTimeNow - windowInMillis;
    } // End of figureWindowStartTime()


    /**
     * Gets the node(s) just using key params.
     *
     * @param graph        the graph
     * @param nodeType     the node type
     * @param keyPropsHash the key props hash
     * @return the node just using key params
     * @throws AAIException the AAI exception
     */
    public List<Vertex> getNodeJustUsingKeyParams(Graph graph, String nodeType,
                                                  Map<String, Object> keyPropsHash) throws AAIException {

        ArrayList<Vertex> retVertList = new ArrayList<>();

        if (keyPropsHash == null || keyPropsHash.isEmpty()) {
            throw new AAIException("AAI_6120", "No key properties passed for this getNodeJustUsingKeyParams() request. NodeType = [" + nodeType + "].");
        }

        int idx = -1;
        ArrayList<String> kName = new ArrayList<>();
        ArrayList<Object> kVal = new ArrayList<>();
        for (Map.Entry<String, Object> entry : keyPropsHash.entrySet()) {
            idx++;
            kName.add(idx, entry.getKey());
            kVal.add(idx, entry.getValue());
        }
        int topPropIndex = idx;

        GraphTraversalSource g = graph.traversal();
        List<Vertex> verts = new ArrayList<>();

        try {
            switch (topPropIndex) {
                case 1 -> { // only ID
                    verts = g.V()
                            .has(kName.get(0), kVal.get(0))
                            .has(AAI_NODE_TYPE, nodeType)
                            .limit(50)
                            .toList();
                }

                case 2 -> { // ID + Name

                    List<Vertex> vertList1 = g.V()
                            .has(kName.get(0), kVal.get(0))
                            .has(AAI_NODE_TYPE, nodeType)
                            .limit(50)
                            .toList();

                    List<Vertex> vertList2 = g.V()
                            .has(kName.get(1), kVal.get(1))
                            .has(AAI_NODE_TYPE, nodeType)
                            .limit(50)
                            .toList();

                    //  Build a set of existing vertex IDs for deduplication
                    Set<Object> vert1Ids = vertList1.stream()
                            .map(Vertex::id)
                            .collect(Collectors.toSet());

                    for (Vertex v : vertList2) {
                        String id = g.V(v.id()).values(kName.get(0)).toString(); // unique id of current vertex
                        // Checking if vertex ids fetched by name are present in vert1Ids(fetched by id)
                        // & current vertex has same unique id as other vertex which was added in vert1Ids
                        // We want to confirm if 2 objects match by name they should also have same ids
                        if (!vert1Ids.contains(v.id()) && id == kVal.get(0)) {
                            vertList1.add(v);
                        }
                    }

                    verts.addAll(vertList1);
                }

                default -> { // More than 2 keys (rare)
                    GraphTraversal<Vertex, Vertex> traversal = g.V();
                    for (int i = 0; i < topPropIndex; i++) {
                        traversal = traversal.has(kName.get(i), kVal.get(i));
                    }
                    traversal = traversal.has(AAI_NODE_TYPE, nodeType);
                    verts = traversal.limit(50).toList();
                }
            }

        } catch (Exception ex) {
            logger.error("Error trying to get node for [{}]: {}", nodeType, ex.getMessage());
            throw new AAIException(String.format("Error trying to get node for [%s]: %s", nodeType, ex.getMessage()));
        }

        if (verts.isEmpty()) {
            logger.debug("No node found for nodeType = [{}], keys = {}", nodeType, kName);
        }

        retVertList.addAll(verts);
        return retVertList;
    }// End of getNodeJustUsingKeyParams()


    /**
     * Gets the node(s) just using key params.
     *
     * @param graph           the graph
     * @param nodeType        the node type
     * @param windowStartTime the window start time
     * @param propsString     the props hash
     * @return the nodes
     * @throws AAIException the AAI exception
     */
    public List<Vertex> figureOutNodesToCheck(Graph graph, String nodeType, long windowStartTime,
                                              String propsString) throws AAIException {

        GraphTraversal<Vertex, Vertex> tgQ = graph.traversal().V().has(AAI_NODE_TYPE, nodeType);
        StringBuilder qStringForMsg = new StringBuilder("graph.traversal().V().has(\"aai-node-type\"," + nodeType + ")");

        if (propsString != null && !propsString.trim().isEmpty()) {
            propsString = propsString.trim();
            int firstPipeLoc = propsString.indexOf("|");
            if (firstPipeLoc <= 0) {
                logger.error("Bad props4Collect passed: [{}].  \n Expecting a format like, 'propName1|propVal1,propName2|propVal2'", propsString);
                throw new AAIException("Bad props4Collect passed: [{}].  \n Expecting a format like, 'propName1|propVal1,propName2|propVal2'", propsString);
            }

            // Note - if they're only passing on parameter, there won't be any commas
            String[] paramArr = propsString.split(",");
            for (String s : paramArr) {
                int pipeLoc = s.indexOf("|");
                if (pipeLoc <= 0) {
                    logger.error("Bad propsString passed: [{}].  \n Expecting a format like, 'propName1|propVal1,propName2|propVal2'", propsString);
                    throw new AAIException("Bad propsString passed: [{}].  \n Expecting a format like, 'propName1|propVal1,propName2|propVal2'", propsString);
                } else {
                    String propName = s.substring(0, pipeLoc);
                    String propVal = s.substring(pipeLoc + 1);
                    tgQ = tgQ.has(propName, propVal);
                    qStringForMsg.append(".has(").append(propName).append(",").append(propVal).append(")");
                }
            }
        }
        ArrayList<Vertex> retVertList = new ArrayList<>();
        if (tgQ == null) {
            logger.error("Bad JanusGraphQuery object.  ");
            throw new AAIException("Bad JanusGraphQuery object.  ");
        } else {
            while (tgQ.hasNext()) {
                Vertex tiV = tgQ.next();
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

        if (retVertList.isEmpty() && logger.isDebugEnabled())
            logger.debug("DEBUG No node found for: [%s, with aai-created-ts > %d".formatted(qStringForMsg, windowStartTime));


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
     * @return Vertex
     * @throws AAIException the AAI exception
     */
    public Vertex getPreferredDupe(String transId,
                                   String fromAppId, Graph g,
                                   List<Vertex> dupeVertexList, String ver,
                                   Boolean specialTenantRule, Loader loader)
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
        Object uriOb = dupeVertexList.get(0).<Object>property(AAI_URI).orElse(null);
        if (uriOb == null || uriOb.toString().isEmpty()) {
            // this is a bad node - hopefully will be picked up by phantom checker
            return nullVtx;
        }
        String thisUri = uriOb.toString();
        for (int i = 1; i < listSize; i++) {
            uriOb = dupeVertexList.get(i).<Object>property(AAI_URI).orElse(null);
            if (uriOb == null || uriOb.toString().isEmpty()) {
                // this is a bad node - hopefully will be picked up by phantom checker
                return nullVtx;
            }
            String nextUri = uriOb.toString();
            if (!thisUri.equals(nextUri)) {
                // there are different URI's on these - so we can't pick
                // a dupe to keep.  Someone will need to look at it.
                return nullVtx;
            }
        }

        Vertex vtxPreferred = null;
        Vertex currentFaveVtx = dupeVertexList.get(0);
        for (int i = 1; i < listSize; i++) {
            Vertex vtxB = dupeVertexList.get(i);
            vtxPreferred = pickOneOfTwoDupes(gts,
                    currentFaveVtx, vtxB, specialTenantRule, loader);
            if (vtxPreferred == null) {
                // We couldn't choose one
                return nullVtx;
            } else {
                currentFaveVtx = vtxPreferred;
            }
        }

        if (currentFaveVtx != null && checkAaiUriOk(gts, currentFaveVtx)) {
            return (currentFaveVtx);
        } else {
            // We had a preferred vertex, but its aai-uri was bad, so
            // we will not recommend one to keep.
            return nullVtx;
        }

    } // end of getPreferredDupe()


    /**
     * Pick one of two dupes.
     *
     * @param gts                   the graphTraversalSource
     * @param vtxA                  the vtx A
     * @param vtxB                  the vtx B
     * @param specialTenantRule     specialTenantRuleFlag flag
     * @param loader                the loader
     * @return Vertex
     * @throws AAIException the AAI exception
     */
    public Vertex pickOneOfTwoDupes(GraphTraversalSource gts, Vertex vtxA,
                                    Vertex vtxB, Boolean specialTenantRule, Loader loader) throws AAIException {

        Vertex nullVtx = null;
        Vertex preferredVtx = null;

        Long vidA = Long.valueOf(vtxA.id().toString());
        Long vidB = Long.valueOf(vtxB.id().toString());

        String vtxANodeType = "";
        String vtxBNodeType = "";
        Object obj = vtxA.<Object>property(AAI_NODE_TYPE).orElse(null);
        if (obj != null) {
            vtxANodeType = obj.toString();
        }
        obj = vtxB.<Object>property(AAI_NODE_TYPE).orElse(null);
        if (obj != null) {
            vtxBNodeType = obj.toString();
        }

        if (vtxANodeType.isEmpty() || (!vtxANodeType.equals(vtxBNodeType))) {
            // Either they're not really dupes or there's some bad data - so
            // don't pick one
            return nullVtx;
        }

        // Check that node A and B both have the same key values (or else they
        // are not dupes)
        // (We'll check dep-node later)
        Collection<String> keyProps = loader.introspectorFromName(vtxANodeType).getKeys();
        for (String propName : keyProps) {
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

            if (vtxAKeyPropVal.isEmpty()
                    || (!vtxAKeyPropVal.equals(vtxBKeyPropVal))) {
                // Either they're not really dupes or they are missing some key
                // data - so don't pick one
                return nullVtx;
            }
        }

        // Collect the vid's and aai-node-types of the vertices that each vertex
        // (A and B) is connected to.
        ArrayList<String> vtxIdsConnToA = new ArrayList<>();
        ArrayList<String> vtxIdsConnToB = new ArrayList<>();
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
                obj = tmpVtx.<Object>property(AAI_NODE_TYPE).orElse(null);
                if (obj != null) {
                    nt = obj.toString();
                }
                nodeTypesConn2A.put(nt, conVid);
                vtxIdsConnToA.add(conVid);
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
                obj = tmpVtx.<Object>property(AAI_NODE_TYPE).orElse(null);
                if (obj != null) {
                    nt = obj.toString();
                }
                nodeTypesConn2B.put(nt, conVid);
                vtxIdsConnToB.add(conVid);
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

        if (vtxIdsConnToA.size() == vtxIdsConnToB.size()) {
            // 2 - If they both have edges to all the same vertices, then return
            // the one with the lower vertexId.

            // OR (2b)-- if this is the SPECIAL case -- of
            //  "tenant|vserver vs. tenant|service-subscription"
            //   then we pick/prefer the one that's connected to
            //   the service-subscription.  AAI-8172
            boolean allTheSame = true;
            Iterator<String> iter = vtxIdsConnToA.iterator();
            while (iter.hasNext()) {
                String vtxIdConn2A = iter.next();
                if (!vtxIdsConnToB.contains(vtxIdConn2A)) {
                    allTheSame = false;
                    break;
                }
            }

            if (allTheSame) {
                if (Boolean.TRUE.equals(checkAaiUriOk(gts, vtxA))) {
                    preferredVtx = vtxA;
                } else if (Boolean.TRUE.equals(checkAaiUriOk(gts, vtxB))) {
                    preferredVtx = vtxB;
                }
                // else we're picking neither because neither one had a working aai-uri index property
            } else if (Boolean.TRUE.equals(specialTenantRule) && vtxIdsConnToA.size() == 2 && vtxANodeType.equals("tenant")) {
                // We're dealing with two tenant nodes which each just have
                // two connections.  One must be the parent (cloud-region)
                // which we check in step 1 above.   If one connects to
                // a vserver and the other connects to a service-subscription,
                // our special rule is to keep the one connected
                // to the
                if (nodeTypesConn2A.containsKey("vserver") && nodeTypesConn2B.containsKey("service-subscription")) {
                    logger.info(" WARNING >>> we are using the special tenant rule to choose to " +
                            " delete tenant vtxId = {}, and keep tenant vtxId = {}", vidA, vidB);
                    preferredVtx = vtxB;
                } else if (nodeTypesConn2B.containsKey("vserver") && nodeTypesConn2A.containsKey("service-subscription")) {
                    logger.info(" WARNING >>> we are using the special tenant rule to choose to " +
                            " delete tenant vtxId = {}, and keep tenant vtxId = {}", vidB, vidA);
                    preferredVtx = vtxA;
                }
            }

        } else if (vtxIdsConnToA.size() > vtxIdsConnToB.size()) {
            // 3 - VertexA is connected to more things than vtxB.
            // We'll pick VtxA if its edges are a superset of vtxB's edges.
            boolean missingOne = false;
            for (String vtxIdConn2B : vtxIdsConnToB) {
                if (!vtxIdsConnToA.contains(vtxIdConn2B)) {
                    missingOne = true;
                    break;
                }
            }
            if (!missingOne) {
                preferredVtx = vtxA;
            }
        } else {
            // 4 - VertexB is connected to more things than vtxA.
            // We'll pick VtxB if its edges are a superset of vtxA's edges.
            boolean missingOne = false;
            for (String vtxIdConn2A : vtxIdsConnToA) {
                if (!vtxIdsConnToB.contains(vtxIdConn2A)) {
                    missingOne = true;
                    break;
                }
            }
            if (!missingOne) {
                preferredVtx = vtxB;
            }
        }

        return (preferredVtx);

    } // end of pickOneOfTwoDupes()


    /**
     * Group verts by dep nodes.
     *
     * @param g              the g
     * @param passedVertList the passed vert list
     * @return the hash map
     */
    private Map<String, ArrayList<Vertex>> groupVertsByDepNodes(
            Graph g,
            ArrayList<Vertex> passedVertList) {

        // Given a list of JanusGraph Vertices, group them together by dependent
        // nodes. Ie. if given a list of ip address nodes (assumed to all
        // have the same key info) they might sit under several different
        // parent vertices.
        // Under Normal conditions, there would only be one per parent -- but
        // we're trying to find duplicates - so we allow for the case
        // where more than one is under the same parent node.

        HashMap<String, ArrayList<Vertex>> retHash = new HashMap<>();
        GraphTraversalSource gts = g.traversal();
        if (passedVertList != null) {
            for (Vertex thisVert : passedVertList) { //vertex
                Vertex parentVtx = getConnectedParent(gts, thisVert);
                if (parentVtx != null) {
                    String parentVid = parentVtx.id().toString();
                    if (retHash.containsKey(parentVid)) {
                        // add this vert to the list for this parent key
                        retHash.get(parentVid).add(thisVert);
                    } else {
                        // This is the first one we found on this parent
                        ArrayList<Vertex> vList = new ArrayList<>();
                        vList.add(thisVert);
                        retHash.put(parentVid, vList); //parentVid,vertex
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
     * @return the boolean
     */
    private Boolean deleteNonKeepers(Graph g,
                                     ArrayList<String> dupeInfoList) throws AAIException {

        // This assumes that each dupeInfoString is in the format of
        // pipe-delimited vid's followed by either "keepVid=xyz" or "keepVid=UNDETERMINED"
        // ie. "3456|9880|keepVid=3456"

        boolean didADelFlag = false;
        for (String dupeInfoString : dupeInfoList) {
            didADelFlag |= deleteNonKeeperForOneSet(g, dupeInfoString);
        }

        return didADelFlag;

    }// end of deleteNonKeepers()


    /**
     * Delete non keepers if appropriate.
     *
     * @param g                 the g
     * @param dupeInfoString    the dupe string
     * @return the boolean
     */
    private Boolean deleteNonKeeperForOneSet(Graph g,
                                             String dupeInfoString) throws AAIException {

        boolean deletedSomething = false;
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
                    if (prefArr.length != 2 || (!prefArr[0].equals(KEEP_VID))) {
                        logger.error("Bad format. Expecting KeepVid=999999");
                        return false;
                    } else {
                        String keepVidStr = prefArr[1];
                        if (idArr.contains(keepVidStr)) {
                            idArr.remove(keepVidStr);
                            // So now, the idArr should just contain the vid's
                            // that we want to remove.
                            for (String s : idArr) {
                                boolean okFlag = true;
                                String thisVid = s;
                                try {
                                    long longVertId = Long.parseLong(thisVid);
                                    Vertex vtx = g.traversal().V(longVertId).next();
                                    logger.info("--->>>   We will delete node with VID = {} <<<---", thisVid);
                                    vtx.remove(); // this will finally delete the duplicate vertex
                                } catch (Exception e) {
                                    okFlag = false;
                                    logger.error("ERROR trying to delete VID = {}, [" + e + "]", thisVid);
                                    throw new AAIException("ERROR trying to delete VID = " + thisVid + ", [" + e + "]");
                                }
                                if (okFlag) {
                                    logger.info(" DELETED VID = {}", thisVid);
                                    deletedSomething = true;
                                }
                            }
                        } else {
                            logger.error("ERROR - Vertex Id to keep not found in list of dupes.  dupeInfoString = [{}]", dupeInfoString);
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
     * @return a hashMap of the propertyNames/values
     */
    private HashMap<String, Object> getNodeKeyVals(Vertex tvx,
                                                   ArrayList<String> keyPropNamesArr, String nameProp, String namePropVal) {

        HashMap<String, Object> retHash = new HashMap<>();
        for (String propName : keyPropNamesArr) {
            if (tvx != null) {
                Object propValObj = tvx.property(propName).orElse(null);
                retHash.put(propName, propValObj); // id, val
                if (null != nameProp) {
                    retHash.put(nameProp, namePropVal); // name, val
                }
            }
        }
        return retHash;

    }// End of getNodeKeyVals()


    /**
     * makes sure aai-uri exists and can be used to get this node back
     *
     * @param graph the graph
     * @param origVtx
     * @return true if aai-uri is populated and the aai-uri-index points to this vtx
     * @throws AAIException the AAI exception
     */
    private Boolean checkAaiUriOk(GraphTraversalSource graph, Vertex origVtx) throws AAIException {
        String aaiUriStr = "";
        try {
            Object ob = origVtx.<Object>property(AAI_URI).orElse(null);
            String origVid = origVtx.id().toString();
            if (ob == null || ob.toString().isEmpty()) {
                // It is missing its aai-uri
                if (logger.isDebugEnabled())
                    logger.debug("DEBUG No [aai-uri] property found for vid = [%s] ".formatted(origVid));
                return false;
            } else {
                aaiUriStr = ob.toString();
                Iterator<Vertex> verts = graph.V().has(AAI_URI, aaiUriStr);
                int count = 0;
                while (verts.hasNext()) {
                    count++;
                    Vertex foundV = verts.next();
                    String foundVid = foundV.id().toString();
                    if (!origVid.equals(foundVid)) {
                        if (logger.isDebugEnabled())
                            logger.debug("DEBUG aai-uri key property [%s] for vid = [%s] brought back different vertex with vid = [%s].".formatted(aaiUriStr, origVid, foundVid));
                        return false;
                    }
                }
                if (count == 0) {
                    if (logger.isDebugEnabled())
                        logger.debug("DEBUG aai-uri key property [%s] for vid = [%s] could not be used to query for that vertex. ".formatted(aaiUriStr, origVid));
                    return false;
                } else if (count > 1) {
                    if (logger.isDebugEnabled())
                        logger.debug("DEBUG aai-uri key property [%s] for vid = [%s] brought back multiple (%d) vertices instead of just one. ".formatted(aaiUriStr, origVid, count));
                    return false;
                }
            }
        } catch (Exception ex) {
            logger.error(" ERROR trying to get node with aai-uri: [" + aaiUriStr + "]" + LogFormatTools.getStackTop(ex));
            throw new AAIException(" ERROR trying to get node with aai-uri: [" + aaiUriStr + "]" + LogFormatTools.getStackTop(ex));
        }
        return true;

    }// End of checkAaiUriOk()


    /**
     * Get values of the key properties for a node as a single string
     *
     * @param tvx              the vertex to pull the properties from
     * @param keyPropNamesArr  collection of key prop names
     * @return a String of concatenated values
     */
    private String getNodeKeyValString(Vertex tvx,
                                       ArrayList<String> keyPropNamesArr) {

        // -- NOTE -- for what we're using this for, we would need to
        // guarantee that the properties are always in the same order

        StringBuilder retString = new StringBuilder();
        for (String propName : keyPropNamesArr) {
            if (tvx != null) {
                Object propValObj = tvx.property(propName).orElse(null);
                retString = new StringBuilder(" " + retString + propValObj.toString());
            }
        }
        return retString.toString();

    }// End of getNodeKeyValString()


    /**
     * Find duplicate sets from two dupe runs.
     *
     * @param firstPassDupeSets  from the first pass
     * @param secondPassDupeSets from the second pass
     * @return commonDupeSets that are common to both passes and have a determined keeper
     */
    private ArrayList<String> figureWhichDupesStillNeedFixing(ArrayList<String> firstPassDupeSets,
                                                              ArrayList<String> secondPassDupeSets) {

        ArrayList<String> commonToBothSet = new ArrayList<>();

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
            return commonToBothSet;
        }

        boolean needToParse = false;
        StringBuilder secondPassDupes = new StringBuilder();
        for (String secondPassDupeSet : secondPassDupeSets) {
            secondPassDupes.append("[").append(secondPassDupeSet).append("] ");
            String secPassDupeSetStr = secondPassDupeSet;
            if (secPassDupeSetStr.endsWith("UNDETERMINED")) {
                // This is a set of dupes where we could not pick one
                // to delete - so don't include it on our list for
                // fixing.
            } else if (firstPassDupeSets.contains(secPassDupeSetStr)) {
                // We have lucked out and do not even need to parse this since
                // it was in the other array with any dupes listed in the same order
                // This is actually the most common scenario since there is
                // usually only one dupe, so order doesn't matter.
                commonToBothSet.add(secPassDupeSetStr);
            } else {
                // We'll need to do some parsing to check this one
                needToParse = true;
            }
        }

        if (needToParse) {
            // Make a hash from the first and second Pass data
            //	where the key is the vid to KEEP and the value is an
            // 	array of (String) vids that would get deleted.
            Map<String, ArrayList<String>> firstPassHash = makeKeeperHashOfDupeStrings(firstPassDupeSets, commonToBothSet);

            Map<String, ArrayList<String>> secPassHash = makeKeeperHashOfDupeStrings(secondPassDupeSets, commonToBothSet);

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
                    for (String s : secList) {
                        if (!firstList.contains(s)) {
                            // The first pass did not think this needed to be deleted
                            skipThisOne = true;
                            break;
                        }
                    }
                }
                if (!skipThisOne) {
                    // Put the string back together and pass it back
                    // Not beautiful, but no time to make it nice right now...
                    // Put it back in the format: "3456|9880|keepVid=3456"
                    StringBuilder thisDelSetStr = new StringBuilder();
                    for (int z = 0; z < secList.size(); z++) {
                        if (z == 0) {
                            thisDelSetStr = new StringBuilder(secList.get(z));
                        } else {
                            thisDelSetStr = new StringBuilder(thisDelSetStr + "|" + secList.get(z));
                        }
                    }
                    thisDelSetStr = new StringBuilder(thisDelSetStr + "|keepVid=" + secKey);
                    commonToBothSet.add(thisDelSetStr.toString());
                }
            }

        }
        return commonToBothSet;

    }// figureWhichDupesStillNeedFixing


    private Map<String, ArrayList<String>> makeKeeperHashOfDupeStrings(ArrayList<String> dupeSets,
                                                                           ArrayList<String> excludeSets) {

        HashMap<String, ArrayList<String>> keeperHash = new HashMap<>();

        for (String tmpSetStr : dupeSets) {
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
                                || (!prefArr[0].equals(KEEP_VID))) {
                            logger.info("Bad format in figureWhichDupesStillNeedFixing(). Expecting " +
                                    " KeepVid=999999 but string looks like: [{}]", tmpSetStr);
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
     * @return void
     */
    private void showNodeDetailsForADupeSet(Graph g, String dupeInfoString) {

        // dang...   parsing this string once again...

        String[] dupeArr = dupeInfoString.split("\\|");
        int lastIndex = dupeArr.length - 1;
        for (int i = 0; i <= lastIndex; i++) {
            if (i < lastIndex) {
                // This is not the last entry, it is one of the dupes,
                String vidString = dupeArr[i];
                long longVertId = Long.parseLong(vidString);
                Vertex vtx = g.traversal().V(longVertId).next();
                showNodeInfo(vtx, false);
            } else {
                // This is the last entry which should tell us if we have a
                // preferred keeper
                String prefString = dupeArr[i];
                if (prefString.equals("KeepVid=UNDETERMINED")) {
                    logger.info(" Our algorithm cannot choose from among these, so they will all be kept. -------\n");
                } else {
                    // If we know which to keep, then the prefString should look
                    // like, "KeepVid=12345"
                    String[] prefArr = prefString.split("=");
                    if (prefArr.length != 2 || (!prefArr[0].equals(KEEP_VID))) {
                        logger.error("Bad format. Expecting KeepVid=999999");
                        throw new ValidationException("Bad format. Expecting KeepVid=999999");
                    } else {
                        String keepVidStr = prefArr[1];
                        logger.info(" vid = {} is the one that we would KEEP. ------\n", keepVidStr);
                    }
                }
            }
        }

    }// End of showNodeDetailsForADupeSet()

    private int graphIndex = 1;

    public JanusGraph setupGraph(Logger logger) throws AAIException {

        JanusGraph janusGraph = null;


        try (InputStream inputStream = new FileInputStream(AAIConstants.REALTIME_DB_CONFIG);) {

            Properties properties = new Properties();
            properties.load(inputStream);

            if (INMEMORY.equals(properties.get("storage.backend"))) {
                janusGraph = AAIGraph.getInstance().getGraph();
                graphType = INMEMORY;
            } else {
                janusGraph = JanusGraphFactory.open(new AAIGraphConfig.Builder(AAIConstants.REALTIME_DB_CONFIG).forService(DupeTool.class.getSimpleName()).withGraphType("realtime" + graphIndex).buildConfiguration());
                graphIndex++;
            }
        } catch (Exception e) {
            logger.error("Unable to open the graph", e);
            throw new AAIException(e.getMessage());
        }

        return janusGraph;
    }

    public void closeGraph(JanusGraph graph, Logger logger) {

        try {
            if (INMEMORY.equals(graphType)) {
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
