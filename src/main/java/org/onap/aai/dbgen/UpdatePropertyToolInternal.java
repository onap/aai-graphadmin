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
import com.beust.jcommander.JCommander;
import com.beust.jcommander.Parameter;
import org.apache.tinkerpop.gremlin.process.traversal.dsl.graph.GraphTraversal;
import org.apache.tinkerpop.gremlin.process.traversal.dsl.graph.GraphTraversalSource;
import org.apache.tinkerpop.gremlin.structure.Vertex;
import org.janusgraph.core.JanusGraph;
import org.janusgraph.core.JanusGraphFactory;
import org.janusgraph.core.JanusGraphTransaction;
import org.onap.aai.dbmap.AAIGraphConfig;
import org.onap.aai.exceptions.AAIException;
import org.onap.aai.util.AAIConfig;

import java.io.BufferedReader;
import java.io.FileReader;
import java.io.IOException;
import java.util.ArrayList;
import java.util.List;

public class UpdatePropertyToolInternal {
    private final Logger LOGGER;

    @Parameter(names = {"--filename", "-f"}, description = "Enter path to the file name containing a list of your vertex IDs separated by new lines")
    String filename;
    @Parameter(names = {"--vertexId", "-v"}, description = "Vertex id(s) used to update the node(s). Flag can be used multiple times to enter a list of vertex ids")
    private List<String> vertexIds = new ArrayList<>();
    @Parameter(names = {"--property", "-p"}, description = "Property to be updated within the given node(s)", required = true)
    String property;
    @Parameter(names = {"-h", "--help"},
            description = "Help/Usage",
            help = true)
    boolean help;

    public UpdatePropertyToolInternal(){
        LOGGER = LoggerFactory.getLogger(UpdatePropertyToolInternal.class.getSimpleName());
    }

    /**
     * Run method for the main method of the update property tool. Returns boolean to indicate success or failure
     * @param graph
     * @param args
     * @return
     */
    public boolean run(JanusGraph graph, String[] args) {
        logAndPrint("===============Start UpdatePropertyTool===============");
        boolean isSuccessful = true;
        try {
            // Error check args
            if (!processCommandLineArguments(args)) {
                isSuccessful = false;
            }

            // Aggregate all ids into one list
            List<String> vertexIdList = aggregateVertexIdList(filename, vertexIds);

            // Attempt update on given vertex ids
            if (!processUpdateTransaction(graph, vertexIdList, property)) {
                isSuccessful = false;
            }

        } catch (Exception e) {
            isSuccessful = false;
            logErrorAndPrint("ERROR exception thrown in run() method of UpdatePropertyTool", e);
        }
        logAndPrint("===============End UpdatePropertyTool===============");
        return isSuccessful;
    }

    /**
     * Use JCommander to process the provided command-line arguments.
     * This method is an instance method (not static) to allow JCommander
     * to use this instance's JCommander-annotated fields.
     *
     * @param args Command-line arguments.
     */
    private boolean processCommandLineArguments(final String[] args) {
        logAndPrint("Start of processCommandLineArguments()");
        final JCommander commander = new JCommander();
        commander.addObject(this);
        commander.setVerbose(1);
        commander.parse(args);
        commander.setProgramName(UpdatePropertyTool.class.getSimpleName());
        boolean filenameExists = false;
        boolean vertexIdExists = false;
        boolean isValidArgs = true;

        // check for help flag
        if (help) {
            commander.usage();
        }

        // Check for property field
        if (property != null && !property.isEmpty()) {
            logAndPrint("The property provided is: " + property);
        } else {
            logAndPrint("ERROR: No property argument was entered. Property argument is required.");
            isValidArgs = false;
        }

        // Check for file name
        if (filename != null && !filename.isEmpty()) {
            logAndPrint("The filename provided is: " + filename);
            filenameExists = true;
        }

        // Check for vertex Ids
        if (vertexIds != null && !vertexIds.isEmpty()) {
            logAndPrint("The vertex id(s) provided: ".concat(String.join("|", vertexIds)));
            vertexIdExists = true;
        }

        // Fail and exit if there are no vertexIds to work with
        if (!filenameExists && !vertexIdExists) {
            isValidArgs = false;
            logAndPrint("ERROR: Cannot execute UpdatePropertyTools without any given vertex Ids.");
        }
        logAndPrint("End of processCommandLineArguments()");

        return isValidArgs;
    }

    /**
     * Executes Gremlin queries to obtain vertices by their ids and updates the property defined by the given property parameter
     * Reads a list of vertex IDs, and uses the prop parameter to indicate which property has corrupt index
     * Uses gremlin console to get the property value and use that value returned to set the property and update. 
     *
     * @param vIdList
     * @param propertyKey    property value to be updated
     */
    private boolean processUpdateTransaction(JanusGraph graph, List<String> vIdList, String propertyKey) {
        logAndPrint("Start of processUpdateTransaction()");
        boolean isValidTransaction = true;

        if (graph == null) {
            logAndPrint("JanusGraph graph object is null. Stopping processUpdateTransaction()");
            return false;
        }

        if (vIdList == null || vIdList.isEmpty()) {
            logAndPrint("Vertex Id list is null or empty. Stopping processUpdateTransaction()");
            return false;
        }

        if (propertyKey == null || propertyKey.isEmpty()) {
            logAndPrint("propertyKey is null or empty. Stopping processUpdateTransaction()");
            return false;
        }

        // if AAIConfig.init() fails, exit application
        if (!setUpAAIConfig(graph)) {
            isValidTransaction = false;
        }

        // Obtain the vertex objects using the given vertex ids
        JanusGraphTransaction transaction = graph.newTransaction();

        try {
            GraphTraversalSource g = transaction.traversal();
            boolean isCommitUpdateValid = false;
            for (String vertexId: vIdList) {
                /*
                 * Query the vertex using the vertex id from the graph
                 * Check if the query obtained a vertex
                 * Get the property value from the vertex itself
                 * Update the property using the value obtained from the query
                 */
                GraphTraversal<Vertex, Vertex> query = g.V(vertexId);
                if (query.hasNext()) {
                    Vertex vertex = query.next();
                    Object propertyValue = vertex.property(propertyKey).orElse(null);
                    if (propertyValue != null) {
                        vertex.property(propertyKey, propertyValue);
                        isCommitUpdateValid = true;
                        logAndPrint("Updated vertex with property: '" + propertyKey + "'. With value of: '" + propertyValue.toString() + "'");
                    } else {
                        logAndPrint("Could not update the value for property '" + propertyKey + "'. Value was empty.");
                    }
                } else {
                    logAndPrint("Vertex not found for id: " + vertexId);
                }
            }

            // If a transaction to update a property has occurred, commit the transaction(s)
            if (isCommitUpdateValid) {
                transaction.commit();
                logAndPrint("Successful update transaction has occurred. Committing update to graph.");
            } else {
                transaction.rollback();
                logAndPrint("Unsuccessful update transaction. Rolling back graph");
            }
        } catch (Exception e) {
            logErrorAndPrint("ERROR: Could not properly query and update vertex.", e);
            if (transaction != null) {
                transaction.rollback();
            } else {
                logAndPrint("ERROR: JanusGraphTransaction object is null");
            }
            isValidTransaction = false;
        } finally {
            // close the transaction -- note: JanusGraph graph object will be closed in the main method.
            if (transaction != null) {
                transaction.close();
            } else {
                logAndPrint("ERROR: JanusGraphTransaction object is null. Cannot close the transaction.");
            }
        }

        logAndPrint("End of processUpdateTransaction()");
        return isValidTransaction;
    }

    /**
     * Combine the vertex ids from the file and list of ids given
     * @param filePath
     * @param vertexIds
     * @return
     */
    private List<String> aggregateVertexIdList(String filePath, List<String> vertexIds) {
        List<String> allVertexIds = new ArrayList<>();

        if (filePath != null && !filePath.isEmpty()) {
            // Add vertex Ids listed from the given file name
            try(BufferedReader br = new BufferedReader(new FileReader(filePath))) {
                logAndPrint("Loading file at: " + filePath);

                StringBuilder sb = new StringBuilder();
                String nextLine = br.readLine();

                while (nextLine != null) {
                    if (!nextLine.matches("[0-9]+")) {
                        logAndPrint("Invalid vertex id: " + nextLine);
                        continue;
                    }
                    allVertexIds.add(nextLine);
                    sb.append(nextLine);
                    sb.append(System.lineSeparator());
                    nextLine = br.readLine();
                }
                String allVertexIdsString = sb.toString();
                logAndPrint("All vertex IDs from file " + filePath + ":\n" + allVertexIdsString);
            } catch (IOException ioe) {
                logErrorAndPrint("ERROR reading in text file failed.", ioe);
            }
        }

        // Add all vertex Ids input one at a time from args parameter
        StringBuilder sb = new StringBuilder();
        for (String vId : vertexIds) {
            if (!vId.matches("[0-9]+")) {
                logAndPrint("Invalid vertex id: " + vId);
                continue;
            }
            allVertexIds.add(vId);
            sb.append(vId);
            sb.append(System.lineSeparator());
        }
        logAndPrint("Vertex IDs from --vertexId args:\n" + sb.toString());

        return allVertexIds;
    }

    /**
     * Set up AAIConfig object
     *
     * @return
     */
    private boolean setUpAAIConfig(JanusGraph graph) {
        String msg = "";
        try {
            AAIConfig.init();
            if (graph == null) {
                String emsg = "Graph is null. Could not get graph object. \n";
                logAndPrint(emsg);
                return false;
            }
        } catch (AAIException e1) {
            msg = e1.getErrorObject().toString();
            logErrorAndPrint("ERROR: AAIConfig set up failed. ", e1);
            return false;
        } catch (Exception e2) {
            msg = e2.toString();
            logErrorAndPrint("ERROR: AAIConfig set up failed. ", e2);
            return false;
        }
        return true;
    }

    /**
     * Set up and return and open JanusGraph Object
     *
     * @return
     */
    public JanusGraph openGraph(String configPath) {
        logAndPrint("Setting up Janus Graph...");
        JanusGraph janusGraph = null;

        try {
            janusGraph = JanusGraphFactory.open(
                    new AAIGraphConfig.Builder(configPath)
                            .forService(UpdatePropertyTool.class.getSimpleName())
                            .withGraphType("AAITools-" + UpdatePropertyTool.class.getSimpleName())
                            .buildConfiguration()
            );
        } catch (Exception e) {
            logErrorAndPrint("Unable to open the graph. ", e);
        }

        return janusGraph;
    }

    /**
     * Closes the given JanusGraph object
     *
     * @param graph
     */
    public void closeGraph(JanusGraph graph) {

        try {
            if (graph != null && graph.isOpen()) {
                graph.tx().close();
                graph.close();
            }
        } catch (Exception ex) {
            // Don't throw anything because JanusGraph sometimes is just saying that the graph is already closed
            logErrorAndPrint("WARNING from final graph.shutdown(): ", ex);
        }
    }

    /**
     * Log and print.
     *
     * @param msg the msg
     */
    protected void logAndPrint(String msg) {
        System.out.println(msg);
        LOGGER.error(msg);
    }

    /**
     * Log error and print.
     *
     * @param msg the msg
     */
    protected void logErrorAndPrint(String msg, Exception e) {
        System.out.println(msg);
        System.out.println(e.getCause() + " - " + e.getMessage());
        LOGGER.error(msg, e);
    }

    /**
     * Log error and print.
     *
     * @param msg the msg
     */
    protected void logErrorAndPrint(String msg) {
        System.out.println(msg);
        LOGGER.error(msg);
    }
}
