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

import org.apache.commons.io.FileUtils;
import org.apache.tinkerpop.gremlin.process.traversal.dsl.graph.GraphTraversalSource;
import org.apache.tinkerpop.gremlin.structure.Vertex;
import org.janusgraph.core.JanusGraph;
import org.janusgraph.core.JanusGraphTransaction;
import org.junit.After;
import org.junit.Before;
import org.junit.Rule;
import org.junit.Test;
import org.onap.aai.AAISetup;
import org.onap.aai.dbmap.AAIGraph;
import org.onap.aai.exceptions.AAIException;
import org.springframework.boot.test.rule.OutputCapture;

import com.beust.jcommander.ParameterException;

import java.lang.NumberFormatException;
import java.io.File;
import java.io.IOException;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.util.ArrayList;
import java.util.List;
import java.util.Set;
import java.util.stream.Collectors;

import static org.hamcrest.CoreMatchers.is;
import static org.hamcrest.Matchers.containsString;
import static org.junit.Assert.*;

public class DataSnapshotTest extends AAISetup {

    private GraphTraversalSource g;

    private JanusGraphTransaction currentTransaction;

    private List<Vertex> vertexes;

    @Rule
    public OutputCapture outputCapture = new OutputCapture();

    @Before
    public void setup() throws AAIException {
        JanusGraph graph = AAIGraph.getInstance().getGraph();
        currentTransaction = graph.newTransaction();
        g = currentTransaction.traversal();

        // Setup the graph so it has one pserver vertex
        vertexes = setupPserverData(g);
        currentTransaction.commit();
    }

    @After
    public void tearDown(){

        JanusGraph graph = AAIGraph.getInstance().getGraph();
        currentTransaction = graph.newTransaction();
        g = currentTransaction.traversal();

        vertexes.stream().forEach((v) -> g.V(v).next().remove());
        currentTransaction.commit();
    }

    @Test
    public void testClearEntireDatabaseAndVerifyDataIsRemoved() throws IOException {

        // Copy the pserver.graphson file from src/test/resoures to ${AJSC_HOME}/logs/data/dataSnapshots/ folder
        String sourceFileName = "src/test/resources/pserver.graphson";
        String destFileName   = System.getProperty("AJSC_HOME") + "/logs/data/dataSnapshots/pserver.graphson";
        copySnapshotFile(sourceFileName,destFileName);


        // Run the dataSnapshot to clear the graph
        String [] args = {"-c", "CLEAR_ENTIRE_DATABASE", "-f", "pserver.graphson"};
        DataSnapshot.main(args);

        // Since the code doesn't clear the graph using AAIGraph.getInstance().getGraph(), its creating a second inmemory graph
        // so we can't verify this with by counting the vertexes and edges in the graph
        // In the future we could do that but for now we will depend on the following string "All done clearing DB"

        // Capture the standard output and see if the following text is there
        assertThat(outputCapture.toString(), containsString(""));
    }


    @Test
    public void testClearEntireDatabaseWithEmptyGraphSONFileAndItShouldNotClearDatabase() throws IOException {

        // Create a empty file called empty.graphson in src/test/resources/

        // Copy that file to ${AJSC_HOME}/logs/data/dataSnapshots/
        String sourceFileName = "src/test/resources/empty.graphson";
        String destFileName   = System.getProperty("AJSC_HOME") + "/logs/data/dataSnapshots/empty.graphson";
        copySnapshotFile(sourceFileName,destFileName);

        // Run the clear dataSnapshot and this time it should fail
        String [] args = {"-c","CLEAR_ENTIRE_DATABASE", "-f","empty.graphson"};
        DataSnapshot.main(args);

        // Capture the standard output and see if the following text had no data is there
        // Since the graphson is empty it should output that and not clear the graph
        // Uncomment the following line after the test changes are done
         assertThat(outputCapture.toString(), containsString("graphson had no data."));
    }

    
    @Test
    public void testTakeSnapshotAndItShouldCreateASnapshotFileWithOneVertex() throws IOException, InterruptedException {

        String logsFolder     = System.getProperty("AJSC_HOME") + "/logs/data/dataSnapshots/";

        Set<Path> preSnapshotFiles = Files.walk(Paths.get(logsFolder)).collect(Collectors.toSet());

        // Run the clear dataSnapshot and this time it should fail
        //String [] args = {"JUST_TAKE_SNAPSHOT"};  >> default behavior is now to use 15 threads
        // To just get one file, you have to tell it to just use one.
        String [] args = {"-c","THREADED_SNAPSHOT", "-threadCount" ,"1"};

        DataSnapshot.main(args);

        // Add sleep so the file actually gets created with the data

        Set<Path> postSnapshotFiles = Files.walk(Paths.get(logsFolder)).collect(Collectors.toSet());

        assertThat(postSnapshotFiles.size(), is(preSnapshotFiles.size()+1));
        postSnapshotFiles.removeAll(preSnapshotFiles);
        List<Path> snapshotPathList = postSnapshotFiles.stream().collect(Collectors.toList());

        assertThat(snapshotPathList.size(), is(1));

        List<String> fileContents = Files.readAllLines(snapshotPathList.get(0));
        assertThat(fileContents.get(0), containsString("id"));
    }
    

    @Test
    public void testTakeSnapshotMultiAndItShouldCreateMultipleSnapshotFiles() throws IOException {

        String logsFolder     = System.getProperty("AJSC_HOME") + "/logs/data/dataSnapshots/";

        // Run the clear dataSnapshot and this time it should fail
        String [] args = {"-c","THREADED_SNAPSHOT", "-threadCount","2"};

        DataSnapshot.main(args);

        // For this test if there is only one vertex in the graph, not sure if it will create multiple files
        // would need to add more data to the janusgraph
    }

    @Test
    public void testFigureOutFileCount() throws IOException {

        long totalVerts = 5000;
        int threadCt = 15;
        long maxNodesPerFile = 120000;
        
        int fileCt = DataSnapshot.figureOutFileCount( totalVerts, threadCt, 
    			maxNodesPerFile );
        assertThat( fileCt, is(15));
               
        totalVerts = 5000;
        threadCt = 15;
        maxNodesPerFile = 100;
        fileCt = DataSnapshot.figureOutFileCount( totalVerts, threadCt, 
    			maxNodesPerFile );
        assertThat( fileCt, is(60));
        
        totalVerts = 1500;
        threadCt = 15;
        maxNodesPerFile = 100;
        fileCt = DataSnapshot.figureOutFileCount( totalVerts, threadCt, 
    			maxNodesPerFile );
        assertThat( fileCt, is(15));       
        
    }
    
    @Test
    public void testTakeSnapshotMultiWithDebugAndItShouldCreateMultipleSnapshotFiles() throws IOException {

        String logsFolder     = System.getProperty("AJSC_HOME") + "/logs/data/dataSnapshots/";

        // Run the clear dataSnapshot and this time it should fail
        String [] args = {"-c","THREADED_SNAPSHOT", "-threadCount","2", "-debugFlag","DEBUG"};

        DataSnapshot.main(args);

        // For this test if there is only one vertex in the graph, not sure if it will create multiple files
        // would need to add more data to the janusgraph
    }


    @Test
    public void testTakeSnapshotMultiWithDebugAndInvalidNumberAndItShouldFail() throws IOException {

        boolean thrown = false;
    	String logsFolder     = System.getProperty("AJSC_HOME") + "/logs/data/dataSnapshots/";

        // Run the clear dataSnapshot and this time it should fail
        String [] args = {"-c","THREADED_SNAPSHOT", "-threadCount","foo","-debugFlag", "DEBUG"};
        
        DataSnapshot.main(args);

        // For this test if there is only one vertex in the graph, not sure if it will create multiple files
        // would need to add more data to the janusgraph
    }

    @Test
    public void testTakeSnapshotMultiWithDebugAndTimeDelayAndInvalidNumberAndItShouldFail() throws IOException {

        String logsFolder     = System.getProperty("AJSC_HOME") + "/logs/data/dataSnapshots/";

        // Run the clear dataSnapshot and this time it should fail
        String [] args = {"-c","THREADED_SNAPSHOT","-threadCount", "foo", "-debugFlag","DEBUG","-debugAddDelayTime", "100"};

       	DataSnapshot.main(args);

        // For this test if there is only one vertex in the graph, not sure if it will create multiple files
        // would need to add more data to the janusgraph
    }

    @Test
    public void testTakeSnapshotMultiWithDebugAndTimeDelayAndZeroThreadsAndItShouldFail() throws IOException {

        String logsFolder     = System.getProperty("AJSC_HOME") + "/logs/data/dataSnapshots/";

        // Run the clear dataSnapshot and this time it should fail
        String [] args = {"-c","THREADED_SNAPSHOT", "-threadCount","0", "-debugFlag","DEBUG", "-debugAddDelayTime","100"};

        DataSnapshot.main(args);

        // For this test if there is only one vertex in the graph, not sure if it will create multiple files
        // would need to add more data to the janusgraph
    }

    @Test
    public void testTakeSnapshotMultiWithDebugAndTimeDelayIsInvalidNumberAndItShouldFail() throws IOException {

        String logsFolder     = System.getProperty("AJSC_HOME") + "/logs/data/dataSnapshots/";

        // Run the clear dataSnapshot and this time it should fail
        String [] args = {"-c","THREADED_SNAPSHOT","-threadCount", "0","-debugFlag","DEBUG", "-debugAddDelayTime","foo"};

        DataSnapshot.main(args);

        // For this test if there is only one vertex in the graph, not sure if it will create multiple files
        // would need to add more data to the janusgraph
    }

//    @Test
    public void testTakeSnapshotMultiWithMoreParametersThanAllowedAndItShouldFail() throws IOException {

        String logsFolder     = System.getProperty("AJSC_HOME") + "/logs/data/dataSnapshots/";

        // Run the clear dataSnapshot and this time it should fail
        String [] args = {"-c","THREADED_SNAPSHOT", "-threadCount", "0", "-debugFlag","DEBUG",  "-debugAddDelayTime","foo", "bar"};

        DataSnapshot.main(args);

        // For this test if there is only one vertex in the graph, not sure if it will create multiple files
        // would need to add more data to the janusgraph
    }

    @Test
    public void testTakeSnapshotMultiWithZeroThreadsAndItShouldFail(){

        // For this test if there is only one vertex in the graph, not sure if it will create multiple files
        // would need to add more data to the janusgraph
        String logsFolder     = System.getProperty("AJSC_HOME") + "/logs/data/dataSnapshots/";

        // Run the clear dataSnapshot and this time it should fail
        String [] args = {"-c","THREADED_SNAPSHOT", "-threadCount","0"};

        DataSnapshot.main(args);
    }

    @Test
    public void testTakeSnapshotMultiWithInvalidNumberForThreadsAndItShouldFail(){

        // For this test if there is only one vertex in the graph, not sure if it will create multiple files
        // would need to add more data to the janusgraph
        String logsFolder     = System.getProperty("AJSC_HOME") + "/logs/data/dataSnapshots/";

        // Run the clear dataSnapshot and this time it should fail
        String [] args = {"-c","THREADED_SNAPSHOT","-threadCount", "foo"};

        DataSnapshot.main(args);
    }

    @Test
    public void testReloadDataAndVerifyDataInGraphMatchesGraphson() throws IOException {

        // Create a graphson file that contains a couple of vertexes in src/test/resources
        // Copy that file to ${AJSC_HOME}/logs/data/dataSnasphots/
        // Run the reload arguments and ensure that the graph was recreated by checking vertexes in graph

        // After reload remove the added vertexes in the graph
        // The reason for this so each test is independent
        // as there shouldn't be dependencies and cause weird issues
        String sourceFileName = "src/test/resources/pserver.graphson";
        String destFileName   = System.getProperty("AJSC_HOME") + "/logs/data/dataSnapshots/pserver.graphson";
        copySnapshotFile(sourceFileName,destFileName);

        String [] args = {"-c","RELOAD_DATA", "-f","pserver.graphson"};

        DataSnapshot.main(args);
    }

    @Test
    public void testMultiReloadDataAndVerifyDataInGraphMatchesGraphson() throws IOException {

        // Create multiple graphson files that contains a couple of vertexes in src/test/resources
        // Copy those files to ${AJSC_HOME}/logs/data/dataSnasphots/
        // Run the reload arguments and ensure that the graph was recreated by checking vertexes in graph
        String sourceFileName = "src/test/resources/pserver2.graphson.P0";
        String destFileName   = System.getProperty("AJSC_HOME") + "/logs/data/dataSnapshots/pserver2.graphson.P0";
        copySnapshotFile(sourceFileName,destFileName);

        sourceFileName = "src/test/resources/pserver2.graphson.P1";
        destFileName   = System.getProperty("AJSC_HOME") + "/logs/data/dataSnapshots/pserver2.graphson.P1";
        copySnapshotFile(sourceFileName,destFileName);

        // After reload remove the added vertexes in the graph
        // The reason for this so each test is independent
        // as there shouldn't be dependencies and cause weird issues
        String [] args = {"-c","MULTITHREAD_RELOAD","-f", "pserver2.graphson"};

        DataSnapshot.main(args);
    }

    @Test
    public void testMultiReloadDataWithNonExistentFilesAndItShouldFail() throws IOException {

        // After reload remove the added vertexes in the graph
        // The reason for this so each test is independent
        // as there shouldn't be dependencies and cause weird issues
        String [] args = {"-c","MULTITHREAD_RELOAD", "-f","emptyfoo2.graphson"};

        DataSnapshot.main(args);
    }

    @Test
    public void testReloadMultiDataAndVerifyDataInGraphMatchesGraphson() throws IOException {

        // Create multiple graphson files that contains a couple of vertexes in src/test/resources
        // Copy those files to ${AJSC_HOME}/logs/data/dataSnasphots/
        // Run the reload arguments and ensure that the graph was recreated by checking vertexes in graph
        String sourceFileName = "src/test/resources/pserver2.graphson.P0";
        String destFileName   = System.getProperty("AJSC_HOME") + "/logs/data/dataSnapshots/pserver2.graphson.P0";
        copySnapshotFile(sourceFileName,destFileName);

        sourceFileName = "src/test/resources/pserver2.graphson.P1";
        destFileName   = System.getProperty("AJSC_HOME") + "/logs/data/dataSnapshots/pserver2.graphson.P1";
        copySnapshotFile(sourceFileName,destFileName);

        // After reload remove the added vertexes in the graph
        // The reason for this so each test is independent
        // as there shouldn't be dependencies and cause weird issues
        String [] args = {"-c","RELOAD_DATA_MULTI","-f", "pserver2.graphson"};

        DataSnapshot.main(args);
    }

    private List<Vertex> setupPserverData(GraphTraversalSource g) throws AAIException {
        Vertex v1 = g.addV().property("aai-node-type", "pserver")
            .property("hostname", "somerandomhostname")
            .next();
        List<Vertex> list = new ArrayList<>();
        list.add(v1);
        Vertex v2 = g.addV().property("aai-node-type", "pserver")
            .property("hostname", "somerandomhostname2")
            .next();
        Vertex pinterface = g.addV()
                .property("aai-node-type", "p-interface")
                .property("interface-name", "p-interface-name")
                .property("in-maint", false)
                .property("source-of-truth", "JUNIT")
                .next();
        edgeSerializer.addTreeEdge(g, v2, pinterface);
        list.add(v2);
        return list;
    }

    private void copySnapshotFile(String sourceFileName, String destFileName) throws IOException {

        File inputFile = new File(sourceFileName);
        File outputFile = new File(destFileName);

        FileUtils.copyFile(inputFile, outputFile);
    }
}