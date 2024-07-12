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
package org.onap.aai;

import com.jayway.jsonpath.JsonPath;
import org.apache.tinkerpop.gremlin.process.traversal.dsl.graph.GraphTraversalSource;
import org.janusgraph.core.JanusGraphTransaction;
import org.junit.*;
import org.onap.aai.restclient.PropertyPasswordConfiguration;
import org.onap.aai.dbmap.AAIGraph;
import org.onap.aai.exceptions.AAIException;
import org.onap.aai.util.AAIConfig;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.boot.web.server.LocalServerPort;
import org.springframework.boot.autoconfigure.EnableAutoConfiguration;
import org.springframework.boot.autoconfigure.cassandra.CassandraAutoConfiguration;
import org.springframework.boot.autoconfigure.data.cassandra.CassandraDataAutoConfiguration;
import org.springframework.boot.test.context.SpringBootTest;
import org.springframework.context.annotation.Import;
import org.springframework.http.*;
import org.springframework.test.context.ContextConfiguration;
import org.springframework.test.context.TestPropertySource;
import org.springframework.test.context.junit4.rules.SpringClassRule;
import org.springframework.test.context.junit4.rules.SpringMethodRule;
import org.springframework.web.client.RestTemplate;

import java.util.Base64;
import java.util.Collections;
import java.util.HashMap;
import java.util.Map;

import static org.hamcrest.CoreMatchers.containsString;
import static org.hamcrest.CoreMatchers.is;
import static org.hamcrest.MatcherAssert.assertThat;
import static org.junit.Assert.fail;

/**
 * A sample junit test using spring boot that provides the ability to spin
 * up the application from the junit layer and run rest requests against
 * SpringBootTest annotation with web environment requires which spring boot
 * class to load and the random port starts the application on a random port
 * and injects back into the application for the field with annotation LocalServerPort
 * <p>
 *
 * This can be used to potentially replace a lot of the fitnesse tests since
 * they will be testing against the same thing except fitnesse uses hbase
 */
@SpringBootTest(webEnvironment = SpringBootTest.WebEnvironment.RANDOM_PORT, classes = GraphAdminApp.class)
@EnableAutoConfiguration(exclude={CassandraDataAutoConfiguration.class, CassandraAutoConfiguration.class}) // there is no running cassandra instance for the test
@ContextConfiguration(initializers = PropertyPasswordConfiguration.class)
@Import(GraphAdminTestConfiguration.class)
@TestPropertySource(
    properties = {
        "schema.uri.base.path = /aai",
        "schema.ingest.file = src/test/resources/application-test.properties",
        "schema.translator.list = config"
    },
    locations = "classpath:application-test.properties"
)
public class AAIGremlinQueryTest {

    @ClassRule
    public static final SpringClassRule springClassRule = new SpringClassRule();

    @Rule
    public final SpringMethodRule springMethodRule = new SpringMethodRule();

    @Autowired
    RestTemplate restTemplate;

    @LocalServerPort
    int randomPort;

    private HttpEntity httpEntity;

    private HttpHeaders headers;

    private String baseUrl;

    @BeforeClass
    public static void setupConfig() throws AAIException {
        System.setProperty("AJSC_HOME", "./");
        System.setProperty("BUNDLECONFIG_DIR", "src/main/resources/");
    }

    public void createGraph(){

        JanusGraphTransaction transaction = AAIGraph.getInstance().getGraph().newTransaction();

        boolean success = true;

        try {

            GraphTraversalSource g = transaction.traversal();

            g.addV()
                .property("aai-node-type", "pserver")
                .property("hostname", "test-pserver")
                .property("in-maint", false)
                .property("source-of-truth", "JUNIT")
                .property("aai-uri", "/cloud-infrastructure/pservers/pserver/test-pserver")
                .next();

        } catch(Exception ex){
            success = false;
        } finally {
            if(success){
                transaction.commit();
            } else {
                transaction.rollback();
                fail("Unable to setup the graph");
            }
        }
    }

    @Before
    public void setup() throws Exception {

        AAIConfig.init();
        AAIGraph.getInstance();

        createGraph();
        headers = new HttpHeaders();

        String authorization = Base64.getEncoder().encodeToString("AAI:AAI".getBytes("UTF-8"));
        headers.setAccept(Collections.singletonList(MediaType.APPLICATION_JSON));
        headers.setContentType(MediaType.APPLICATION_JSON);
        headers.add("Real-Time", "true");
        headers.add("X-FromAppId", "JUNIT");
        headers.add("X-TransactionId", "JUNIT");
        headers.add("Authorization", "Basic " + authorization);



        baseUrl = "http://localhost:" + randomPort;
    }

    @Test
    public void testPserverCountUsingGremlin() throws Exception {
        Map<String, String> gremlinQueryMap = new HashMap<>();
        gremlinQueryMap.put("gremlin-query", "g.V().has('hostname', 'test-pserver').count()");

        String payload = PayloadUtil.getTemplatePayload("gremlin-query.json", gremlinQueryMap);

        ResponseEntity responseEntity = null;

        String endpoint = "/aai/v11/dbquery?format=console";

        httpEntity = new HttpEntity(payload, headers);
        responseEntity = restTemplate.exchange(baseUrl + endpoint, HttpMethod.PUT, httpEntity, String.class);
        assertThat(responseEntity.getStatusCode(), is(HttpStatus.OK));

        String result = JsonPath.read(responseEntity.getBody().toString(), "$.results[0].result");
        assertThat(result, is("1"));
    }

    @Test
    public void testPserverCountUsingDsl() throws Exception {
        Map<String, String> dslQuerymap = new HashMap<>();
        dslQuerymap.put("dsl-query", "pserver*('hostname', 'test-pserver')");

        String payload = PayloadUtil.getTemplatePayload("dsl-query.json", dslQuerymap);

        ResponseEntity responseEntity;

        String endpoint = "/aai/v11/dbquery?format=console";

        httpEntity = new HttpEntity(payload, headers);
        responseEntity = restTemplate.exchange(baseUrl + endpoint, HttpMethod.PUT, httpEntity, String.class);
        assertThat(responseEntity.getStatusCode(), is(HttpStatus.OK));

        String result = JsonPath.read(responseEntity.getBody().toString(), "$.results[0].result");
        assertThat(result, containsString("v["));
    }

    @After
    public void tearDown() {

        JanusGraphTransaction transaction = AAIGraph.getInstance().getGraph().newTransaction();
        boolean success = true;

        try {

            GraphTraversalSource g = transaction.traversal();

            g.V().has("source-of-truth", "JUNIT")
                    .toList()
                    .forEach(v -> v.remove());

        } catch(Exception ex){
            success = false;
        } finally {
            if(success){
                transaction.commit();
            } else {
                transaction.rollback();
                fail("Unable to teardown the graph");
            }
        }

    }
}
