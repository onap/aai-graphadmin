package org.onap.aai;

import com.jayway.jsonpath.JsonPath;
import org.apache.tinkerpop.gremlin.process.traversal.dsl.graph.GraphTraversalSource;
import org.apache.tinkerpop.gremlin.structure.Vertex;
import org.hamcrest.CoreMatchers;
import org.janusgraph.core.JanusGraphTransaction;
import org.junit.*;
import org.junit.runner.RunWith;
import org.onap.aai.config.PropertyPasswordConfiguration;
import org.onap.aai.dbmap.AAIGraph;
import org.onap.aai.exceptions.AAIException;
import org.onap.aai.util.AAIConfig;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.boot.context.embedded.LocalServerPort;
import org.springframework.boot.test.context.SpringBootTest;
import org.springframework.context.annotation.Import;
import org.springframework.http.*;
import org.springframework.test.context.ContextConfiguration;
import org.springframework.test.context.TestPropertySource;
import org.springframework.test.context.junit4.SpringRunner;
import org.springframework.test.context.junit4.rules.SpringClassRule;
import org.springframework.test.context.junit4.rules.SpringMethodRule;
import org.springframework.web.client.RestTemplate;

import javax.ws.rs.core.Response;
import java.io.UnsupportedEncodingException;
import java.util.*;

import static org.hamcrest.CoreMatchers.containsString;
import static org.hamcrest.CoreMatchers.is;
import static org.hamcrest.MatcherAssert.assertThat;
import static org.junit.Assert.assertNotNull;
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
@ContextConfiguration(initializers = PropertyPasswordConfiguration.class)
@Import(GraphAdminTestConfiguration.class)
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

        headers.setAccept(Collections.singletonList(MediaType.APPLICATION_JSON));
        headers.setContentType(MediaType.APPLICATION_JSON);
        headers.add("Real-Time", "true");
        headers.add("X-FromAppId", "JUNIT");
        headers.add("X-TransactionId", "JUNIT");

        String authorization = Base64.getEncoder().encodeToString("AAI:AAI".getBytes("UTF-8"));
        headers.add("Authorization", "Basic " + authorization);
        httpEntity = new HttpEntity(headers);
        baseUrl = "https://localhost:" + randomPort;
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

        ResponseEntity responseEntity = null;

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
