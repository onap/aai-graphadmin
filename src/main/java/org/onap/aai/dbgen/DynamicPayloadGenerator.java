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

import com.beust.jcommander.JCommander;
import com.beust.jcommander.Parameter;
import com.fasterxml.jackson.core.type.TypeReference;
import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.fasterxml.jackson.databind.node.ObjectNode;

import org.apache.tinkerpop.gremlin.process.traversal.P;
import org.apache.tinkerpop.gremlin.process.traversal.dsl.graph.GraphTraversal;
import org.apache.tinkerpop.gremlin.process.traversal.step.util.Tree;
import org.apache.tinkerpop.gremlin.structure.Element;
import org.apache.tinkerpop.gremlin.structure.Vertex;
import org.onap.aai.db.props.AAIProperties;
import org.onap.aai.dbmap.InMemoryGraph;
import org.onap.aai.edges.EdgeIngestor;
import org.onap.aai.edges.EdgeRule;
import org.onap.aai.edges.EdgeRuleQuery;
import org.onap.aai.edges.enums.AAIDirection;
import org.onap.aai.edges.enums.EdgeType;
import org.onap.aai.edges.exceptions.AmbiguousRuleChoiceException;
import org.onap.aai.edges.exceptions.EdgeRuleNotFoundException;
import org.onap.aai.exceptions.AAIException;
import org.onap.aai.introspection.Introspector;
import org.onap.aai.introspection.Loader;
import org.onap.aai.introspection.LoaderFactory;
import org.onap.aai.introspection.ModelType;
import org.onap.aai.introspection.exceptions.AAIUnknownObjectException;
import org.onap.aai.logging.ErrorLogHelper;
import org.onap.aai.logging.LogFormatTools;
import org.onap.aai.parsers.uri.URIToObject;
import org.onap.aai.serialization.db.DBSerializer;
import org.onap.aai.serialization.engines.InMemoryDBEngine;
import org.onap.aai.serialization.engines.QueryStyle;
import org.onap.aai.serialization.tinkerpop.TreeBackedVertex;
import org.onap.aai.setup.SchemaVersion;
import org.onap.aai.setup.SchemaVersions;
import org.onap.aai.util.AAIConfig;
import org.onap.aai.util.AAIConstants;
import org.onap.aai.util.AAISystemExitUtil;
import org.onap.aai.util.ExceptionTranslator;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.slf4j.MDC;
import org.springframework.context.annotation.AnnotationConfigApplicationContext;

import java.io.*;
import java.net.URI;
import java.nio.file.Files;
import java.nio.file.InvalidPathException;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.text.DateFormat;
import java.text.SimpleDateFormat;
import java.util.*;
import java.util.Map.Entry;
import java.util.regex.Matcher;
import java.util.regex.Pattern;

/*
 * The Class ListEndpoints.
 */
public class DynamicPayloadGenerator {

	/*
	 * Create a Dynamic memory graph instance which should not affect the
	 * AAIGraph
	 */
	private InMemoryGraph inMemGraph = null;

	private InMemoryDBEngine dbEngine;
	private InputStream sequenceInputStreams;
	/*
	 * Loader, QueryStyle, ConnectionType for the Serializer
	 */
	private Loader loader;
	private String urlBase;
	private BufferedWriter bw = null;
	private boolean exitFlag = true;
	private CommandLineArgs cArgs;

	private static final Logger LOGGER = LoggerFactory.getLogger(DynamicPayloadGenerator.class);

	private static final QueryStyle queryStyle = QueryStyle.TRAVERSAL;
	private static final ModelType introspectorFactoryType = ModelType.MOXY;
	private final LoaderFactory loaderFactory;
	private final EdgeIngestor edgeRules;
	private final SchemaVersions schemaVersions;
	private final SchemaVersion version;

	public DynamicPayloadGenerator(LoaderFactory loaderFactory, EdgeIngestor edgeIngestor, SchemaVersions schemaVersions){
	    this.loaderFactory = loaderFactory;
		this.edgeRules = edgeIngestor;
		this.schemaVersions = schemaVersions;
		this.version = schemaVersions.getDefaultVersion();
	}

	/**
	 * The run method.
	 *
	 * @param args
	 *            the arguments
	 * @param exitFlag true if running from a shell script to call system exit, false if running from scheduled task
	 * @throws AAIException
	 * @throws Exception
	 */

	public static void run (LoaderFactory loaderFactory, EdgeIngestor edgeIngestor, SchemaVersions schemaVersions, String[] args, boolean isSystemExit) {
		//
		MDC.put("logFilenameAppender", DynamicPayloadGenerator.class.getSimpleName());
		DynamicPayloadGenerator payloadgen = new DynamicPayloadGenerator(loaderFactory, edgeIngestor, schemaVersions);
		payloadgen.exitFlag = isSystemExit;
		try {
			payloadgen.init(args);

			payloadgen.generatePayloads();
		} catch (AAIException | IOException e) {
			LOGGER.error("Exception {}", LogFormatTools.getStackTop(e));
		}
		if ( isSystemExit ) {
			AAISystemExitUtil.systemExitCloseAAIGraph(1);
		}
		else {
			AAISystemExitUtil.systemExitCloseAAIGraph(0);
		}

	}
	public static void main(String[] args) throws AAIException {
		AnnotationConfigApplicationContext ctx = new AnnotationConfigApplicationContext();
		try {
			ctx.scan(
					"org.onap.aai"
			);
			ctx.refresh();
		} catch (Exception e) {
			AAIException aai = ExceptionTranslator.schemaServiceExceptionTranslator(e);
			LOGGER.error("Problems running tool {}", aai.getMessage());
			ErrorLogHelper.logError(aai.getCode(), e.getMessage() + ", resolve and retry");
			throw aai;

		}
		LoaderFactory loaderFactory = ctx.getBean(LoaderFactory.class);
		EdgeIngestor  edgeIngestor  = ctx.getBean(EdgeIngestor.class);
		SchemaVersions schemaVersions = (SchemaVersions) ctx.getBean("schemaVersions");
		run (loaderFactory, edgeIngestor, schemaVersions, args, true);
	}


	public void taskExit() {
		if ( this.exitFlag ) {
			AAISystemExitUtil.systemExitCloseAAIGraph(1);
		}
		else {
			AAISystemExitUtil.systemExitCloseAAIGraph(0);
		}
	}
	public void init(String[] args) throws AAIException {
		cArgs = new CommandLineArgs();
		JCommander jCommander = new JCommander(cArgs, args);
		jCommander.setProgramName(DynamicPayloadGenerator.class.getSimpleName());
		LOGGER.debug("Snapshot file " + cArgs.dataSnapshot);


		// TODO- How to add dynamic.properties

		LOGGER.debug("output file " + cArgs.output);
		LOGGER.debug("format file " + cArgs.format);
		LOGGER.debug("schema enabled " + cArgs.schemaEnabled);
		LOGGER.debug("Multiple snapshots " + cArgs.isMultipleSnapshot);
		LOGGER.debug("Is Partial Graph " + cArgs.isPartialGraph);

		if (cArgs.config.isEmpty())
			cArgs.config = AAIConstants.AAI_HOME_ETC_APP_PROPERTIES + "dynamic.properties";

		LOGGER.debug("config file " + cArgs.config);
		if (cArgs.nodePropertyFile.isEmpty())
			cArgs.nodePropertyFile = AAIConstants.AAI_HOME_ETC_SCRIPT + "/tenant_isolation/nodes.json";
		LOGGER.debug("nodePropertyFile file " + cArgs.nodePropertyFile);

		if (cArgs.inputFilterPropertyFile.isEmpty())
			cArgs.inputFilterPropertyFile = AAIConstants.AAI_HOME_ETC_SCRIPT + "/tenant_isolation/inputFilters.json";
		LOGGER.debug("inputFilterPropertyFile file " + cArgs.inputFilterPropertyFile);

		if (cArgs.isPartialGraph)
			cArgs.dataSnapshot = cArgs.dataSnapshot+".partial";

		if (!cArgs.isMultipleSnapshot) {
			validateFile(cArgs.dataSnapshot);
		} else {
			// for multiple snapshots dataSnapshot + ".P" is the prefix of the
			// files
			sequenceInputStreams = validateMultipleSnapshots(cArgs.dataSnapshot);
		}

		LOGGER.debug("Datasnapshot file " + cArgs.dataSnapshot);
		AAIConfig.init();

		urlBase = AAIConfig.get("aai.server.url.base", "");

	}

	public void generatePayloads() throws AAIException, IOException {

		List<Map<String, List<String>>> nodeFilters = readFile(cArgs.nodePropertyFile);
		/*
		 * Read the inputFilters which will include for each node-type the regex that needs to be
		 * applied and the filtered-node-type
		 * For eg: complex --> apply regex on cloud-region and then traverse to complex
		 * complex --> filtered-node-type: cloud-region, filters: include regex on cloud-region
		 */
		/*
		 * Example:
		 * { "cloud-region" :
		 *		 {"filtered-node-type":"cloud-region",
		 * 		  "filters": [ { "property": "cloud-owner", "regex": "att-aic" },
		 * 					 { "property": "cloud-region-id", "regex": "M*" },
		 *                   { "property":"cloud-region-version", "regex": "aic2.5|aic3.0" }
		 *                 ] },
		 *  "complex" : {
		 * 		"filtered-node-type":"cloud-region",
		 *       "filters": [ { "property": "cloud-owner", "regex": "att-aic" },
		 * 					 { "property": "cloud-region-id", "regex": "M*" },
		 *                   { "property":"cloud-region-version", "regex": "aic2.5|aic3.0" }
		 *                 ] },
		 *
		 * } }
		 */
		Map<String, Map<String, String>> inputFilters = readInputFilterPropertyFile(cArgs.inputFilterPropertyFile);
		Map<String, String> filteredNodeTypes = findFilteredNodeTypes(cArgs.inputFilterPropertyFile);
		// Read the input filter criteria
		LOGGER.debug("Load the Graph");

		this.loadGraph();
		LOGGER.debug("Generate payload");
		this.generatePayload(nodeFilters, inputFilters, filteredNodeTypes);
		LOGGER.debug("Close graph");
		this.closeGraph();

	}

	private List<Map<String, List<String>>> readFile(String inputFile) throws IOException {

		// validate that we can read the inputFile
		validateFile(inputFile);

		InputStream is = new FileInputStream(inputFile);
		Scanner scanner = new Scanner(is);
		String jsonFile = scanner.useDelimiter("\\Z").next();
		scanner.close();

		List<Map<String, List<String>>> allNodes = new ArrayList<>();
		Map<String, List<String>> filterCousins = new HashMap<>();
		Map<String, List<String>> filterParents = new HashMap<>();

		ObjectMapper mapper = new ObjectMapper();

		JsonNode rootNode = mapper.readTree(jsonFile);

		Iterator<Entry<String, JsonNode>> nodeFields = rootNode.fields();

		while (nodeFields.hasNext()) {
			Entry<String, JsonNode> entry = nodeFields.next();
			String nodeType = entry.getKey();
			JsonNode nodeProperty = entry.getValue();

			JsonNode cousinFilter = nodeProperty.path("cousins");
			JsonNode parentFilter = nodeProperty.path("parents");
			List<String> cousins = new ObjectMapper().readValue(cousinFilter.traverse(),
					new TypeReference<ArrayList<String>>() {
					});

			List<String> parents = new ObjectMapper().readValue(parentFilter.traverse(),
					new TypeReference<ArrayList<String>>() {
					});
			for (String cousin : cousins) {
				LOGGER.debug("Cousins-Filtered " + cousin);
			}
			for (String parent : parents) {
				LOGGER.debug("Parents-Filtered " + parent);
			}
			filterCousins.put(nodeType, cousins);
			filterParents.put(nodeType, parents);

		}

		allNodes.add(filterCousins);
		allNodes.add(filterParents);
		return allNodes;

	}

 /* Example:
{
  "cloud-region" : {
      "filtered-node-type" :"cloud-region",
      "filters": [
             {
                 "property": "cloud-owner",
                "regex": "att-aic"
             },
             {
                 "property": "cloud-region-id",
                "regex": "M*"
             },
             {
                 "property": "cloud-region-version",
                "regex": "aic2.5|aic3.0"
             }
    ]
  },
  "complex" : {
           "filters":[
           ]

  }
}
*/
	private Map<String, Map<String, String>> readInputFilterPropertyFile(String inputFile) throws IOException {

		validateFile(inputFile);

		InputStream is = new FileInputStream(inputFile);
		Scanner scanner = new Scanner(is);
		String jsonFile = scanner.useDelimiter("\\Z").next();
		scanner.close();

		Map<String, Map<String, String>> propToRegex = new HashMap<String, Map<String, String>>();

		ObjectMapper mapper = new ObjectMapper();

		JsonNode rootNode = mapper.readTree(jsonFile);

		Iterator<Entry<String, JsonNode>> nodeFields = rootNode.fields();

		while (nodeFields.hasNext()) {
			Entry<String, JsonNode> entry = nodeFields.next();
			String nodeType = entry.getKey();
			JsonNode nodeProperty = entry.getValue();

			JsonNode filter = nodeProperty.path("filters");
			List<JsonNode> filterMap = new ObjectMapper().readValue(filter.traverse(),
					new TypeReference<ArrayList<JsonNode>>() {
					});
			HashMap<String, String> filterMaps = new HashMap<String, String>();
			for (JsonNode n : filterMap) {
				filterMaps.put(n.get("property").asText(), n.get("regex").asText());
			}

			propToRegex.put(nodeType, filterMaps);
		}
		return (propToRegex);
	}

	private Map<String, String> findFilteredNodeTypes(String inputFile) throws IOException {

		validateFile(inputFile);

		InputStream is = new FileInputStream(inputFile);
		Scanner scanner = new Scanner(is);
		String jsonFile = scanner.useDelimiter("\\Z").next();
		scanner.close();

		Map<String, String> filteredNodeTypes = new HashMap<String, String>();

		ObjectMapper mapper = new ObjectMapper();

		JsonNode rootNode = mapper.readTree(jsonFile);

		Iterator<Entry<String, JsonNode>> nodeFields = rootNode.fields();

		while (nodeFields.hasNext()) {
			Entry<String, JsonNode> entry = nodeFields.next();
			String nodeType = entry.getKey();
			JsonNode nodeProperty = entry.getValue();

			JsonNode filter = nodeProperty.path("filtered-node-type");

			filteredNodeTypes.put(nodeType, filter.asText());
		}
		return (filteredNodeTypes);
	}

	public void loadGraph() throws IOException {

		loadGraphIntoMemory();
		buildDbEngine();

	}

	private void loadGraphIntoMemory() throws IOException {
		if (!(cArgs.isMultipleSnapshot)) {
			inMemGraph = new InMemoryGraph.Builder().build(cArgs.dataSnapshot, cArgs.config, cArgs.schemaEnabled,
					cArgs.isPartialGraph);
		} else {
			inMemGraph = new InMemoryGraph.Builder().build(sequenceInputStreams, cArgs.config, cArgs.schemaEnabled,
					cArgs.isPartialGraph);
		}
	}

	private void buildDbEngine() {
		// TODO : parametrise version
		loader = loaderFactory.createLoaderForVersion(introspectorFactoryType, version);

		dbEngine = new InMemoryDBEngine(queryStyle, loader, inMemGraph.getGraph());
		dbEngine.startTransaction();
	}

	private void generatePayload(List<Map<String, List<String>>> nodeFilters,
			Map<String, Map<String, String>> inputFilters, Map<String, String> filteredNodeTypes)
			throws AAIException, IOException {

		Map<String, List<String>> filterCousinsMap = nodeFilters.get(0);
		Map<String, List<String>> filterParentsMap = nodeFilters.get(1);
    	Set<String> nodeTypes = filterCousinsMap.keySet();

		for (String nodeType : nodeTypes) {
			if ("DMAAP-MR".equals(cArgs.format)) {
				bw = createFile(nodeType + ".json");
			}
			List<String> filterCousins = filterCousinsMap.get(nodeType);
			List<String> filterParents = filterParentsMap.get(nodeType);
			Map<String, String> nodeInputFilterMap = inputFilters.get(nodeType);
			String filteredNodeType = nodeType;
			if(filteredNodeTypes.get(nodeType) != null && !filteredNodeTypes.get(nodeType).isEmpty())
				filteredNodeType = filteredNodeTypes.get(nodeType);
			readVertices(nodeType, filterCousins, filterParents, nodeInputFilterMap, filteredNodeType);
			if(bw != null)
				bw.close();
			LOGGER.debug("All Done-" + nodeType);
		}

	}

	private BufferedWriter createFile(String outfileName) throws IOException {
		// FileLocation
		String fileName = outfileName;
		File outFile = new File(fileName);
		FileWriter fw = null;
		LOGGER.debug(" Will write to " + fileName);
		try {
			fw = new FileWriter(outFile.getAbsoluteFile());
		} catch (IOException i) {
			String emsg = "Unable to write to " + fileName + " Exception = " + i.getMessage();
			LOGGER.error(emsg);
			System.out.println(emsg);
			throw i;
		}
		return new BufferedWriter(fw);
	}

	private void createDirectory(String dirName) throws IOException {
		// FileLocation
		Path pathDir = null;
		try {
			pathDir = Paths.get(dirName);
		} catch (InvalidPathException i) {
			String emsg = "Directory " + dirName + " could not be found.";
			LOGGER.error(emsg);
			System.out.println(emsg);
			taskExit();
		}
		try {
			Files.createDirectories(pathDir);
		} catch (Exception e) {
			String emsg = "Directory " + dirName + " could not be created. Exception = " + e.getMessage();
			LOGGER.error(emsg);
			System.out.println(emsg);
			taskExit();
		}
	}

	public void readVertices(String nodeType, List<String> filterCousins, List<String> filterParents,
			Map<String, String> nodeInputFilters, String filteredNodeType) throws AAIException, IOException {

		DBSerializer serializer = new DBSerializer(version, dbEngine, introspectorFactoryType, "sourceOfTruth");

		/*
		 * Start with nodeType you need to filter and then traverse to the actual nodeType
		 */
		GraphTraversal<Vertex, Vertex> gtraversal = inMemGraph.getGraph().traversal().V().has("aai-node-type",
				filteredNodeType);


		// input regex
		if (nodeInputFilters != null && (!nodeInputFilters.isEmpty())) {
			for (Map.Entry<String, String> entry : nodeInputFilters.entrySet()) {
				String property = entry.getKey();
				String regex = entry.getValue();
				Pattern pa = Pattern.compile(regex);

				gtraversal = gtraversal.has(property, P.test((t, p) -> {
					Matcher m = ((Pattern) p).matcher((CharSequence) t);
					boolean b = m.matches();
					return b;
				}, pa));
			}
		}

		/*
		 * Tenant, AZ, Complex, Zone, pserver come here
		 */
		if (!filteredNodeType.equals(nodeType)) {

			EdgeRuleQuery treeEdgeRuleQuery = new EdgeRuleQuery
					.Builder(filteredNodeType, nodeType)
					.edgeType(EdgeType.TREE)
					.build();

			EdgeRuleQuery cousinEdgeQuery = new EdgeRuleQuery
					.Builder(filteredNodeType, nodeType)
					.edgeType(EdgeType.COUSIN)
					.build();

			EdgeRule rule = null;
			boolean hasTreeEdgeRule = true;

			try {
				rule = edgeRules.getRule(treeEdgeRuleQuery);
			} catch (EdgeRuleNotFoundException | AmbiguousRuleChoiceException e) {
				hasTreeEdgeRule = false;
			}

			if(!hasTreeEdgeRule) {
				try {
					rule = edgeRules.getRule(cousinEdgeQuery);
				} catch (EdgeRuleNotFoundException | AmbiguousRuleChoiceException e) {
				    LOGGER.error("Unable to get a tree or cousin edge between {} and {}", filteredNodeType, nodeType);
				    return;
				}
			}

			if (rule.getDirection().toString().equals(AAIDirection.OUT.toString())) {
				gtraversal.out(rule.getLabel()).has("aai-node-type", nodeType);
			} else {
				gtraversal.in(rule.getLabel()).has("aai-node-type", nodeType);
			}

		}

		String dirName = cArgs.output + AAIConstants.AAI_FILESEP + nodeType + AAIConstants.AAI_FILESEP;
		createDirectory(dirName);
		// TODO: Formatter

		if ("DMAAP-MR".equals(cArgs.format)) {
			while (gtraversal.hasNext()) {
				if (bw != null)
					bw = createFile(nodeType + ".json");
				Vertex node = gtraversal.next();
				Introspector nodeObj = serializer.getLatestVersionView(node);
				createPayloadForDmaap(node, nodeObj);
			}
		} else {
			if ("PAYLOAD".equals(cArgs.format)) {
				int counter = 0;
				while (gtraversal.hasNext()) {
					Vertex node = gtraversal.next();
					try {
						counter++;
						String filename = dirName + counter + "-" + nodeType + ".json";
						bw = createFile(filename);
						Introspector obj = loader.introspectorFromName(nodeType);
						Set<Vertex> seen = new HashSet<>();
						int depth = AAIProperties.MAXIMUM_DEPTH;
						boolean nodeOnly = false;

						Tree<Element> tree = dbEngine.getQueryEngine().findSubGraph(node, depth, nodeOnly);
						TreeBackedVertex treeVertex = new TreeBackedVertex(node, tree);
						serializer.dbToObjectWithFilters(obj, treeVertex, seen, depth, nodeOnly, filterCousins,
								filterParents);
						createPayloadForPut(obj);
						if(bw != null)
							bw.close();

						URI uri = serializer.getURIForVertex(node);
						String filenameWithUri = dirName + counter + "-" + nodeType + ".txt";
						bw = createFile(filenameWithUri);
						bw.write(uri.toString());
						bw.newLine();
						bw.close();
					} catch (Exception e) {
						String emsg = "Caught exception while processing [" + counter + "-" + nodeType + "] continuing";
						System.out.println(emsg);
						LOGGER.error(emsg);

					}
				}
			}
		}

	}

	public void createPayloadForPut(Introspector nodeObj) throws IOException {

		String entityJson = nodeObj.marshal(false);
		ObjectMapper mapper = new ObjectMapper();

		ObjectNode rootNode = (ObjectNode) mapper.readTree(entityJson);
		rootNode.remove("resource-version");

		bw.newLine();
		bw.write(rootNode.toString());
		bw.newLine();
	}

	public void createPayloadForDmaap(Vertex node, Introspector nodeObj)
			throws AAIException, UnsupportedEncodingException {

		DBSerializer serializer = new DBSerializer(version, dbEngine, introspectorFactoryType, "sourceOfTruth");

		URI uri = serializer.getURIForVertex(node);

		String sourceOfTruth = "";
		HashMap<String, Introspector> relatedVertices = new HashMap<>();
		List<Vertex> vertexChain = dbEngine.getQueryEngine().findParents(node);

		for (Vertex vertex : vertexChain) {
			try {

				Introspector vertexObj = serializer.getVertexProperties(vertex);

				relatedVertices.put(vertexObj.getObjectId(), vertexObj);
			} catch (AAIUnknownObjectException e) {
				LOGGER.warn("Unable to get vertex properties, partial list of related vertices returned");
			}

		}

		String transactionId = "TXID";
		createNotificationEvent(transactionId, sourceOfTruth, uri, nodeObj, relatedVertices);

	}

	public void createNotificationEvent(String transactionId, String sourceOfTruth, URI uri, Introspector obj,
			Map<String, Introspector> relatedObjects) throws AAIException, UnsupportedEncodingException {

		String action = "CREATE";
		final Introspector notificationEvent = loader.introspectorFromName("notification-event");

		try {
			Introspector eventHeader = loader.introspectorFromName("notification-event-header");
			URIToObject parser = new URIToObject(loader, uri, (HashMap) relatedObjects);

			String entityLink = urlBase + version + uri;

			notificationEvent.setValue("cambria-partition", "AAI");

			eventHeader.setValue("entity-link", entityLink);
			eventHeader.setValue("action", action);
			eventHeader.setValue("entity-type", obj.getDbName());
			eventHeader.setValue("top-entity-type", parser.getTopEntityName());
			eventHeader.setValue("source-name", sourceOfTruth);
			eventHeader.setValue("version", version.toString());
			eventHeader.setValue("id", transactionId);
			eventHeader.setValue("event-type", "AAI-BASELINE");
			if (eventHeader.getValue("domain") == null) {
				eventHeader.setValue("domain", AAIConfig.get("aai.notificationEvent.default.domain", "UNK"));
			}

			if (eventHeader.getValue("sequence-number") == null) {
				eventHeader.setValue("sequence-number",
						AAIConfig.get("aai.notificationEvent.default.sequenceNumber", "UNK"));
			}

			if (eventHeader.getValue("severity") == null) {
				eventHeader.setValue("severity", AAIConfig.get("aai.notificationEvent.default.severity", "UNK"));
			}

			if (eventHeader.getValue("id") == null) {
				eventHeader.setValue("id", genDate2() + "-" + UUID.randomUUID().toString());

			}

			if (eventHeader.getValue("timestamp") == null) {
				eventHeader.setValue("timestamp", genDate());
			}

			List<Object> parentList = parser.getParentList();
			parentList.clear();

			if (!parser.getTopEntity().equals(parser.getEntity())) {
				Introspector child;
				String json = obj.marshal(false);
				child = parser.getLoader().unmarshal(parser.getEntity().getName(), json);
				parentList.add(child.getUnderlyingObject());
			}

			final Introspector eventObject;

			String json = "";
			if (parser.getTopEntity().equals(parser.getEntity())) {
				json = obj.marshal(false);
				eventObject = loader.unmarshal(obj.getName(), json);
			} else {
				json = parser.getTopEntity().marshal(false);

				eventObject = loader.unmarshal(parser.getTopEntity().getName(), json);
			}
			notificationEvent.setValue("event-header", eventHeader.getUnderlyingObject());
			notificationEvent.setValue("entity", eventObject.getUnderlyingObject());

			String entityJson = notificationEvent.marshal(false);

			bw.newLine();
			bw.write(entityJson);

		} catch (AAIUnknownObjectException e) {
			LOGGER.error("Fatal error - notification-event-header object not found!");
		} catch (Exception e) {
			LOGGER.error("Unmarshalling error occurred while generating Notification " + LogFormatTools.getStackTop(e));
		}
	}

	private void closeGraph() {
		inMemGraph.getGraph().tx().rollback();
		inMemGraph.getGraph().close();
	}

	public static String genDate() {
		Date date = new Date();
		DateFormat formatter = new SimpleDateFormat("yyyyMMdd-HH:mm:ss:SSS");
		return formatter.format(date);
	}

	public static String genDate2() {
		Date date = new Date();
		DateFormat formatter = new SimpleDateFormat("yyyyMMddHHmmss");
		return formatter.format(date);
	}

	private void validateFile(String filename) {
		File f = new File(filename);
		if (!f.exists()) {
			String emsg = "File " + filename + " could not be found.";
			LOGGER.error(emsg);
			System.out.println(emsg);
			taskExit();
		} else if (!f.canRead()) {
			String emsg = "File " + filename + " could not be read.";
			LOGGER.error(emsg);
			System.out.println(emsg);
			taskExit();
		} else if (f.length() == 0) {
			String emsg = "File " + filename + " had no data.";
			LOGGER.error(emsg);
			System.out.println(emsg);
			taskExit();
		}
	}

	private InputStream validateMultipleSnapshots(String filenamePrefix) {
		if (filenamePrefix == null || filenamePrefix.length() == 0) {
			String emsg = "No snapshot path was provided.";
			LOGGER.error(emsg);
			System.out.println(emsg);
			taskExit();
		}
		String targetDir = ".";
		int lastSeparator = filenamePrefix.lastIndexOf(File.separator);

		LOGGER.debug("File separator=[" + File.separator + "] lastSeparator=" + lastSeparator + " filenamePrefix="
				+ filenamePrefix);
		if (lastSeparator >= 0) {
			targetDir = filenamePrefix.substring(0, lastSeparator);
			LOGGER.debug("targetDir=" + targetDir);
		}
		if (targetDir.length() == 0) {
			String emsg = "No snapshot directory was found in path:" + filenamePrefix;
			LOGGER.error(emsg);
			System.out.println(emsg);
			taskExit();
		}
		String prefix = filenamePrefix.substring(lastSeparator + 1);
		if (prefix == null || prefix.length() == 0) {
			String emsg = "No snapshot file prefix was provided.";
			LOGGER.error(emsg);
			System.out.println(emsg);
			taskExit();
		}
		long timeA = System.nanoTime();

		ArrayList<File> snapFilesArr = new ArrayList<File>();
		String thisSnapPrefix = prefix + ".P";
		File fDir = new File(targetDir); // Snapshot directory
		File[] allFilesArr = fDir.listFiles();
		for (File snapFile : allFilesArr) {
			String snapFName = snapFile.getName();
			if (snapFName.startsWith(thisSnapPrefix)) {
				snapFilesArr.add(snapFile);
			}
		}

		if (snapFilesArr.isEmpty()) {
			String fullFName = targetDir + AAIConstants.AAI_FILESEP + thisSnapPrefix;
			String emsg = "Snapshot files " + fullFName + "* could not be found.";
			LOGGER.error(emsg);
			System.out.println(emsg);
			taskExit();
		}

		int fCount = snapFilesArr.size();
		Iterator<File> fItr = snapFilesArr.iterator();
		Vector<InputStream> inputStreamsV = new Vector<>();
		for (int i = 0; i < fCount; i++) {
			File f = snapFilesArr.get(i);
			String fname = f.getName();
			if (!f.canRead()) {
				String emsg = "Snapshot file " + fname + " could not be read.";
				LOGGER.error(emsg);
				System.out.println(emsg);
				taskExit();
			} else if (f.length() == 0) {
				String emsg = "Snapshot file " + fname + " had no data.";
				LOGGER.error(emsg);
				System.out.println(emsg);
				taskExit();
			}
			String fullFName = targetDir + AAIConstants.AAI_FILESEP + fname;
			InputStream fis = null;
			try {
				fis = new FileInputStream(fullFName);
			} catch (FileNotFoundException e) {
				// should not happen at this point
				String emsg = "Snapshot file " + fullFName + " could not be found";
				LOGGER.error(emsg);
				System.out.println(emsg);
				taskExit();
			}
			inputStreamsV.add(fis);
		}
		// Now add inputStreams.elements() to the Vector,
		InputStream sis = new SequenceInputStream(inputStreamsV.elements());
		return (sis);
	}

	public InMemoryGraph getInMemGraph() {
		return inMemGraph;
	}

	public void setInMemGraph(InMemoryGraph inMemGraph) {
		this.inMemGraph = inMemGraph;
	}
}

class CommandLineArgs {

	@Parameter(names = "--help", help = true)
	public boolean help;

	@Parameter(names = "-d", description = "snapshot file to be loaded", required = true)
	public String dataSnapshot;

	@Parameter(names = "-s", description = "is schema to be enabled ", arity = 1)
	public boolean schemaEnabled = true;

	@Parameter(names = "-c", description = "location of configuration file")
	public String config = "";

	@Parameter(names = "-o", description = "output location")
	public String output = "";

	@Parameter(names = "-f", description = "format of output")
	public String format = "PAYLOAD";

	@Parameter(names = "-n", description = "Node input file")
	public String nodePropertyFile = "";

	@Parameter(names = "-m", description = "multipe snapshots or not", arity = 1)
	public boolean isMultipleSnapshot = false;

	@Parameter(names = "-i", description = "input filter configuration file")
	public String inputFilterPropertyFile = "";

	@Parameter(names = "-p", description = "Use the partial graph", arity = 1)
	public boolean isPartialGraph = true;

}
