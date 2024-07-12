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
package org.onap.aai.rest;

import com.google.gson.JsonElement;
import com.google.gson.JsonObject;
import com.google.gson.JsonParser;
import org.onap.aai.concurrent.AaiCallable;
import org.onap.aai.exceptions.AAIException;
import org.onap.aai.introspection.ModelType;
import org.onap.aai.logging.StopWatch;
import org.onap.aai.rest.db.HttpEntry;
import org.onap.aai.rest.dsl.DslQueryProcessor;
import org.onap.aai.rest.search.GenericQueryProcessor;
import org.onap.aai.rest.search.QueryProcessorType;
import org.onap.aai.restcore.HttpMethod;
import org.onap.aai.restcore.RESTAPI;
import org.onap.aai.serialization.db.DBSerializer;
import org.onap.aai.serialization.engines.TransactionalGraphEngine;
import org.onap.aai.serialization.queryformats.Format;
import org.onap.aai.serialization.queryformats.FormatFactory;
import org.onap.aai.serialization.queryformats.Formatter;
import org.onap.aai.serialization.queryformats.SubGraphStyle;
import org.onap.aai.setup.SchemaVersion;
import org.onap.aai.setup.SchemaVersions;
import org.onap.aai.util.AAIConstants;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.beans.factory.annotation.Qualifier;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.stereotype.Component;

import javax.servlet.http.HttpServletRequest;
import javax.ws.rs.*;
import javax.ws.rs.core.*;
import javax.ws.rs.core.Response.Status;
import java.util.List;

@Component
@Path("{version: v[1-9][0-9]*|latest}/dbquery")
public class QueryConsumer extends RESTAPI {

	/** The introspector factory type. */
	private ModelType introspectorFactoryType = ModelType.MOXY;

	private QueryProcessorType processorType = QueryProcessorType.LOCAL_GROOVY;

	private static final String TARGET_ENTITY = "DB";
	private static final Logger LOGGER = LoggerFactory.getLogger(QueryConsumer.class);

	private HttpEntry traversalUriHttpEntry;

	private DslQueryProcessor dslQueryProcessor;

	private SchemaVersions schemaVersions;

	private String basePath;

	@Autowired
	public QueryConsumer(
		@Qualifier("traversalUriHttpEntry") HttpEntry traversalUriHttpEntry,
		DslQueryProcessor dslQueryProcessor,
		SchemaVersions schemaVersions,
		@Value("${schema.uri.base.path}") String basePath
	){
		this.traversalUriHttpEntry = traversalUriHttpEntry;
		this.dslQueryProcessor     = dslQueryProcessor;
		this.basePath 				= basePath;
		this.schemaVersions		    = schemaVersions;
	}


	@PUT
	@Consumes({ MediaType.APPLICATION_JSON})
	@Produces({ MediaType.APPLICATION_JSON})
	public Response executeQuery(String content, @PathParam("version")String versionParam, @PathParam("uri") @Encoded String uri, @DefaultValue("graphson") @QueryParam("format") String queryFormat,@DefaultValue("no_op") @QueryParam("subgraph") String subgraph, @Context HttpHeaders headers, @Context UriInfo info, @Context HttpServletRequest req){
		return runner(AAIConstants.AAI_GRAPHADMIN_TIMEOUT_ENABLED,
				AAIConstants.AAI_GRAPHADMIN_TIMEOUT_APP,
				AAIConstants.AAI_GRAPHADMIN_TIMEOUT_LIMIT,
				headers,
				info,
				HttpMethod.GET,
				new AaiCallable<Response>() {
					@Override
					public Response process() {
						return processExecuteQuery(content, versionParam, uri, queryFormat, subgraph, headers, info, req);
					}
				}
		);
	}

	public Response processExecuteQuery(String content, @PathParam("version")String versionParam, @PathParam("uri") @Encoded String uri, @DefaultValue("graphson") @QueryParam("format") String queryFormat,@DefaultValue("no_op") @QueryParam("subgraph") String subgraph, @Context HttpHeaders headers, @Context UriInfo info, @Context HttpServletRequest req) {

		String sourceOfTruth = headers.getRequestHeaders().getFirst("X-FromAppId");
		String queryProcessor = headers.getRequestHeaders().getFirst("QueryProcessor");
		QueryProcessorType processorType = this.processorType;
		Response response = null;
		TransactionalGraphEngine dbEngine = null;
		try {
			this.checkQueryParams(info.getQueryParameters());
			Format format = Format.getFormat(queryFormat);
			if (queryProcessor != null) {
				processorType = QueryProcessorType.valueOf(queryProcessor);
			}
			SubGraphStyle subGraphStyle = SubGraphStyle.valueOf(subgraph);
			JsonParser parser = new JsonParser();

			JsonObject input = parser.parse(content).getAsJsonObject();

			JsonElement gremlinElement = input.get("gremlin");
			JsonElement dslElement = input.get("dsl");
			String gremlin = "";
			String dsl = "";

			SchemaVersion version = new SchemaVersion(versionParam);
			traversalUriHttpEntry.setHttpEntryProperties(version);
			dbEngine = traversalUriHttpEntry.getDbEngine();

			if (gremlinElement != null) {
				gremlin = gremlinElement.getAsString();
			}
			if (dslElement != null) {
				dsl = dslElement.getAsString();
			}
			GenericQueryProcessor processor;

			StopWatch.conditionalStart();

			if(!dsl.equals("")){
				processor =  new GenericQueryProcessor.Builder(dbEngine)
						.queryFrom(dsl, "dsl")
						.queryProcessor(dslQueryProcessor)
						.processWith(processorType).create();
			}else {
				processor =  new GenericQueryProcessor.Builder(dbEngine)
						.queryFrom(gremlin, "gremlin")
						.processWith(processorType).create();
			}

			String result = "";
			List<Object> vertices = processor.execute(subGraphStyle);

			DBSerializer serializer = new DBSerializer(version, dbEngine, introspectorFactoryType, sourceOfTruth);
			FormatFactory ff = new FormatFactory(traversalUriHttpEntry.getLoader(), serializer, schemaVersions, basePath);

			Formatter formater =  ff.get(format, info.getQueryParameters());

			result = formater.output(vertices).toString();

			LOGGER.info ("Completed");

			response = Response.status(Status.OK)
					.type(MediaType.APPLICATION_JSON)
					.entity(result).build();

		} catch (AAIException e) {
			response = consumerExceptionResponseGenerator(headers, info, HttpMethod.GET, e);
		} catch (Exception e ) {
			AAIException ex = new AAIException("AAI_4000", e);
			response = consumerExceptionResponseGenerator(headers, info, HttpMethod.GET, ex);
		} finally {
			if (dbEngine != null) {
				dbEngine.rollback();
			}

		}

		return response;
	}

	public void checkQueryParams(MultivaluedMap<String, String> params) throws AAIException {

		if (params.containsKey("depth") && params.getFirst("depth").matches("\\d+")) {
			String depth = params.getFirst("depth");
			int i = Integer.parseInt(depth);
			if (i > 1) {
				throw new AAIException("AAI_3303");
			}
		}


	}

}
