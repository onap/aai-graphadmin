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

import org.onap.aai.audit.AuditGraphson2Sql;
import org.onap.aai.concurrent.AaiCallable;
import org.onap.aai.exceptions.AAIException;
import org.onap.aai.restcore.HttpMethod;
import org.onap.aai.restcore.RESTAPI;
import org.onap.aai.util.AAIConstants;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.stereotype.Component;

import jakarta.ws.rs.*;
import jakarta.ws.rs.core.*;
import jakarta.ws.rs.core.Response.Status;

@Component
@Path("{version: v1}/audit-sql-db")
public class AuditSqlDbConsumer extends RESTAPI {

	private static String SOURCE_FILE_NAME = "sourceFileName";
	private static String SOURCE_FILE_DIR_DEFAULT = "logs/data/dataSnapshots/";

	private static final Logger LOGGER = LoggerFactory.getLogger(AuditSqlDbConsumer.class);

	private String rdbmsDbName;
	private AuditGraphson2Sql auditGraphson2Sql;

	public AuditSqlDbConsumer(
		AuditGraphson2Sql auditGraphson2Sql,
		@Value("${aperture.rdbmsname}") String rdbmsDbName
	){
		this.auditGraphson2Sql  = auditGraphson2Sql;
		this.rdbmsDbName		= rdbmsDbName;
	}


	@GET
	@Consumes({ MediaType.APPLICATION_JSON})
	@Produces({ MediaType.APPLICATION_JSON})
	public Response executeAudit(String content,
								 @PathParam("uri") @Encoded String uri,
								 @Context HttpHeaders headers,
								 @Context UriInfo info){
		return runner(AAIConstants.AAI_GRAPHADMIN_TIMEOUT_ENABLED,
				AAIConstants.AAI_GRAPHADMIN_TIMEOUT_APP,
				AAIConstants.AAI_GRAPHADMIN_TIMEOUT_LIMIT,
				headers,
				info,
				HttpMethod.GET,
				new AaiCallable<Response>() {
					@Override
					public Response process() {
						return processExecuteAudit(content, uri, headers, info);
					}
				}
		);
	}


	public Response processExecuteAudit(String content,
										@PathParam("uri") @Encoded String uri,
										@Context HttpHeaders headers,
										@Context UriInfo info) {

		Response response = null;
		try {
			this.checkParams(info.getQueryParameters());

			String resStr = auditGraphson2Sql.runAudit( rdbmsDbName,
					info.getQueryParameters().getFirst(SOURCE_FILE_NAME),
					SOURCE_FILE_DIR_DEFAULT );

			LOGGER.info ("Completed");
			
			response = Response.status(Status.OK)
					.type(MediaType.APPLICATION_JSON)
					.entity(resStr).build();
		
		} catch (AAIException e) {
			response = consumerExceptionResponseGenerator(headers, info, HttpMethod.GET, e);
		} catch (Exception e ) {
			AAIException ex = new AAIException("AAI_4000", e);
			response = consumerExceptionResponseGenerator(headers, info, HttpMethod.GET, ex);
		}
		
		return response;
	}


	public void checkParams(MultivaluedMap<String, String> params) throws AAIException {
		
		if (!params.containsKey(SOURCE_FILE_NAME)) {
			throw new AAIException("AAI_6120", "parameter: sourceFileName (of snapshot file) is required. ");
		}
	}

}
