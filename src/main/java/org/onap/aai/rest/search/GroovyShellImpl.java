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
package org.onap.aai.rest.search;

import org.apache.tinkerpop.gremlin.process.traversal.dsl.graph.GraphTraversal;
import org.onap.aai.aailog.logs.AaiDBGraphadminMetricLog;
import org.onap.aai.restcore.search.GremlinGroovyShell;
import org.onap.aai.util.AAIConstants;
import org.onap.aai.util.GraphAdminConstants;

import java.util.Map;

public class GroovyShellImpl extends GenericQueryProcessor {

	protected GroovyShellImpl(Builder builder) {
		super(builder);
	}
	
	@Override
	protected GraphTraversal<?,?> runQuery(String query, Map<String, Object> params) {
		
		AaiDBGraphadminMetricLog metricLog = new AaiDBGraphadminMetricLog (GraphAdminConstants.AAI_GRAPHADMIN_MS);
		metricLog.pre(uri);
		
		params.put("g", this.dbEngine.asAdmin().getTraversalSource());
		
		GremlinGroovyShell shell = new GremlinGroovyShell();
		
		GraphTraversal<?,?> graphTraversal = shell.executeTraversal(query, params);
		
		metricLog.post();
		return graphTraversal;
	}
		
}


