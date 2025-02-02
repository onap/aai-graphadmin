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
package org.onap.aai.db.schema;

import org.janusgraph.core.JanusGraph;
import org.onap.aai.edges.EdgeIngestor;
import org.onap.aai.introspection.LoaderFactory;
import org.onap.aai.setup.SchemaVersion;

public class AuditorFactory {

	private LoaderFactory loaderFactory;

    public AuditorFactory(LoaderFactory loaderFactory){
        this.loaderFactory = loaderFactory;
	}
	/**
	 * Gets the OXM auditor.
	 *
	 * @param v the v
	 * @return the OXM auditor
	 */
	public Auditor getOXMAuditor (SchemaVersion v, EdgeIngestor ingestor) {
		return new AuditOXM(loaderFactory, v, ingestor);
	}
	
	/**
	 * Gets the graph auditor.
	 *
	 * @param g the g
	 * @return the graph auditor
	 */
	public Auditor getGraphAuditor (JanusGraph g) {
		return new AuditJanusGraph(g);
	}
}
