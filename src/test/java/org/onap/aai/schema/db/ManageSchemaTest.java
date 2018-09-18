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
package org.onap.aai.schema.db;

import org.codehaus.jackson.JsonParseException;
import org.codehaus.jackson.map.JsonMappingException;
import org.codehaus.jackson.map.ObjectMapper;
import org.janusgraph.core.JanusGraph;
import org.janusgraph.core.JanusGraphFactory;
import org.janusgraph.core.schema.JanusGraphManagement;
import org.junit.Before;
import org.junit.Ignore;
import org.junit.Test;
import org.onap.aai.AAISetup;
import org.onap.aai.db.schema.DBIndex;
import org.onap.aai.db.schema.ManageJanusGraphSchema;

import java.io.IOException;
import java.util.Set;

@Ignore("not ready yet")
public class ManageSchemaTest extends AAISetup {

	private JanusGraph graph = null;

	@Before
	public void beforeTest() {
		graph = JanusGraphFactory.open("bundleconfig-local/etc/appprops/aaiconfig.properties");
	}
	
	/*
	@Test
	public void populateEmptyGraph() {
		ManageJanusGraphSchema schema = new ManageJanusGraphSchema(graph);
		schema.buildSchema();
	}
	
	@Test
	public void modifyIndex() {
		ManageJanusGraphSchema schema = new ManageJanusGraphSchema(graph);
		schema.buildSchema();
		Vertex v = graph.addVertex();
		v.setProperty("aai-node-type", "pserver");
		v.setProperty("hostname", "test1");
		v.setProperty("internet-topology", "test2");
		graph.commit();
		DBIndex index = new DBIndex();
		index.setName("internet-topology");
		index.setUnique(false);
		schema.updateIndex(index);
		
	}
	*/
	@Test
	public void closeRunningInstances() {
		
		JanusGraphManagement mgmt = graph.openManagement();
 		Set<String> instances = mgmt.getOpenInstances();
		
		for (String instance : instances) {
			
			if (!instance.contains("(current)")) {
				mgmt.forceCloseInstance(instance);
			}
		}
		mgmt.commit();
		
		graph.close();
		
	}
	@Test
	public void addNewIndex() throws JsonParseException, JsonMappingException, IOException {
		ObjectMapper mapper = new ObjectMapper();
		String content = " {\r\n" + 
				"    \"name\" : \"equipment-name\",\r\n" + 
				"    \"unique\" : false,\r\n" + 
				"    \"properties\" : [ {\r\n" + 
				"      \"name\" : \"equipment-name\",\r\n" + 
				"      \"cardinality\" : \"SINGLE\",\r\n" + 
				"      \"typeClass\" : \"java.lang.String\"\r\n" + 
				"    } ]\r\n" + 
				"  }";
		DBIndex index = mapper.readValue(content, DBIndex.class);
		ManageJanusGraphSchema schema = new ManageJanusGraphSchema(graph, auditorFactory, schemaVersions, edgeIngestor);
		JanusGraphManagement mgmt = graph.openManagement();
		Set<String> instances = mgmt.getOpenInstances();
		System.out.println(instances);
		schema.updateIndex(index);
		
		graph.close();
		
	}
	
}
