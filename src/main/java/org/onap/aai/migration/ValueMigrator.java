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
package org.onap.aai.migration;

import java.util.Map;
import org.apache.tinkerpop.gremlin.process.traversal.dsl.graph.GraphTraversal;
import org.apache.tinkerpop.gremlin.structure.Vertex;
import org.onap.aai.db.props.AAIProperties;
import org.onap.aai.edges.EdgeIngestor;
import org.onap.aai.introspection.LoaderFactory;
import org.onap.aai.serialization.db.EdgeSerializer;
import org.onap.aai.serialization.engines.TransactionalGraphEngine;
import org.janusgraph.core.schema.JanusGraphManagement;
import org.onap.aai.setup.SchemaVersions;

/**
 * A migration template for filling in default values that are missing or are empty
 */
@MigrationPriority(0)
@MigrationDangerRating(1)
public abstract class ValueMigrator extends Migrator {

    protected final Map<String, Map<String, ?>> propertyValuePairByNodeType;
    protected final Boolean updateExistingValues;
	protected final JanusGraphManagement graphMgmt;

    /**
     *
     * @param engine
     * @param propertyValuePairByNodeType - format {nodeType: { property: newValue}}
     * @param updateExistingValues - if true, updates the value regardless if it already exists
     */
	public ValueMigrator(TransactionalGraphEngine engine, LoaderFactory loaderFactory, EdgeIngestor edgeIngestor, EdgeSerializer edgeSerializer, SchemaVersions schemaVersions, Map propertyValuePairByNodeType, Boolean updateExistingValues) {
		super(engine, loaderFactory, edgeIngestor, edgeSerializer, schemaVersions);
	    this.propertyValuePairByNodeType = propertyValuePairByNodeType;
	    this.updateExistingValues = updateExistingValues;
		this.graphMgmt = engine.asAdmin().getManagementSystem();
	}

	/**
	 * Do not override this method as an inheritor of this class
	 */
	@Override
	public void run() {
	    updateValues();
	}

    protected void updateValues() {
        for (Map.Entry<String, Map<String, ?>> entry: propertyValuePairByNodeType.entrySet()) {
            String nodeType = entry.getKey();
            Map<String, ?> propertyValuePair = entry.getValue();
            for (Map.Entry<String, ?> pair : propertyValuePair.entrySet()) {
                String property = pair.getKey();
                Object newValue = pair.getValue();
                try {
                    GraphTraversal<Vertex, Vertex> g = this.engine.asAdmin().getTraversalSource().V()
                            .has(AAIProperties.NODE_TYPE, nodeType);
                    while (g.hasNext()) {
                        Vertex v = g.next();
                        if (v.property(property).isPresent() && !updateExistingValues) {
                            String propertyValue = v.property(property).value().toString();
                            if (propertyValue.isEmpty()) {
                                v.property(property, newValue);
                                logger.info(String.format("Node Type %s: Property %s is empty, adding value %s",
                                        nodeType, property, newValue.toString()));
                                this.touchVertexProperties(v, false);
                            } else {
                                logger.info(String.format("Node Type %s: Property %s value already exists - skipping",
                                        nodeType, property));
                            }
                        } else {
                            logger.info(String.format("Node Type %s: Property %s does not exist or " +
                                    "updateExistingValues flag is set to True - adding the property with value %s",
                                    nodeType, property, newValue.toString()));
                            v.property(property, newValue);
                            this.touchVertexProperties(v, false);
                        }
                    }
                } catch (Exception e) {
                    logger.error(String.format("caught exception updating aai-node-type %s's property %s's value to " +
                            "%s: %s", nodeType, property, newValue.toString(), e.getMessage()));
                    logger.error(e.getMessage());
                }
            }
        }
    }
}
