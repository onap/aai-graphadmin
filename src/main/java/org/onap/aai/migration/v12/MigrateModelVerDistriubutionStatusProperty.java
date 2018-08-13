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
package org.onap.aai.migration.v12;
import org.apache.tinkerpop.gremlin.process.traversal.dsl.graph.GraphTraversal;
import org.apache.tinkerpop.gremlin.structure.Vertex;
import org.onap.aai.db.props.AAIProperties;
import org.onap.aai.edges.EdgeIngestor;
import org.onap.aai.introspection.LoaderFactory;
import org.onap.aai.migration.*;
import org.onap.aai.serialization.db.EdgeSerializer;
import org.onap.aai.serialization.engines.TransactionalGraphEngine;
import org.onap.aai.setup.SchemaVersions;

import java.util.Optional;

@MigrationPriority(20)
@MigrationDangerRating(2)
public class MigrateModelVerDistriubutionStatusProperty extends Migrator{

    private final String PARENT_NODE_TYPE = "model-ver";
    private boolean success = true;

    public MigrateModelVerDistriubutionStatusProperty(TransactionalGraphEngine engine, LoaderFactory loaderFactory, EdgeIngestor edgeIngestor, EdgeSerializer edgeSerializer, SchemaVersions schemaVersions) {
        super(engine, loaderFactory, edgeIngestor, edgeSerializer, schemaVersions);
    }



    @Override
    public void run() {


        GraphTraversal<Vertex, Vertex> f = this.engine.asAdmin().getTraversalSource().V().has(AAIProperties.NODE_TYPE,"model-ver");

        while(f.hasNext()) {
            Vertex v = f.next();
            try {
                    v.property("distribution-status", "DISTRIBUTION_COMPLETE_OK");
                    logger.info("changed model-ver.distribution-status property value for model-version-id: " + v.property("model-version-id").value());

            } catch (Exception e) {
                e.printStackTrace();
                success = false;
                logger.error("encountered exception for model-version-id:" + v.property("model-version-id").value(), e);
            }
        }
    }


    @Override
    public Status getStatus() {
        if (success) {
            return Status.SUCCESS;
        } else {
            return Status.FAILURE;
        }
    }
    @Override
    public Optional<String[]> getAffectedNodeTypes() {
        return Optional.of(new String[]{PARENT_NODE_TYPE});
    }

    @Override
    public String getMigrationName() {
        return "MigrateModelVerDistriubutionStatusProperty";
    }

}
