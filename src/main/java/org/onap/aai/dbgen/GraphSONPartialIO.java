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

import org.apache.tinkerpop.gremlin.structure.Graph;
import org.apache.tinkerpop.gremlin.structure.io.Io;
import org.apache.tinkerpop.gremlin.structure.io.IoRegistry;
import org.apache.tinkerpop.gremlin.structure.io.Mapper;
import org.apache.tinkerpop.gremlin.structure.io.graphson.GraphSONMapper;
import org.apache.tinkerpop.gremlin.structure.io.graphson.GraphSONReader;
import org.apache.tinkerpop.gremlin.structure.io.graphson.GraphSONVersion;
import org.apache.tinkerpop.gremlin.structure.io.graphson.GraphSONWriter;

import java.io.FileInputStream;
import java.io.FileOutputStream;
import java.io.IOException;
import java.io.InputStream;
import java.io.OutputStream;
import java.util.Optional;
import java.util.function.Consumer;

/**
 * Constructs GraphSON IO implementations given a {@link Graph} and {@link IoRegistry}. Implementers of the {@link Graph}
 * interfaces should see the {@link GraphSONMapper} for information on the expectations for the {@link IoRegistry}.
 *
 * @author Stephen Mallette (http://stephen.genoprime.com)
 */
public final class GraphSONPartialIO implements Io<GraphSONPartialReader.Builder, GraphSONWriter.Builder, GraphSONMapper.Builder> {
    private final IoRegistry registry;
    private final Graph graph;
    private final Optional<Consumer<Mapper.Builder>> onMapper;
    private final GraphSONVersion version;

    private GraphSONPartialIO(final Builder builder) {
        this.registry = builder.registry;
        this.graph = builder.graph;
        this.onMapper = Optional.ofNullable(builder.onMapper);
        this.version = builder.version;
    }

    /**
     * {@inheritDoc}
     */
    @Override
    public GraphSONPartialReader.Builder reader() {
        return GraphSONPartialReader.build().mapper(mapper().create());
    }

    /**
     * {@inheritDoc}
     */
    @Override
    public GraphSONWriter.Builder writer() {
        return GraphSONWriter.build().mapper(mapper().create());
    }

    /**
     * {@inheritDoc}
     */
    @Override
    public GraphSONMapper.Builder mapper() {
        final GraphSONMapper.Builder builder = (null == this.registry) ?
                GraphSONMapper.build().version(version) : GraphSONMapper.build().version(version).addRegistry(this.registry);
        onMapper.ifPresent(c -> c.accept(builder));
        return builder;
    }

    /**
     * {@inheritDoc}
     */
    @Override
    public void writeGraph(final String file) throws IOException {
        try (final OutputStream out = new FileOutputStream(file)) {
            writer().create().writeGraph(out, graph);
        }
    }

    /**
     * {@inheritDoc}
     */
    @Override
    public void readGraph(final String file) throws IOException {
        try (final InputStream in = new FileInputStream(file)) {
            reader().create().readGraph(in, graph);
        }
    }

    /**
     * Create a new builder using the default version of GraphSON.
     */
    public static Io.Builder<GraphSONPartialIO> build() {
        return build(GraphSONVersion.V1_0);
    }

    /**
     * Create a new builder using the specified version of GraphSON.
     */
    public static Io.Builder<GraphSONPartialIO> build(final GraphSONVersion version) {
        return new Builder(version);
    }

    public final static class Builder implements Io.Builder<GraphSONPartialIO> {

        private IoRegistry registry = null;
        private Graph graph;
        private Consumer<Mapper.Builder> onMapper = null;
        private final GraphSONVersion version;

        Builder(final GraphSONVersion version) {
            this.version = version;
        }

        /**
         * @deprecated As of release 3.2.2, replaced by {@link #onMapper(Consumer)}.
         */
        @Deprecated
        @Override
        public Io.Builder<GraphSONPartialIO> registry(final IoRegistry registry) {
            this.registry = registry;
            return this;
        }

        @Override
        public Io.Builder<? extends Io> onMapper(final Consumer<Mapper.Builder> onMapper) {
            this.onMapper = onMapper;
            return this;
        }

        @Override
        public Io.Builder<GraphSONPartialIO> graph(final Graph g) {
            this.graph = g;
            return this;
        }

        @Override
        public GraphSONPartialIO create() {
            if (null == graph) throw new IllegalArgumentException("The graph argument was not specified");
            return new GraphSONPartialIO(this);
        }
    }
}
