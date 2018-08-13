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

import org.apache.tinkerpop.gremlin.process.traversal.P;
import org.apache.tinkerpop.gremlin.process.traversal.dsl.graph.GraphTraversal;
import org.apache.tinkerpop.gremlin.process.traversal.dsl.graph.__;
import org.apache.tinkerpop.gremlin.structure.Graph;
import org.apache.tinkerpop.gremlin.structure.Vertex;
import org.javatuples.Pair;
import org.onap.aai.exceptions.AAIException;
import org.onap.aai.rest.dsl.DslQueryProcessor;
import org.onap.aai.restcore.search.GroovyQueryBuilderSingleton;
import org.onap.aai.restcore.util.URITools;
import org.onap.aai.serialization.engines.TransactionalGraphEngine;
import org.onap.aai.serialization.queryformats.SubGraphStyle;

import javax.ws.rs.core.MultivaluedHashMap;
import javax.ws.rs.core.MultivaluedMap;
import java.io.FileNotFoundException;
import java.net.URI;
import java.util.*;
import java.util.regex.Matcher;
import java.util.regex.Pattern;

public abstract class GenericQueryProcessor {

	protected final Optional<URI> uri;
	protected final MultivaluedMap<String, String> queryParams;
	protected final Optional<Collection<Vertex>> vertices;
	protected static Pattern p = Pattern.compile("query/(.*+)");
	protected Optional<String> gremlin;
	protected final TransactionalGraphEngine dbEngine;
	protected static GroovyQueryBuilderSingleton queryBuilderSingleton = GroovyQueryBuilderSingleton.getInstance();
	protected final boolean isGremlin;
	protected Optional<DslQueryProcessor> dslQueryProcessorOptional;
	/* dsl parameters to store dsl query and to check
	 * if this is a DSL request
	 */
	protected Optional<String> dsl;
	protected final boolean isDsl ;
	
	protected GenericQueryProcessor(Builder builder) {
		this.uri = builder.getUri();
		this.dbEngine = builder.getDbEngine();
		this.vertices = builder.getVertices();
		this.gremlin = builder.getGremlin();
		this.isGremlin = builder.isGremlin();
		this.dsl = builder.getDsl();
		this.isDsl = builder.isDsl();
		this.dslQueryProcessorOptional = builder.getDslQueryProcessor();
		
		if (uri.isPresent()) {
			queryParams = URITools.getQueryMap(uri.get());
		} else {
			queryParams = new MultivaluedHashMap<>();
		}
	}
	
	protected abstract GraphTraversal<?,?> runQuery(String query, Map<String, Object> params);
	
	protected List<Object> processSubGraph(SubGraphStyle style, GraphTraversal<?,?> g) {
		final List<Object> resultVertices = new Vector<>();
		g.store("y");
		
		if (SubGraphStyle.prune.equals(style) || SubGraphStyle.star.equals(style)) {
			g.barrier().bothE();
			if (SubGraphStyle.prune.equals(style)) {
				g.where(__.otherV().where(P.within("y")));
			}
			g.dedup().subgraph("subGraph").cap("subGraph").map(x -> (Graph)x.get()).next().traversal().V().forEachRemaining(x -> {
				resultVertices.add(x);
			});
		} else {
			resultVertices.addAll(g.toList());
		}
		return resultVertices;
	}
	
	public List<Object> execute(SubGraphStyle style) throws FileNotFoundException, AAIException {
		final List<Object> resultVertices;

		Pair<String, Map<String, Object>> tuple = this.createQuery();
		String query = tuple.getValue0();
		Map<String, Object> params = tuple.getValue1();

		if (query.equals("") && (vertices.isPresent() && vertices.get().isEmpty())) {
			//nothing to do, just exit
			return new ArrayList<>();
		}
		GraphTraversal<?,?> g = this.runQuery(query, params);
		
		resultVertices = this.processSubGraph(style, g);
		
		return resultVertices;
	}
	
	protected Pair<String, Map<String, Object>> createQuery() throws AAIException {
		Map<String, Object> params = new HashMap<>();
		String query = "";
		 if (this.isGremlin) {
			query = gremlin.get();
			
		}else if (this.isDsl) {
			String dslUserQuery = dsl.get();
			if(dslQueryProcessorOptional.isPresent()){
				String dslQuery = dslQueryProcessorOptional.get().parseAaiQuery(dslUserQuery);
				query = queryBuilderSingleton.executeTraversal(dbEngine, dslQuery, params);
				String startPrefix = "g.V()";
				query = startPrefix + query;
			}
		}
		
		return new Pair<>(query, params);
	}
	
	public static class Builder {

		private final TransactionalGraphEngine dbEngine;
		private Optional<URI> uri = Optional.empty();
		private Optional<String> gremlin = Optional.empty();
		private boolean isGremlin = false;
		private Optional<Collection<Vertex>> vertices = Optional.empty();
		private QueryProcessorType processorType = QueryProcessorType.GREMLIN_SERVER;
		
		private Optional<String> dsl = Optional.empty();
		private boolean isDsl = false;
		private DslQueryProcessor dslQueryProcessor;

		public Builder(TransactionalGraphEngine dbEngine) {
			this.dbEngine = dbEngine;
		}
		
		public Builder queryFrom(URI uri) {
			this.uri = Optional.of(uri);
			this.isGremlin = false;
			return this;
		}
		
		public Builder startFrom(Collection<Vertex> vertices) {
			this.vertices = Optional.of(vertices);
			return this;
		}
		
		public Builder queryFrom( String query, String queryType) {
			
			if(queryType.equals("gremlin")){
				this.gremlin = Optional.of(query);
				this.isGremlin = true;
			}
			if(queryType.equals("dsl")){
				this.dsl = Optional.of(query);
				this.isDsl = true;
			}
			return this;
		}
		
		public Builder processWith(QueryProcessorType type) {
			this.processorType = type;
			return this;
		}

		public Builder queryProcessor(DslQueryProcessor dslQueryProcessor){
			this.dslQueryProcessor = dslQueryProcessor;
			return this;
		}

		public Optional<DslQueryProcessor> getDslQueryProcessor(){
			return Optional.ofNullable(this.dslQueryProcessor);
		}

		public TransactionalGraphEngine getDbEngine() {
			return dbEngine;
		}

		public Optional<URI> getUri() {
			return uri;
		}

		public Optional<String> getGremlin() {
			return gremlin;
		}

		public boolean isGremlin() {
			return isGremlin;
		}
		
		public Optional<String> getDsl() {
			return dsl;
		}

		public boolean isDsl() {
			return isDsl;
		}

		public Optional<Collection<Vertex>> getVertices() {
			return vertices;
		}
		
		public QueryProcessorType getProcessorType() {
			return processorType;
		}
		
		public GenericQueryProcessor create() {
			return new GroovyShellImpl(this);
		}
		
	}
}
