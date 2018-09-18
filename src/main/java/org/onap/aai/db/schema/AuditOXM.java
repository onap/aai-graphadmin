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

import com.att.eelf.configuration.EELFLogger;
import com.att.eelf.configuration.EELFManager;
import com.google.common.collect.Multimap;
import org.janusgraph.core.Cardinality;
import org.janusgraph.core.Multiplicity;
import org.janusgraph.core.schema.SchemaStatus;
import org.onap.aai.db.props.AAIProperties;
import org.onap.aai.edges.EdgeIngestor;
import org.onap.aai.edges.EdgeRule;
import org.onap.aai.edges.exceptions.EdgeRuleNotFoundException;
import org.onap.aai.introspection.Introspector;
import org.onap.aai.introspection.Loader;
import org.onap.aai.introspection.LoaderFactory;
import org.onap.aai.introspection.ModelType;
import org.onap.aai.introspection.exceptions.AAIUnknownObjectException;
import org.onap.aai.logging.LogFormatTools;
import org.onap.aai.schema.enums.ObjectMetadata;
import org.onap.aai.setup.SchemaVersion;

import java.util.*;
import java.util.stream.Collectors;

public class AuditOXM extends Auditor {

	private static final EELFLogger LOGGER = EELFManager.getInstance().getLogger(AuditOXM.class);

	private Set<Introspector> allObjects;
	private EdgeIngestor ingestor;

	/**
	 * Instantiates a new audit OXM.
	 *
	 * @param version the version
	 */
	public AuditOXM(LoaderFactory loaderFactory, SchemaVersion version, EdgeIngestor ingestor) {
		
		Loader loader = loaderFactory.createLoaderForVersion(ModelType.MOXY, version);
		Set<String> objectNames = getAllObjects(loader);
		this.ingestor = ingestor;

		allObjects = new HashSet<>();
		for (String key : objectNames) {
			try {
				final Introspector temp = loader.introspectorFromName(key);
				allObjects.add(temp);
				this.createDBProperties(temp);
			} catch (AAIUnknownObjectException e) {
				LOGGER.warn("Skipping audit for object " + key + " (Unknown Object) " + LogFormatTools.getStackTop(e));
			}
		}
		for (Introspector temp : allObjects) {
			this.createDBIndexes(temp);
		}
		try {
			createEdgeLabels();
		} catch (EdgeRuleNotFoundException e) {
			LOGGER.warn("Skipping audit for version " + version + " due to " + LogFormatTools.getStackTop(e));
		}

	}

	/**
	 * Gets the all objects.
	 *
	 * @param version the version
	 * @return the all objects
	 */
	private Set<String> getAllObjects(Loader loader) {

		Set<String> result = loader.getAllObjects().entrySet()
			.stream()
			.map(Map.Entry::getKey)
			.collect(Collectors.toSet());

		result.remove("EdgePropNames");
		return result;
	}
	
	/**
	 * Creates the DB properties.
	 *
	 * @param temp the temp
	 */
	private void createDBProperties(Introspector temp) {
		Set<String> objectProperties = temp.getProperties();
		
		for (String prop : objectProperties) {
			if (!properties.containsKey(prop)) {
				DBProperty dbProperty = new DBProperty();
				dbProperty.setName(prop);
				if (temp.isListType(prop)) {
					dbProperty.setCardinality(Cardinality.SET);
					if (temp.isSimpleGenericType(prop)) {
						Class<?> clazz = null;
						try {
							clazz = Class.forName(temp.getGenericType(prop));
						} catch (ClassNotFoundException e) {
							clazz = Object.class;
						}
						dbProperty.setTypeClass(clazz);
						properties.put(prop, dbProperty);
					}
				} else {
					dbProperty.setCardinality(Cardinality.SINGLE);
					if (temp.isSimpleType(prop)) {
						Class<?> clazz = null;
						try {
							clazz = Class.forName(temp.getType(prop));
						} catch (ClassNotFoundException e) {
							clazz = Object.class;
						}
						dbProperty.setTypeClass(clazz);
						properties.put(prop, dbProperty);
					}
				}
			}
		}
		
	}
	
	/**
	 * Creates the DB indexes.
	 *
	 * @param temp the temp
	 */
	private void createDBIndexes(Introspector temp) {
		String uniqueProps = temp.getMetadata(ObjectMetadata.UNIQUE_PROPS);
		String namespace = temp.getMetadata(ObjectMetadata.NAMESPACE);
		if (uniqueProps == null) {
			uniqueProps = "";
		}
		if (namespace == null) {
			namespace = "";
		}
		boolean isTopLevel = namespace != "";
		List<String> unique = Arrays.asList(uniqueProps.split(","));
		Set<String> indexed = temp.getIndexedProperties();
		Set<String> keys = temp.getKeys();
		
		for (String prop : indexed) {
			DBIndex dbIndex = new DBIndex();
			LinkedHashSet<DBProperty> properties = new LinkedHashSet<>();
			if (!this.indexes.containsKey(prop)) {
				dbIndex.setName(prop);
				dbIndex.setUnique(unique.contains(prop));
				properties.add(this.properties.get(prop));
				dbIndex.setProperties(properties);
				dbIndex.setStatus(SchemaStatus.ENABLED);
				this.indexes.put(prop, dbIndex);
			}
		}
		if (keys.size() > 1 || isTopLevel) {
			DBIndex dbIndex = new DBIndex();
			LinkedHashSet<DBProperty> properties = new LinkedHashSet<>();
			dbIndex.setName("key-for-" + temp.getDbName());
			if (!this.indexes.containsKey(dbIndex.getName())) {
				boolean isUnique = false;
				if (isTopLevel) {
					properties.add(this.properties.get(AAIProperties.NODE_TYPE));
				}
				for (String key : keys) {
					properties.add(this.properties.get(key));
	
					if (unique.contains(key) && !isUnique) {
						isUnique = true;
					}
				}
				dbIndex.setUnique(isUnique);
				dbIndex.setProperties(properties);
				dbIndex.setStatus(SchemaStatus.ENABLED);
				this.indexes.put(dbIndex.getName(), dbIndex);
			}
		}

	}
	
	/**
	 * Creates the edge labels.
	 */
	private void createEdgeLabels() throws EdgeRuleNotFoundException {
		Multimap<String, EdgeRule> edgeRules = ingestor.getAllCurrentRules();
		for (String key : edgeRules.keySet()) {
			Collection<EdgeRule> collection = edgeRules.get(key);
			EdgeProperty prop = new EdgeProperty();
			//there is only ever one, they used the wrong type for EdgeRules
			String label = "";
			for (EdgeRule item : collection) {
				label = item.getLabel();
			}
			prop.setName(label);
			prop.setMultiplicity(Multiplicity.MULTI);
			this.edgeLabels.put(label, prop);
		}
	}
	
	/**
	 * Gets the all introspectors.
	 *
	 * @return the all introspectors
	 */
	public Set<Introspector> getAllIntrospectors() {
		return this.allObjects;
	}
}
