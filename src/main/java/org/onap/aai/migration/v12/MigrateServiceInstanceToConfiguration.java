package org.onap.aai.migration.v12;

import java.io.UnsupportedEncodingException;
import java.util.Iterator;
import java.util.Optional;
import java.util.UUID;

import org.apache.tinkerpop.gremlin.process.traversal.P;
import org.apache.tinkerpop.gremlin.process.traversal.dsl.graph.GraphTraversal;
import org.apache.tinkerpop.gremlin.structure.Direction;
import org.apache.tinkerpop.gremlin.structure.Vertex;
import org.onap.aai.db.props.AAIProperties;
import org.onap.aai.edges.EdgeIngestor;
import org.onap.aai.exceptions.AAIException;
import org.onap.aai.introspection.Introspector;
import org.onap.aai.introspection.LoaderFactory;
import org.onap.aai.introspection.exceptions.AAIUnknownObjectException;
import org.onap.aai.migration.MigrationDangerRating;
import org.onap.aai.migration.MigrationPriority;
import org.onap.aai.migration.Migrator;
import org.onap.aai.migration.Status;
import org.onap.aai.edges.enums.EdgeType;
import org.onap.aai.serialization.db.EdgeSerializer;
import org.onap.aai.serialization.engines.TransactionalGraphEngine;
import org.onap.aai.setup.SchemaVersions;

//@Enabled
@MigrationPriority(10)
@MigrationDangerRating(10)
public class MigrateServiceInstanceToConfiguration extends Migrator {

	private boolean success = true;
	private final String CONFIGURATION_NODE_TYPE = "configuration";
	private final String SERVICE_INSTANCE_NODE_TYPE = "service-instance";
	private Introspector configObj;

	public MigrateServiceInstanceToConfiguration(TransactionalGraphEngine engine, LoaderFactory loaderFactory, EdgeIngestor edgeIngestor, EdgeSerializer edgeSerializer, SchemaVersions schemaVersions) {
		super(engine, loaderFactory, edgeIngestor, edgeSerializer, schemaVersions);
		try {
			this.configObj = this.loader.introspectorFromName(CONFIGURATION_NODE_TYPE);
		} catch (AAIUnknownObjectException e) {
			this.configObj = null;
		}
	}

	@Override
	public void run() {		
		Vertex serviceInstance = null;
		Vertex configuration = null;
		String serviceInstanceId = "", tunnelBandwidth = "";
		String bandwidthTotal, configType, nodeType;
		GraphTraversal<Vertex, Vertex> serviceInstanceItr;
		Iterator<Vertex> configurationItr;

		try {
			serviceInstanceItr = this.engine.asAdmin().getTraversalSource().V()
					.has(AAIProperties.NODE_TYPE, P.within(getAffectedNodeTypes().get()))
					.where(this.engine.getQueryBuilder()
							.createEdgeTraversal(EdgeType.TREE, "service-instance", "service-subscription")
							.getVerticesByProperty("service-type", "DHV")
							.<GraphTraversal<?, ?>>getQuery());

			if (serviceInstanceItr == null || !serviceInstanceItr.hasNext()) {
				logger.info("No servince-instance nodes found with service-type of DHV");
				return;
			}

			// iterate through all service instances of service-type DHV
			while (serviceInstanceItr.hasNext()) {
				serviceInstance = serviceInstanceItr.next();

				if (serviceInstance != null && serviceInstance.property("bandwidth-total").isPresent()) {
					serviceInstanceId = serviceInstance.value("service-instance-id");
					logger.info("Processing service instance with id=" + serviceInstanceId);
					bandwidthTotal = serviceInstance.value("bandwidth-total");

					if (bandwidthTotal != null && !bandwidthTotal.isEmpty()) {

						// check for existing edges to configuration nodes 
						configurationItr = serviceInstance.vertices(Direction.OUT, "has");

						// create new configuration node if service-instance does not have existing ones
						if (!configurationItr.hasNext()) {
							logger.info(serviceInstanceId + " has no existing configuration nodes, creating new node");
							createConfigurationNode(serviceInstance, bandwidthTotal);
							continue;
						}

						// in case if configuration nodes exist, but none are DHV
						boolean hasDHVConfig = false;

						// service-instance has existing configuration nodes
						while (configurationItr.hasNext()) {
							configuration = configurationItr.next();
							nodeType = configuration.value("aai-node-type").toString();

							if (configuration != null && "configuration".equalsIgnoreCase(nodeType)) {
								logger.info("Processing configuration node with id=" + configuration.property("configuration-id").value());
								configType = configuration.value("configuration-type");
								logger.info("Configuration type: " + configType);

								// if configuration-type is DHV, update tunnel-bandwidth to bandwidth-total value
								if ("DHV".equalsIgnoreCase(configType)) {
									if (configuration.property("tunnel-bandwidth").isPresent()) {
										tunnelBandwidth = configuration.value("tunnel-bandwidth");
									} else {
										tunnelBandwidth = "";
									}

									logger.info("Existing tunnel-bandwidth: " + tunnelBandwidth);
									configuration.property("tunnel-bandwidth", bandwidthTotal);
									touchVertexProperties(configuration, false);
									logger.info("Updated tunnel-bandwidth: " + configuration.value("tunnel-bandwidth"));
									hasDHVConfig = true;
								}
							}
						}

						// create new configuration node if none of existing config nodes are of type DHV 
						if (!hasDHVConfig) {
							logger.info(serviceInstanceId + " has existing configuration nodes, but none are DHV, create new node");
							createConfigurationNode(serviceInstance, bandwidthTotal);
						}
					}
				}
			}
		} catch (AAIException | UnsupportedEncodingException e) {
			logger.error("Caught exception while processing service instance with id=" + serviceInstanceId + " | " + e.toString());
			success = false;
		}
	}

	private void createConfigurationNode(Vertex serviceInstance, String bandwidthTotal) throws UnsupportedEncodingException, AAIException {
		// create new vertex
		Vertex configurationNode = serializer.createNewVertex(configObj);

		// configuration-id: UUID format
		String configurationUUID = UUID.randomUUID().toString();
		configObj.setValue("configuration-id", configurationUUID);

		// configuration-type: DHV
		configObj.setValue("configuration-type", "DHV");

		// migrate the bandwidth-total property from the service-instance to the 
		// tunnel-bandwidth property of the related configuration object
		configObj.setValue("tunnel-bandwidth", bandwidthTotal);

		// create edge between service instance and configuration: cousinEdge(out, in)
		createCousinEdge(serviceInstance, configurationNode);

		// serialize edge & vertex, takes care of everything
		serializer.serializeSingleVertex(configurationNode, configObj, "migrations");
		logger.info("Created configuration node with uuid=" + configurationUUID + ", tunnel-bandwidth=" + bandwidthTotal);
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
		return Optional.of(new String[] {SERVICE_INSTANCE_NODE_TYPE});
	}

	@Override
	public String getMigrationName() {
		return "service-instance-to-configuration";
	}
}
