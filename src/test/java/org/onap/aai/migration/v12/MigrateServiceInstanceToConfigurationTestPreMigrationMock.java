package org.onap.aai.migration.v12;

import static org.junit.Assert.assertArrayEquals;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertNotNull;
import static org.mockito.Mockito.spy;
import static org.mockito.Mockito.when;

import java.util.Optional;

import org.apache.tinkerpop.gremlin.process.traversal.dsl.graph.GraphTraversalSource;
import org.apache.tinkerpop.gremlin.process.traversal.strategy.verification.ReadOnlyStrategy;
import org.apache.tinkerpop.gremlin.structure.Vertex;
import org.junit.*;
import org.onap.aai.AAISetup;
import org.onap.aai.dbmap.DBConnectionType;
import org.onap.aai.introspection.Loader;
import org.onap.aai.introspection.LoaderFactory;
import org.onap.aai.introspection.ModelType;
import org.onap.aai.setup.SchemaVersions;
import org.onap.aai.setup.SchemaVersion;
import org.onap.aai.serialization.engines.QueryStyle;
import org.onap.aai.serialization.engines.JanusGraphDBEngine;
import org.onap.aai.serialization.engines.TransactionalGraphEngine;

import org.janusgraph.core.JanusGraphFactory;
import org.janusgraph.core.JanusGraph;
import org.janusgraph.core.JanusGraphTransaction;

public class MigrateServiceInstanceToConfigurationTestPreMigrationMock extends AAISetup {

	private final static ModelType introspectorFactoryType = ModelType.MOXY;
	private final static QueryStyle queryStyle = QueryStyle.TRAVERSAL;
	private final static DBConnectionType type = DBConnectionType.REALTIME;

	private Loader loader;
	private TransactionalGraphEngine dbEngine;
	private JanusGraph graph;
	private MigrateServiceInstanceToConfiguration migration;
	private JanusGraphTransaction tx;
	private GraphTraversalSource g;

	@Before
	public void setUp() throws Exception {
		graph = JanusGraphFactory.build().set("storage.backend","inmemory").open();
		tx = graph.newTransaction();
		g = tx.traversal();
		loader = loaderFactory.createLoaderForVersion(introspectorFactoryType, schemaVersions.getDefaultVersion());
		dbEngine = new JanusGraphDBEngine(
				queryStyle,
				type,
				loader);

		Vertex customer = g.addV()
				.property("aai-node-type", "customer")
				.property("global-customer-id", "customer-9972-BandwidthMigration")
				.property("subscriber-type", "CUST")
				.next();
		
		Vertex servSubSDNEI = g.addV()
				.property("aai-node-type", "service-subscription")
				.property("service-type", "SDN-ETHERNET-INTERNET")
				.next();
		
		Vertex servInstance22 = g.addV()
				.property("aai-node-type", "service-instance")
				.property("service-instance-id", "servInstance-9972-22-BandwidthMigration")
				.property("operational-status", "activated")
				.property("bandwidth-total", "bandwidth-total-22-BandwidthMigration")
				.next();
		
		Vertex servInstance11 = g.addV()
				.property("aai-node-type", "service-instance")
				.property("service-instance-id", "servInstance-9972-11-BandwidthMigration")
				.property("operational-status", "activated")
				.property("bandwidth-total", "bandwidth-total-11-BandwidthMigration")
				.next();
		
		Vertex servSubDHV = g.addV()
				.property("aai-node-type", "service-subscription")
				.property("service-type", "DHV")
				.next();
		
		Vertex servInstance4 = g.addV()
				.property("aai-node-type", "service-instance")
				.property("service-instance-id", "servInstance-9972-4-BandwidthMigration")
				.property("operational-status", "activated")
				.property("bandwidth-total", "bandwidth-total-4-BandwidthMigration")
				.next();
		
		Vertex servInstance1 = g.addV()
				.property("aai-node-type", "service-instance")
				.property("service-instance-id", "ServInstance-9972-1-BandwidthMigration")
				.property("operational-status", "activated")
				.property("bandwidth-total", "2380")
				.next();
		
		Vertex servInstance3 = g.addV()
				.property("aai-node-type", "service-instance")
				.property("service-instance-id", "servInstance-9972-3-BandwidthMigration")
				.property("operational-status", "activated")
				.property("bandwidth-total", "bandwidth-total-3-BandwidthMigration")
				.next();

		Vertex servInstance2 = g.addV()
				.property("aai-node-type", "service-instance")
				.property("service-instance-id", "servInstance-9972-2-BandwidthMigration")
				.property("operational-status", "activated")
				.property("bandwidth-total", "bandwidth-total-2-BandwidthMigration")
				.next();
		
		Vertex config1 = g.addV()
				.property("aai-node-type", "configuration")
				.property("configuration-id", "9972-config-LB1113")
				.property("configuration-type", "DHV")
				.property("tunnel-bandwidth", "12")
				.next();
		
		Vertex config2 = g.addV()
				.property("aai-node-type", "configuration")
				.property("configuration-id", "9972-1config-LB1113")
				.property("configuration-type", "configuration-type1-9972")
				.next();
		
		Vertex allottedResource = g.addV()
				.property("aai-node-type", "allotted-resource")
				.property("id", "allResource-9972-BandwidthMigration")
				.next();

		edgeSerializer.addTreeEdge(g, customer, servSubSDNEI);
		edgeSerializer.addTreeEdge(g, customer, servSubDHV);
		edgeSerializer.addTreeEdge(g, servSubSDNEI, servInstance22);
		edgeSerializer.addTreeEdge(g, servSubSDNEI, servInstance11);
		edgeSerializer.addTreeEdge(g, servSubDHV, servInstance4);
		edgeSerializer.addTreeEdge(g, servSubDHV, servInstance1);
		edgeSerializer.addTreeEdge(g, servSubDHV, servInstance3);
		edgeSerializer.addTreeEdge(g, servSubDHV, servInstance2);
		edgeSerializer.addEdge(g, servInstance1, allottedResource);
		edgeSerializer.addEdge(g, servInstance1, config1);
		edgeSerializer.addEdge(g, servInstance2, config2);

		TransactionalGraphEngine spy = spy(dbEngine);
		TransactionalGraphEngine.Admin adminSpy = spy(dbEngine.asAdmin());

		GraphTraversalSource traversal = g;
		GraphTraversalSource readOnly = tx.traversal(GraphTraversalSource.build().with(ReadOnlyStrategy.instance()));
		when (spy.tx()).thenReturn(tx);
		when(spy.asAdmin()).thenReturn(adminSpy);
		when(adminSpy.getTraversalSource()).thenReturn(traversal);
		when(adminSpy.getReadOnlyTraversalSource()).thenReturn(readOnly);
		
		migration = new MigrateServiceInstanceToConfiguration(spy, loaderFactory, edgeIngestor, edgeSerializer, schemaVersions);
		migration.run();
	}
	
	@After
	public void cleanUp() {
		tx.tx().rollback();
		graph.close();
	}
	
	@Test
	public void testRun() throws Exception {
		// check if graph nodes exist
		assertEquals("customer node exists", true, 
				g.V().has("global-customer-id", "customer-9972-BandwidthMigration")
				.hasNext());

		assertEquals("service subscription node, service-type=SDN-ETHERNET-INTERNET", true, 
				g.V().has("global-customer-id", "customer-9972-BandwidthMigration")
				.in("org.onap.relationships.inventory.BelongsTo").has("service-type", "SDN-ETHERNET-INTERNET")
				.hasNext());

		assertEquals("service instance node, bandwidth-total=bandwidth-total-22-BandwidthMigration", true, 
				g.V().has("global-customer-id", "customer-9972-BandwidthMigration")
				.in("org.onap.relationships.inventory.BelongsTo").has("service-type", "SDN-ETHERNET-INTERNET")
				.in("org.onap.relationships.inventory.BelongsTo").has("service-instance-id", "servInstance-9972-22-BandwidthMigration")
				.has("bandwidth-total", "bandwidth-total-22-BandwidthMigration")
				.hasNext());
		
		assertEquals("service instance node, bandwidth-total=bandwidth-total-11-BandwidthMigration", true, 
				g.V().has("global-customer-id", "customer-9972-BandwidthMigration")
				.in("org.onap.relationships.inventory.BelongsTo").has("service-type", "SDN-ETHERNET-INTERNET")
				.in("org.onap.relationships.inventory.BelongsTo").has("service-instance-id", "servInstance-9972-11-BandwidthMigration")
				.has("bandwidth-total", "bandwidth-total-11-BandwidthMigration")
				.hasNext());
		
		assertEquals("service subscription node, service-type=DHV", true, 
				g.V().has("global-customer-id", "customer-9972-BandwidthMigration")
				.in("org.onap.relationships.inventory.BelongsTo").has("service-type", "DHV")
				.hasNext());

		assertEquals("service instance node, bandwidth-total=servInstance-9972-4-BandwidthMigration", true, 
				g.V().has("global-customer-id", "customer-9972-BandwidthMigration")
				.in("org.onap.relationships.inventory.BelongsTo").has("service-type", "DHV")
				.in("org.onap.relationships.inventory.BelongsTo").has("service-instance-id", "servInstance-9972-4-BandwidthMigration")
				.has("bandwidth-total", "bandwidth-total-4-BandwidthMigration")
				.hasNext());
		
		assertEquals("service instance node, bandwidth-total=ServInstance-9972-1-BandwidthMigration", true, 
				g.V().has("global-customer-id", "customer-9972-BandwidthMigration")
				.in("org.onap.relationships.inventory.BelongsTo").has("service-type", "DHV")
				.in("org.onap.relationships.inventory.BelongsTo").has("service-instance-id", "ServInstance-9972-1-BandwidthMigration")
				.has("bandwidth-total", "2380")
				.hasNext());
		
		assertEquals("service instance node, bandwidth-total=servInstance-9972-3-BandwidthMigration", true, 
				g.V().has("global-customer-id", "customer-9972-BandwidthMigration")
				.in("org.onap.relationships.inventory.BelongsTo").has("service-type", "DHV")
				.in("org.onap.relationships.inventory.BelongsTo").has("service-instance-id", "servInstance-9972-3-BandwidthMigration")
				.has("bandwidth-total", "bandwidth-total-3-BandwidthMigration")
				.hasNext());
		
		assertEquals("service instance node, bandwidth-total=servInstance-9972-2-BandwidthMigration", true, 
				g.V().has("global-customer-id", "customer-9972-BandwidthMigration")
				.in("org.onap.relationships.inventory.BelongsTo").has("service-type", "DHV")
				.in("org.onap.relationships.inventory.BelongsTo").has("service-instance-id", "servInstance-9972-2-BandwidthMigration")
				.has("bandwidth-total", "bandwidth-total-2-BandwidthMigration")
				.hasNext());
		
		assertEquals("configuration node with type=configuration-type1-9972, tunnel-bandwidth does not exist", true, 
				g.V().has("global-customer-id", "customer-9972-BandwidthMigration")
				.in("org.onap.relationships.inventory.BelongsTo").has("service-type", "DHV")
				.in("org.onap.relationships.inventory.BelongsTo").has("service-instance-id", "servInstance-9972-2-BandwidthMigration")
				.out("org.onap.relationships.inventory.Uses").has("aai-node-type", "configuration")
				.has("configuration-type", "configuration-type1-9972")
				.hasNext());
		
		// check if configuration node gets created for 2, 3, 4
		assertEquals("configuration node created with type=DHV, tunnel-bandwidth=servInstance-9972-4-BandwidthMigration", true, 
				g.V().has("global-customer-id", "customer-9972-BandwidthMigration")
				.in("org.onap.relationships.inventory.BelongsTo").has("service-type", "DHV")
				.in("org.onap.relationships.inventory.BelongsTo").has("service-instance-id", "servInstance-9972-4-BandwidthMigration")
				.out("org.onap.relationships.inventory.Uses").has("aai-node-type", "configuration")
				.has("configuration-type", "DHV").has("tunnel-bandwidth", "bandwidth-total-4-BandwidthMigration")
				.hasNext());
		
		assertEquals("configuration node created with type=DHV, tunnel-bandwidth=servInstance-9972-3-BandwidthMigration", true, 
				g.V().has("global-customer-id", "customer-9972-BandwidthMigration")
				.in("org.onap.relationships.inventory.BelongsTo").has("service-type", "DHV")
				.in("org.onap.relationships.inventory.BelongsTo").has("service-instance-id", "servInstance-9972-3-BandwidthMigration")
				.out("org.onap.relationships.inventory.Uses").has("aai-node-type", "configuration")
				.has("configuration-type", "DHV").has("tunnel-bandwidth", "bandwidth-total-3-BandwidthMigration")
				.hasNext());
		
		assertEquals("configuration node created with type=DHV, tunnel-bandwidth=servInstance-9972-2-BandwidthMigration", true, 
				g.V().has("global-customer-id", "customer-9972-BandwidthMigration")
				.in("org.onap.relationships.inventory.BelongsTo").has("service-type", "DHV")
				.in("org.onap.relationships.inventory.BelongsTo").has("service-instance-id", "servInstance-9972-2-BandwidthMigration")
				.out("org.onap.relationships.inventory.Uses").has("aai-node-type", "configuration")
				.has("configuration-type", "DHV").has("tunnel-bandwidth", "bandwidth-total-2-BandwidthMigration")
				.hasNext());
		
		// configuration modified for ServInstance-9972-1-BandwidthMigration
		assertEquals("configuration node modified for ServInstance-9972-1-BandwidthMigration, tunnel-bandwidth=2380", true, 
				g.V().has("global-customer-id", "customer-9972-BandwidthMigration")
				.in("org.onap.relationships.inventory.BelongsTo").has("service-type", "DHV")
				.in("org.onap.relationships.inventory.BelongsTo").has("service-instance-id", "ServInstance-9972-1-BandwidthMigration")
				.out("org.onap.relationships.inventory.Uses").has("aai-node-type", "configuration")
				.has("configuration-type", "DHV").has("tunnel-bandwidth", "2380")
				.hasNext());
	}
	
	@Test
	public void testGetAffectedNodeTypes() {
		Optional<String[]> types = migration.getAffectedNodeTypes();
		Optional<String[]> expected = Optional.of(new String[]{"service-instance"});
		
		assertNotNull(types);
		assertArrayEquals(expected.get(), types.get());
	}

	@Test
	public void testGetMigrationName() {
		String migrationName = migration.getMigrationName();

		assertNotNull(migrationName);
		assertEquals("service-instance-to-configuration", migrationName);
	}
}
