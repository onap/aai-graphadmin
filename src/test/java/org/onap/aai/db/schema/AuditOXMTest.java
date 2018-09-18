package org.onap.aai.db.schema;

import org.junit.After;
import org.junit.Before;
import org.junit.Test;
import org.mockito.Mock;
import org.mockito.Mockito;
import org.mockito.MockitoAnnotations;
import org.onap.aai.AAISetup;
import org.onap.aai.introspection.Loader;
import org.onap.aai.introspection.LoaderFactory;
import org.onap.aai.introspection.ModelType;
import org.onap.aai.setup.SchemaVersion;

import static org.hamcrest.CoreMatchers.is;
import static org.junit.Assert.assertThat;
import static org.junit.Assert.assertTrue;
import static org.mockito.AdditionalMatchers.not;

public class AuditOXMTest extends AAISetup {

    private AuditOXM auditOXM;

    @Before
    public void setUp() {
    }

    @After
    public void tearDown() throws Exception {
    }

    @Test
    public void getAllIntrospectors() {
        auditOXM = new AuditOXM(loaderFactory, schemaVersions.getDefaultVersion(), edgeIngestor);
        assertTrue(auditOXM.getAllIntrospectors().size() > 0);
    }

    @Test
    public void setEdgeIngestor() {
    }
}