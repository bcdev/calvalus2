package com.bc.calvalus.production.store;

import com.bc.calvalus.commons.ProcessState;
import com.bc.calvalus.commons.ProcessStatus;
import com.bc.calvalus.production.Production;
import com.bc.calvalus.production.ProductionRequest;
import com.bc.calvalus.production.TestProcessingService;
import org.junit.Test;

import java.sql.Connection;
import java.sql.DriverManager;
import java.util.Date;

import static org.junit.Assert.*;

/**
 * @author Norman
 */
public class SqlProductionStoreTest {

    static {

    }

    @Test
    public void testIt() throws Exception {
        Class.forName("org.hsqldb.jdbcDriver");
        Connection connection = DriverManager.getConnection("jdbc:hsqldb:mem:testdb", "SA", "");
        SqlProductionStore.initDatabase(connection);
        SqlProductionStore store = new SqlProductionStore(new TestProcessingService(), connection);
        Production production = new Production("pid1", "pname1", "/out/spath1", true, new ProductionRequest("X", "eva", "a", "1"),
                                               new ProxyWorkflow(new TestProcessingService(), new Object[]{"job1", "job2"},
                                                                 new Date(1315153761000L),
                                                                 new Date(1315153764000L),
                                                                 null,
                                                                 new ProcessStatus(ProcessState.SCHEDULED, 0.0F, "Under way")));
        production.setStagingStatus(new ProcessStatus(ProcessState.UNKNOWN, 0.0F, "Not yet started"));
        store.addProduction(production);

        Production[] productions = store.getProductions();
        assertNotNull(productions);
        assertEquals(1, productions.length);
        Production production1 = productions[0];
        assertEquals("pid1", production1.getId());
        assertEquals("pname1", production1.getName());
        assertEquals("/out/spath1", production1.getStagingPath());
        assertArrayEquals(new Object[]{"job1", "job2"}, production1.getJobIds());
        assertEquals(new ProcessStatus(ProcessState.SCHEDULED, 0.0F, "Under way"), production1.getProcessingStatus());
        assertEquals(new ProcessStatus(ProcessState.UNKNOWN, 0.0F, "Not yet started"), production1.getStagingStatus());
        assertNotNull(production1.getProductionRequest());
        assertEquals("X", production1.getProductionRequest().getProductionType());
        assertEquals("eva", production1.getProductionRequest().getUserName());
        assertNotNull(production1.getWorkflow());
        /*
        // todo - solve typical DB time locale problem (how to tell HSQLDB that we always use UTC?)
        assertEquals(new Date(1315153761000L), production1.getWorkflow().getSubmitTime());
        assertEquals(new Date(1315153764000L), production1.getWorkflow().getStartTime());
        assertEquals(null, production1.getWorkflow().getStopTime());
        */
        assertNotNull(production1.getWorkflow().getSubmitTime());
        assertNotNull(production1.getWorkflow().getStartTime());
        assertNull(production1.getWorkflow().getStopTime());
    }
}
