package com.bc.calvalus.production.store;

import com.bc.calvalus.commons.ProcessState;
import com.bc.calvalus.commons.ProcessStatus;
import com.bc.calvalus.production.Production;
import com.bc.calvalus.production.ProductionException;
import com.bc.calvalus.production.ProductionRequest;
import com.bc.calvalus.production.TestProcessingService;
import org.junit.Test;

import java.util.Date;

import static org.junit.Assert.*;

/**
 * @author Norman
 */
public class SqlProductionStoreTest {


    @Test
    public void testAddAndRemove() throws Exception {
        Production[] productions;

        SqlProductionStore store = openStore(true);
        store.addProduction(createProduction1());
        store.addProduction(createProduction2());
        store.addProduction(createProduction3());
        store.persist();
        store.close();

        productions = store.getProductions();
        assertEquals(3, productions.length);

        store = openStore(false);
        store.update();
        store.removeProduction("pid2");
        store.removeProduction("pid3");
        store.persist();
        store.close();

        store = openStore(false);
        store.update();

        productions = store.getProductions();
        assertEquals(1, productions.length);
        store.removeProduction("pid1");
        store.persist();
        store.close();

        store = openStore(false);
        store.update();
        productions = store.getProductions();
        assertEquals(0, productions.length);
    }

    @Test
    public void testPersistAndUpdateWithAStatusChange() throws Exception {
        SqlProductionStore store = openStore(true);
        Production production = createProduction2();
        store.addProduction(production);

        ProcessStatus oldStatus = new ProcessStatus(ProcessState.RUNNING, 0.4F, "In progress");
        ProcessStatus newStatus = new ProcessStatus(ProcessState.COMPLETED, 1.0F, "");

        // Check current status
        assertEquals(oldStatus, production.getProcessingStatus());
        // Change current status
        production.getWorkflow().setStatus(newStatus);
        // Persist change
        store.persist();
        store.close();

        // Reopen store
        store = openStore(false);
        store.update();
        Production[] updatedProductions = store.getProductions();
        assertEquals(1, updatedProductions.length);
        // Expect change
        assertEquals(newStatus, updatedProductions[0].getProcessingStatus());
    }

    @Test
    public void testPersistUpdateOf3AddedProductions() throws Exception {
        SqlProductionStore store1 = openStore(true);
        SqlProductionStore store2 = openStore(false);

        store1.addProduction(createProduction1());
        store1.addProduction(createProduction2());
        store1.addProduction(createProduction3());

        Production[] productions1 = store1.getProductions();
        assertNotNull(productions1);
        assertEquals(3, productions1.length);

        Production[] productions2 = store2.getProductions();
        assertNotNull(productions2);
        assertEquals(0, productions2.length);

        store1.persist();
        store2.update();

        productions2 = store2.getProductions();
        assertNotNull(productions2);
        assertEquals(3, productions2.length);
    }

    @Test
    public void testThatGetProductionsReturnsSameInstances() throws Exception {

        SqlProductionStore store = openStore(true);
        store.addProduction(createProduction1());
        store.addProduction(createProduction2());

        Production[] productions1 = store.getProductions();
        Production[] productions2 = store.getProductions();

        assertNotNull(productions1);
        assertEquals(2, productions1.length);
        assertNotNull(productions2);
        assertEquals(2, productions2.length);
        assertSame(productions2[0], productions1[0]);
        assertSame(productions2[1], productions1[1]);
    }

    @Test
    public void testThatGetProductionWithIdReturnsSameInstances() throws Exception {

        SqlProductionStore store = openStore(true);
        store.addProduction(createProduction1());
        store.addProduction(createProduction2());

        Production production1 = store.getProduction("pid1");
        Production production2 = store.getProduction("pid2");

        assertNotNull(production1);
        assertNotNull(production2);
        assertSame(production1, store.getProduction("pid1"));
        assertSame(production2, store.getProduction("pid2"));
    }

    @Test
    public void testThatProductionIsDeserialisedCorrectly() throws Exception {

        SqlProductionStore store1 = openStore(true);
        SqlProductionStore store2 = openStore(false);
        store1.addProduction(createProduction1());
        store1.persist();

        store2.update();
        Production[] productions = store2.getProductions();
        assertNotNull(productions);
        assertEquals(1, productions.length);
        Production production1 = productions[0];
        assertEquals("pid1", production1.getId());
        assertEquals("pname1", production1.getName());
        assertEquals("home/ewa/tmp6455", production1.getOutputPath());
        assertArrayEquals(new String[]{"data1", "data2"}, production1.getIntermediateDataPath());
        assertEquals("/out/spath1", production1.getStagingPath());
        assertArrayEquals(new Object[]{"job1", "job2"}, production1.getJobIds());
        assertEquals(new ProcessStatus(ProcessState.SCHEDULED, 0.0F, "Under way"), production1.getProcessingStatus());
        assertEquals(new ProcessStatus(ProcessState.UNKNOWN, 0.0F, "Not yet started"), production1.getStagingStatus());
        assertNotNull(production1.getProductionRequest());
        assertEquals("X", production1.getProductionRequest().getProductionType());
        assertEquals("eva", production1.getProductionRequest().getUserName());
        assertEquals("1", production1.getProductionRequest().getParameter("a", true));
        assertNotNull(production1.getWorkflow());
        assertEquals(new Date(1315153761000L), production1.getWorkflow().getSubmitTime());
        assertEquals(new Date(1315153764000L), production1.getWorkflow().getStartTime());
        assertEquals(null, production1.getWorkflow().getStopTime());
        assertNotNull(production1.getWorkflow().getSubmitTime());
        assertNotNull(production1.getWorkflow().getStartTime());
        assertNull(production1.getWorkflow().getStopTime());
    }

    private SqlProductionStore openStore(boolean init) throws ProductionException {
        return SqlProductionStore.create(new TestProcessingService(),
                                         "org.hsqldb.jdbcDriver",
                                         "jdbc:hsqldb:mem:calvalus-test",
                                         "SA", "", init);
    }

    private static Production createProduction1() {
        Production production = new Production("pid1", "pname1", "home/ewa/tmp6455",
                                               new String[]{"data1", "data2"},
                                               "/out/spath1", true,
                                               new ProductionRequest("X", "eva", "a", "1"),
                                               new ProxyWorkflow(new TestProcessingService(),
                                                                 new Object[]{"job1", "job2"},
                                                                 new Date(1315153761000L),
                                                                 new Date(1315153764000L),
                                                                 null,
                                                                 new ProcessStatus(ProcessState.SCHEDULED, 0.0F,
                                                                                   "Under way")));
        production.setStagingStatus(new ProcessStatus(ProcessState.UNKNOWN, 0.0F, "Not yet started"));
        return production;
    }

    private static Production createProduction2() {
        Production production = new Production("pid2", "pname2", null, "/out/spath2", true,
                                               new ProductionRequest("X", "eva", "a", "6"),
                                               new ProxyWorkflow(new TestProcessingService(),
                                                                 new Object[]{"job4", "job5"},
                                                                 new Date(1315153961000L),
                                                                 new Date(1315153964000L),
                                                                 null,
                                                                 new ProcessStatus(ProcessState.RUNNING, 0.4F,
                                                                                   "In progress")));
        production.setStagingStatus(new ProcessStatus(ProcessState.UNKNOWN, 0.0F, "Not yet started"));
        return production;
    }

    private static Production createProduction3() {
        Production production = new Production("pid3", "pname3", "home/ewa/tmp6457", "/out/spath3", true,
                                               new ProductionRequest("X", "eva", "a", "3"),
                                               new ProxyWorkflow(new TestProcessingService(),
                                                                 new Object[]{"job6", "job7"},
                                                                 new Date(1315153961000L),
                                                                 new Date(1315153964000L),
                                                                 null,
                                                                 new ProcessStatus(ProcessState.COMPLETED, 1.0F,
                                                                                   "Processed")));
        production.setStagingStatus(new ProcessStatus(ProcessState.COMPLETED, 1.0F, "Staged"));
        return production;
    }
}
