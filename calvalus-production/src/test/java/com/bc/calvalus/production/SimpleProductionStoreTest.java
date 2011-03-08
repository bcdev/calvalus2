package com.bc.calvalus.production;


import com.bc.calvalus.commons.ProcessState;
import com.bc.calvalus.commons.ProcessStatus;
import com.bc.calvalus.processing.JobIdFormat;
import org.junit.Test;

import java.io.BufferedReader;
import java.io.File;
import java.io.IOException;
import java.io.PrintWriter;
import java.io.StringReader;
import java.io.StringWriter;

import static org.junit.Assert.*;

public class SimpleProductionStoreTest {

    @Test
    public void testIO() throws IOException {
        File unusedDbFile = new File("x");
        SimpleProductionStore db = new SimpleProductionStore(JobIdFormat.STRING, unusedDbFile);

        Production prod1 = new Production("id1", "name1", "marco",
                                          false,
                                          new Object[]{"job5"},
                                          new ProductionRequest("test", "a", "5", "b", "9"));
        prod1.setProcessingStatus(new ProcessStatus(ProcessState.IN_PROGRESS, 0.6f));

        Production prod2 = new Production("id2", "name2", "martin",
                                          false,
                                          new Object[]{"job9"},
                                          new ProductionRequest("test", "a", "9", "b", "2"));
        prod2.setProcessingStatus(new ProcessStatus(ProcessState.COMPLETED));

        Production prod3 = new Production("id3", "name3", "norman",
                                          true,
                                          new Object[]{"job2"},
                                          new ProductionRequest("test", "a", "1", "b", "0"));
        prod3.setProcessingStatus(new ProcessStatus(ProcessState.COMPLETED));
        prod3.setStagingStatus(new ProcessStatus(ProcessState.COMPLETED));

        db.addProduction(prod1);
        db.addProduction(prod2);
        db.addProduction(prod3);

        StringWriter out = new StringWriter();
        db.store(new PrintWriter(out));

        SimpleProductionStore db2 = new SimpleProductionStore(JobIdFormat.STRING, unusedDbFile);
        db2.load(new BufferedReader(new StringReader(out.toString())));

        Production[] productions = db2.getProductions();
        assertNotNull(productions);
        assertEquals(3, productions.length);

        Production restoredProd1 = productions[0];
        assertEquals("id1", restoredProd1.getId());
        assertEquals("name1", restoredProd1.getName());
        assertEquals("marco", restoredProd1.getUser());
        assertEquals("job5", restoredProd1.getJobIds()[0]);
        assertEquals(false, restoredProd1.isOutputStaging());
        assertEquals(new ProcessStatus(ProcessState.IN_PROGRESS, 0.6f), restoredProd1.getProcessingStatus());
        assertEquals(ProcessStatus.UNKNOWN, restoredProd1.getStagingStatus());
        assertNotNull(restoredProd1.getProductionRequest());
        assertEquals("test", restoredProd1.getProductionRequest().getProductionType());
        assertEquals("5", restoredProd1.getProductionRequest().getProductionParameter("a"));
        assertEquals("9", restoredProd1.getProductionRequest().getProductionParameter("b"));

        Production restoredProd2 = productions[1];
        assertEquals("id2", restoredProd2.getId());
        assertEquals("name2", restoredProd2.getName());
        assertEquals("martin", restoredProd2.getUser());
        assertEquals("job9", restoredProd2.getJobIds()[0]);
        assertEquals(false, restoredProd2.isOutputStaging());
        assertEquals(new ProcessStatus(ProcessState.COMPLETED), restoredProd2.getProcessingStatus());
        assertEquals(ProcessStatus.UNKNOWN, restoredProd2.getStagingStatus());
        assertNotNull(restoredProd2.getProductionRequest());
        assertEquals("test", restoredProd2.getProductionRequest().getProductionType());
        assertEquals("9", restoredProd2.getProductionRequest().getProductionParameter("a"));
        assertEquals("2", restoredProd2.getProductionRequest().getProductionParameter("b"));

        Production restoredProd3 = productions[2];
        assertEquals("id3", restoredProd3.getId());
        assertEquals("name3", restoredProd3.getName());
        assertEquals("norman", restoredProd3.getUser());
        assertEquals("job2", restoredProd3.getJobIds()[0]);
        assertEquals(true, restoredProd3.isOutputStaging());
        assertEquals(new ProcessStatus(ProcessState.COMPLETED), restoredProd3.getProcessingStatus());
        assertEquals(new ProcessStatus(ProcessState.COMPLETED), restoredProd3.getStagingStatus());
        assertNotNull(restoredProd3.getProductionRequest());
        assertEquals("test", restoredProd3.getProductionRequest().getProductionType());
        assertEquals("1", restoredProd3.getProductionRequest().getProductionParameter("a"));
        assertEquals("0", restoredProd3.getProductionRequest().getProductionParameter("b"));
    }
}
