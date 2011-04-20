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
import java.util.Date;

import static org.junit.Assert.*;

public class SimpleProductionStoreTest {

    @Test
    public void testIO() throws IOException {
        File unusedDbFile = new File("x");
        SimpleProductionStore db = new SimpleProductionStore(JobIdFormat.STRING, unusedDbFile);

        Production prod1 = new Production("id1", "name1",
                                          "path1",
                                          new ProductionRequest("test", "marco",
                                                                "a", "5",
                                                                "b", "9"),
                                          new TestWorkflowItem<String>("job5",
                                                                       new ProcessStatus(ProcessState.RUNNING, 0.6f),
                                                                       new Date(1),
                                                                       new Date(5),
                                                                       null));

        Production prod2 = new Production("id2", "name2", null,
                                          new ProductionRequest("test", "martin",
                                                                "a", "9",
                                                                "b", "2"),
                                          new TestWorkflowItem<String>("job9",
                                                                       new ProcessStatus(ProcessState.COMPLETED),
                                                                       new Date(1),
                                                                       new Date(2),
                                                                       new Date(13)));

        Production prod3 = new Production("id3", "name3", "path3",
                                          new ProductionRequest("test", "norman",
                                                                "a", "1",
                                                                "b", "0",
                                                                "autoStaging", "true"),
                                          new TestWorkflowItem<String>("job2",
                                                                       new ProcessStatus(ProcessState.COMPLETED),
                                                                       new Date(1),
                                                                       new Date(7),
                                                                       new Date(42)));
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
        assertEquals("path1", restoredProd1.getStagingPath());
        assertEquals("job5", restoredProd1.getJobIds()[0]);
        assertEquals(false, restoredProd1.isAutoStaging());
        assertEquals(new ProcessStatus(ProcessState.RUNNING, 0.6f), restoredProd1.getProcessingStatus());
        assertEquals(ProcessStatus.UNKNOWN, restoredProd1.getStagingStatus());
        assertEquals(1, restoredProd1.getWorkflow().getSubmitTime().getTime());
        assertEquals(5, restoredProd1.getWorkflow().getStartTime().getTime());
        assertEquals(null, restoredProd1.getWorkflow().getStopTime());
        assertNotNull(restoredProd1.getProductionRequest());
        assertEquals("test", restoredProd1.getProductionRequest().getProductionType());
        assertEquals("marco", restoredProd1.getProductionRequest().getUserName());
        assertEquals("5", restoredProd1.getProductionRequest().getParameter("a"));
        assertEquals("9", restoredProd1.getProductionRequest().getParameter("b"));

        Production restoredProd2 = productions[1];
        assertEquals("id2", restoredProd2.getId());
        assertEquals("name2", restoredProd2.getName());
        assertEquals(null, restoredProd2.getStagingPath());
        assertEquals("job9", restoredProd2.getJobIds()[0]);
        assertEquals(false, restoredProd2.isAutoStaging());
        assertEquals(new ProcessStatus(ProcessState.COMPLETED), restoredProd2.getProcessingStatus());
        assertEquals(ProcessStatus.UNKNOWN, restoredProd2.getStagingStatus());
        assertEquals(1, restoredProd2.getWorkflow().getSubmitTime().getTime());
        assertEquals(2, restoredProd2.getWorkflow().getStartTime().getTime());
        assertEquals(13, restoredProd2.getWorkflow().getStopTime().getTime());
        assertNotNull(restoredProd2.getProductionRequest());
        assertEquals("test", restoredProd2.getProductionRequest().getProductionType());
        assertEquals("martin", restoredProd2.getProductionRequest().getUserName());
        assertEquals("9", restoredProd2.getProductionRequest().getParameter("a"));
        assertEquals("2", restoredProd2.getProductionRequest().getParameter("b"));

        Production restoredProd3 = productions[2];
        assertEquals("id3", restoredProd3.getId());
        assertEquals("name3", restoredProd3.getName());
        assertEquals("path3", restoredProd3.getStagingPath());
        assertEquals("job2", restoredProd3.getJobIds()[0]);
        assertEquals(true, restoredProd3.isAutoStaging());
        assertEquals(new ProcessStatus(ProcessState.COMPLETED), restoredProd3.getProcessingStatus());
        assertEquals(new ProcessStatus(ProcessState.COMPLETED), restoredProd3.getStagingStatus());
        assertEquals(1, restoredProd3.getWorkflow().getSubmitTime().getTime());
        assertEquals(7, restoredProd3.getWorkflow().getStartTime().getTime());
        assertEquals(42, restoredProd3.getWorkflow().getStopTime().getTime());
        assertNotNull(restoredProd3.getProductionRequest());
        assertEquals("test", restoredProd3.getProductionRequest().getProductionType());
        assertEquals("norman", restoredProd3.getProductionRequest().getUserName());
        assertEquals("1", restoredProd3.getProductionRequest().getParameter("a"));
        assertEquals("0", restoredProd3.getProductionRequest().getParameter("b"));
    }
}
