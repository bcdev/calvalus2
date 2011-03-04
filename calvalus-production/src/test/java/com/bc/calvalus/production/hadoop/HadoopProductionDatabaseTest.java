package com.bc.calvalus.production.hadoop;



import com.bc.calvalus.production.ProductionException;
import com.bc.calvalus.production.ProductionRequest;

import java.io.BufferedReader;
import java.io.IOException;
import java.io.PrintWriter;
import java.io.StringReader;
import java.io.StringWriter;

import com.bc.calvalus.production.ProductionState;
import com.bc.calvalus.production.ProductionStatus;
import org.apache.hadoop.mapreduce.JobID;
import org.junit.Test;

import static org.junit.Assert.*;

public class HadoopProductionDatabaseTest {

    @Test
    public void testIO() throws IOException {
        ProcessingRequestFactory processingRequestFactory = new L3ProcessingRequestFactory() {
            @Override
            public String[] getInputFiles(ProductionRequest request) throws ProductionException {
                return new String[]{"F1.N1", "F2.N1", "F3.N1", "F4.N1"};
            }
        };
        HadoopProductionDatabase db = new HadoopProductionDatabase(processingRequestFactory);
        HadoopProduction prod1 = new HadoopProduction("id1", "name1",
                                                      new JobID("34627598547", 11),
                                                      false,
                                                      new ProductionRequest("testprod", "a", "5", "b", "9"));
        prod1.setProcessingStatus(new ProductionStatus(ProductionState.IN_PROGRESS, 0.6f));

        HadoopProduction prod2 = new HadoopProduction("id2", "name2",
                                                      new JobID("34627598547", 426),
                                                      false,
                                                      new ProductionRequest("testprod", "a", "9", "b", "2"));
        prod2.setProcessingStatus(new ProductionStatus(ProductionState.COMPLETED));

        HadoopProduction prod3 = new HadoopProduction("id3", "name3",
                                                      new JobID("34627598547", 87),
                                                      true,
                                                      new ProductionRequest("testprod", "a", "1", "b", "0"));
        prod3.setProcessingStatus(new ProductionStatus(ProductionState.COMPLETED));
        prod3.setStagingStatus(new ProductionStatus(ProductionState.COMPLETED));

        db.addProduction(prod1);
        db.addProduction(prod2);
        db.addProduction(prod3);

        StringWriter out = new StringWriter();
        db.store(new PrintWriter(out));

        HadoopProductionDatabase db2 = new HadoopProductionDatabase(processingRequestFactory);
        db2.load(new BufferedReader(new StringReader(out.toString())));

        HadoopProduction[] productions = db2.getProductions();
        assertNotNull(productions);
        assertEquals(3, productions.length);

        HadoopProduction restoredProd1 = productions[0];
        assertEquals("id1", restoredProd1.getId());
        assertEquals("name1", restoredProd1.getName());
        assertEquals(new JobID("34627598547", 11).toString(), restoredProd1.getJobId().toString());
        assertEquals(false, restoredProd1.isOutputStaging());
        assertEquals(new ProductionStatus(ProductionState.IN_PROGRESS, 0.6f), restoredProd1.getProcessingStatus());
        assertEquals(new ProductionStatus(), restoredProd1.getStagingStatus());
        assertNotNull(restoredProd1.getProductionRequest());
        assertEquals("testprod", restoredProd1.getProductionRequest().getProductionType());
        assertEquals("5", restoredProd1.getProductionRequest().getProductionParameter("a"));
        assertEquals("9", restoredProd1.getProductionRequest().getProductionParameter("b"));

        HadoopProduction restoredProd2 = productions[1];
        assertEquals("id2", restoredProd2.getId());
        assertEquals("name2", restoredProd2.getName());
        assertEquals(new JobID("34627598547", 426).toString(), restoredProd2.getJobId().toString());
        assertEquals(false, restoredProd2.isOutputStaging());
        assertEquals(new ProductionStatus(ProductionState.COMPLETED), restoredProd2.getProcessingStatus());
        assertEquals(new ProductionStatus(), restoredProd2.getStagingStatus());
        assertNotNull(restoredProd2.getProductionRequest());
        assertEquals("testprod", restoredProd2.getProductionRequest().getProductionType());
        assertEquals("9", restoredProd2.getProductionRequest().getProductionParameter("a"));
        assertEquals("2", restoredProd2.getProductionRequest().getProductionParameter("b"));

        HadoopProduction restoredProd3 = productions[2];
        assertEquals("id3", restoredProd3.getId());
        assertEquals("name3", restoredProd3.getName());
        assertEquals(new JobID("34627598547", 87).toString(), restoredProd3.getJobId().toString());
        assertEquals(true, restoredProd3.isOutputStaging());
        assertEquals(new ProductionStatus(ProductionState.COMPLETED), restoredProd3.getProcessingStatus());
        assertEquals(new ProductionStatus(ProductionState.COMPLETED), restoredProd3.getStagingStatus());
        assertNotNull(restoredProd3.getProductionRequest());
        assertEquals("testprod", restoredProd3.getProductionRequest().getProductionType());
        assertEquals("1", restoredProd3.getProductionRequest().getProductionParameter("a"));
        assertEquals("0", restoredProd3.getProductionRequest().getProductionParameter("b"));
    }
}
