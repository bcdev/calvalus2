package com.bc.calvalus.production.hadoop;

import com.bc.calvalus.processing.JobConfigNames;
import com.bc.calvalus.processing.hadoop.HadoopProcessingService;
import com.bc.calvalus.JobClientsMap;
import com.bc.calvalus.production.Production;
import com.bc.calvalus.production.ProductionException;
import com.bc.calvalus.production.ProductionRequest;
import com.bc.calvalus.production.TestInventoryService;
import com.bc.calvalus.production.TestStagingService;
import com.bc.calvalus.staging.Staging;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.mapred.JobConf;
import org.junit.Before;
import org.junit.Test;

import static org.junit.Assert.*;

/**
 * @author Norman Fomferra
 */
public class HadoopProductionTypeTest {

    private HadoopProductionType productionType;

    @Before
    public void setUp() throws Exception {
        JobClientsMap jobClientsMap = new JobClientsMap(new JobConf());
        productionType = new HadoopProductionType("X",
                                                  new TestInventoryService(),
                                                  new HadoopProcessingService(jobClientsMap),
                                                  new TestStagingService()
        ) {
            @Override
            protected Staging createUnsubmittedStaging(Production production) {
                return null;
            }

            @Override
            public Production createProduction(ProductionRequest productionRequest) throws ProductionException {
                return null;
            }
        };
    }

    @Test
    public void testThatCalavlusNamesGoIntoJobConfig() throws Exception {

        ProductionRequest productionRequest = new ProductionRequest("X", "eva",
                                                                    "calvalus.in", "*.N1",
                                                                    "calvalus.hadoop.fs.s3.maxRetries", "16",
                                                                    "beam.mem", "6TB");
        Configuration jobConfig = productionType.createJobConfig(productionRequest);
        productionType.setRequestParameters(productionRequest, jobConfig);

        assertEquals("X", jobConfig.get(JobConfigNames.CALVALUS_PRODUCTION_TYPE));
        assertEquals("eva", jobConfig.get(JobConfigNames.CALVALUS_USER));
        assertEquals("*.N1", jobConfig.get("calvalus.in"));
        assertEquals(null, jobConfig.get("calvalus.hadoop.fs.s3.maxRetries"));
        assertEquals("16", jobConfig.get("fs.s3.maxRetries"));
        assertEquals(null, jobConfig.get("beam.mem"));
    }

    @Test
    public void testGetOutputPath() throws Exception {

        // default dir
        ProductionRequest productionRequest = new ProductionRequest("X", "eva");
        assertEquals("hdfs://master00:9000/calvalus/outputs/home/eva/idsuffix", productionType.getOutputPath(productionRequest, "id", "suffix"));
        assertEquals("hdfs://master00:9000/calvalus/outputs/home/eva/id", productionType.getOutputPath(productionRequest, "id", ""));

        // outputPath
        productionRequest = new ProductionRequest("X", "eva", "outputPath", "blob");
        assertEquals("hdfs://master00:9000/calvalus/outputs/blobsuffix", productionType.getOutputPath(productionRequest, "id", "suffix"));
        assertEquals("hdfs://master00:9000/calvalus/outputs/blob", productionType.getOutputPath(productionRequest, "id", ""));

        // outputPath + calvalus.output.dir
        productionRequest = new ProductionRequest("X", "eva", "outputPath", "blob", "calvalus.output.dir", "hop");
        assertEquals("hdfs://master00:9000/calvalus/outputs/hopsuffix", productionType.getOutputPath(productionRequest, "id", "suffix"));
        assertEquals("hdfs://master00:9000/calvalus/outputs/hop", productionType.getOutputPath(productionRequest, "id", ""));

        // outputPath + calvalus.output.dir + trailing SLASH
        productionRequest = new ProductionRequest("X", "eva", "outputPath", "blob", "calvalus.output.dir", "hop/");
        assertEquals("hdfs://master00:9000/calvalus/outputs/hopsuffix", productionType.getOutputPath(productionRequest, "id", "suffix"));
        assertEquals("hdfs://master00:9000/calvalus/outputs/hop", productionType.getOutputPath(productionRequest, "id", ""));
    }
}
