package com.bc.calvalus.production.hadoop;

import com.bc.calvalus.processing.JobConfigNames;
import com.bc.calvalus.processing.hadoop.HadoopProcessingService;
import com.bc.calvalus.production.Production;
import com.bc.calvalus.production.ProductionException;
import com.bc.calvalus.production.ProductionRequest;
import com.bc.calvalus.production.TestInventoryService;
import com.bc.calvalus.production.TestStagingService;
import com.bc.calvalus.staging.Staging;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.mapred.JobClient;
import org.apache.hadoop.mapred.JobConf;
import org.junit.Assert;
import org.junit.Test;

/**
 * @author Norman Fomferra
 */
public class HadoopProductionTypeTest {
    @Test
    public void testThatCalavlusNamesGoIntoJobConfig() throws Exception {
        HadoopProductionType productionType = new HadoopProductionType("X",
                                                                       new TestInventoryService(),
                                                                       new HadoopProcessingService(new JobClient(new JobConf())),
                                                                       new TestStagingService()) {
            @Override
            protected Staging createUnsubmittedStaging(Production production) {
                return null;
            }

            @Override
            public Production createProduction(ProductionRequest productionRequest) throws ProductionException {
                return null;
            }
        };

        ProductionRequest productionRequest = new ProductionRequest("X", "eva",
                                                                    "calvalus.in", "*.N1",
                                                                    "calvalus.hadoop.fs.s3.maxRetries", "16",
                                                                    "beam.mem", "6TB");
        Configuration jobConfig = productionType.createJobConfig(productionRequest);
        productionType.setRequestParameters(jobConfig, productionRequest);

        Assert.assertEquals("X", jobConfig.get(JobConfigNames.CALVALUS_PRODUCTION_TYPE));
        Assert.assertEquals("eva", jobConfig.get(JobConfigNames.CALVALUS_USER));
        Assert.assertEquals("*.N1", jobConfig.get("calvalus.in"));
        Assert.assertEquals(null, jobConfig.get("calvalus.hadoop.fs.s3.maxRetries"));
        Assert.assertEquals("16", jobConfig.get("fs.s3.maxRetries"));
        Assert.assertEquals(null, jobConfig.get("beam.mem"));
    }
}
