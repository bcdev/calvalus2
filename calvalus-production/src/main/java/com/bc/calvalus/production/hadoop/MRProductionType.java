package com.bc.calvalus.production.hadoop;

import com.bc.calvalus.commons.Workflow;
import com.bc.calvalus.commons.WorkflowItem;
import com.bc.calvalus.inventory.FileSystemService;
import com.bc.calvalus.processing.JobConfigNames;
import com.bc.calvalus.processing.hadoop.HadoopProcessingService;
import com.bc.calvalus.processing.hadoop.PatternBasedInputFormat;
import com.bc.calvalus.processing.hadoop.TableInputFormat;
import com.bc.calvalus.processing.l3.MRWorkflowItem;
import com.bc.calvalus.production.Production;
import com.bc.calvalus.production.ProductionException;
import com.bc.calvalus.production.ProductionRequest;
import com.bc.calvalus.production.ProductionType;
import com.bc.calvalus.staging.Staging;
import com.bc.calvalus.staging.StagingService;
import org.locationtech.jts.geom.Geometry;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.mapreduce.lib.output.SequenceFileOutputFormat;

import java.io.File;
import java.io.IOException;
import java.util.Date;

/**
 * A production type configured with mapper, reducer, and all the types.
 *
 * @author Martin
 */
public class MRProductionType extends HadoopProductionType {

    public static class Spi extends HadoopProductionType.Spi {

        @Override
        public ProductionType create(FileSystemService fileSystemService, HadoopProcessingService processing, StagingService staging) {
            return new MRProductionType(fileSystemService, processing, staging);
        }
    }

    MRProductionType(FileSystemService fileSystemService, HadoopProcessingService processingService,
                     StagingService stagingService) {
        super("MR", fileSystemService, processingService, stagingService);
    }

    @Override
    public Production createProduction(ProductionRequest productionRequest) throws ProductionException {

        final String productionId = Production.createId(productionRequest.getProductionType());
        String defaultProductionName = createProductionName("MR ", productionRequest);
        final String productionName = productionRequest.getProductionName(defaultProductionName);

        final Date minDate = productionRequest.getDate("minDate");
        final Date maxDate = productionRequest.getDate("maxDate");
        final String minDateStr = ProductionRequest.getDateFormat().format(minDate);
        final String maxDateStr = ProductionRequest.getDateFormat().format(maxDate);
        final String outputDir;
        try {
            outputDir = getFileSystemService().getQualifiedPath(productionRequest.getUserName(),
                                                                productionRequest.getString("calvalus.output.dir") + File.separator + minDateStr);
        } catch (IOException e) {
            throw new ProductionException(e);
        }

        Configuration jobConfig = createJobConfig(productionRequest);

        ProcessorProductionRequest processorProductionRequest = new ProcessorProductionRequest(productionRequest);
        setDefaultProcessorParameters(processorProductionRequest, jobConfig);
        setRequestParameters(productionRequest, jobConfig);
        processorProductionRequest.configureProcessor(jobConfig);

        setInputLocationParameters(productionRequest, jobConfig);
        jobConfig.set(JobConfigNames.CALVALUS_OUTPUT_DIR, outputDir);
        jobConfig.set(JobConfigNames.CALVALUS_MIN_DATE, minDateStr);
        jobConfig.set(JobConfigNames.CALVALUS_MAX_DATE, maxDateStr);
        jobConfig.setIfUnset("calvalus.system.snap.dataio.reader.tileHeight", "64");
        jobConfig.setIfUnset("calvalus.system.snap.dataio.reader.tileWidth", "*");
        jobConfig.setIfUnset("calvalus.system.beam.reader.tileHeight", "64");
        jobConfig.setIfUnset("calvalus.system.beam.reader.tileWidth", "*");

        final Class defaultInputFormat;
        if (productionRequest.getParameters().containsKey("inputTable")) {
            defaultInputFormat = TableInputFormat.class;
        } else {
            defaultInputFormat = PatternBasedInputFormat.class;
        }

        jobConfig.set("mapreduce.inputformat.class", productionRequest.getString("calvalus.mapreduce.inputformat.class", defaultInputFormat.getName()));
        jobConfig.set("mapreduce.map.class", productionRequest.getString("calvalus.mapreduce.map.class"));
        jobConfig.set("mapred.mapoutput.key.class", productionRequest.getString("calvalus.mapred.mapoutput.key.class"));
        jobConfig.set("mapred.mapoutput.value.class", productionRequest.getString("calvalus.mapred.mapoutput.value.class"));
        jobConfig.set("mapreduce.partitioner.class", productionRequest.getString("calvalus.mapreduce.partitioner.class"));
        jobConfig.set("mapreduce.reduce.class", productionRequest.getString("calvalus.mapreduce.reduce.class"));
        jobConfig.set("mapred.output.key.class", productionRequest.getString("calvalus.mapred.output.key.class"));
        jobConfig.set("mapred.output.value.class", productionRequest.getString("calvalus.mapred.output.value.class"));
        jobConfig.set("mapreduce.outputformat.class", productionRequest.getString("calvalus.mapreduce.outputformat.class", SequenceFileOutputFormat.class.getName()));

        jobConfig.set("mapred.reduce.tasks", productionRequest.getString("calvalus.mapred.reduce.tasks", "8"));
        String l3ConfigXml = L3ProductionType.getL3ConfigXml(productionRequest);
        jobConfig.set(JobConfigNames.CALVALUS_L3_PARAMETERS, l3ConfigXml);
        Geometry regionGeometry = productionRequest.getRegionGeometry(null);
        jobConfig.set(JobConfigNames.CALVALUS_REGION_GEOMETRY, regionGeometry != null ? regionGeometry.toString() : "");
        jobConfig.set(JobConfigNames.CALVALUS_INPUT_DATE_RANGES, "[" + minDateStr + ":" + maxDateStr + "]");

        WorkflowItem item = new MRWorkflowItem(getProcessingService(), productionRequest.getUserName(), productionName + " " + minDateStr, jobConfig);
        Workflow workflow = new Workflow.Parallel();
        workflow.add(item);

        String stagingDir = productionRequest.getStagingDirectory(productionId);
        boolean autoStaging = productionRequest.isAutoStaging();

        return new Production(productionId,
                              productionName,
                              outputDir,
                              stagingDir,
                              autoStaging,
                              productionRequest,
                              workflow);
    }
}
