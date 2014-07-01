/*
 * Copyright (C) 2014 Brockmann Consult GmbH (info@brockmann-consult.de)
 *
 * This program is free software; you can redistribute it and/or modify it
 * under the terms of the GNU General Public License as published by the Free
 * Software Foundation; either version 3 of the License, or (at your option)
 * any later version.
 * This program is distributed in the hope that it will be useful, but WITHOUT
 * ANY WARRANTY; without even the implied warranty of MERCHANTABILITY or
 * FITNESS FOR A PARTICULAR PURPOSE. See the GNU General Public License for
 * more details.
 *
 * You should have received a copy of the GNU General Public License along
 * with this program; if not, see http://www.gnu.org/licenses/
 */

package com.bc.calvalus.production.hadoop;

import com.bc.calvalus.commons.DateRange;
import com.bc.calvalus.commons.Workflow;
import com.bc.calvalus.commons.WorkflowItem;
import com.bc.calvalus.inventory.InventoryService;
import com.bc.calvalus.processing.JobConfigNames;
import com.bc.calvalus.processing.hadoop.HadoopProcessingService;
import com.bc.calvalus.processing.l2tol3.L2toL3WorkflowItem;
import com.bc.calvalus.processing.l3.L3FormatWorkflowItem;
import com.bc.calvalus.processing.l3.L3WorkflowItem;
import com.bc.calvalus.production.Production;
import com.bc.calvalus.production.ProductionException;
import com.bc.calvalus.production.ProductionRequest;
import com.bc.calvalus.production.ProductionType;
import com.bc.calvalus.staging.Staging;
import com.bc.calvalus.staging.StagingService;
import com.bc.ceres.binding.BindingException;
import com.vividsolutions.jts.geom.Geometry;
import org.apache.hadoop.conf.Configuration;
import org.esa.beam.binning.operator.BinningConfig;
import org.esa.beam.binning.operator.VariableConfig;
import org.esa.beam.framework.datamodel.ProductData;

import java.io.IOException;
import java.util.Calendar;
import java.util.Date;
import java.util.List;

/**
 * A production type used for generating comparing Level-3 products with Level-2 products.
 *
 * @author MarcoZ
 * @author Norman
 */
public class L2toL3ProductionType extends HadoopProductionType {

    public static class Spi extends HadoopProductionType.Spi {

        @Override
        public ProductionType create(InventoryService inventory, HadoopProcessingService processing, StagingService staging) {
            return new L2toL3ProductionType(inventory, processing, staging);
        }
    }

    L2toL3ProductionType(InventoryService inventoryService, HadoopProcessingService processingService,
                         StagingService stagingService) {
        super("L2toL3", inventoryService, processingService, stagingService);
    }

    @Override
    public Production createProduction(ProductionRequest productionRequest) throws ProductionException {

        final String productionId = Production.createId(productionRequest.getProductionType());
        String defaultProductionName = createProductionName("L2-to-L3 ", productionRequest);
        final String productionName = productionRequest.getProductionName(defaultProductionName);

        List<DateRange> dateRanges = L3ProductionType.getDateRanges(productionRequest, 10);
        if (dateRanges.size() == 0) {
            throw new ProductionException("No time ranges specified.");
        }

        ProcessorProductionRequest processorProductionRequest = new ProcessorProductionRequest(productionRequest);

        Geometry regionGeometry = productionRequest.getRegionGeometry(null);

        String l3ConfigXmlStep1 = L3ProductionType.getL3ConfigXml(productionRequest);
        String l3ConfigXmlStep2;

        try {
            BinningConfig binningConfig = BinningConfig.fromXml(l3ConfigXmlStep1);
            VariableConfig[] variableConfigs = binningConfig.getVariableConfigs();
            VariableConfig xaxisVariable = new VariableConfig("xaxis", "X");
            if (variableConfigs != null && variableConfigs.length > 0) {
                VariableConfig[] variableConfigsWithXaxis = new VariableConfig[variableConfigs.length + 1];
                variableConfigsWithXaxis[0] = xaxisVariable;
                System.arraycopy(variableConfigs, 0, variableConfigsWithXaxis, 1, variableConfigs.length);
                binningConfig.setVariableConfigs(variableConfigsWithXaxis);
            } else {
                binningConfig.setVariableConfigs(xaxisVariable);
            }
            l3ConfigXmlStep2 = binningConfig.toXml();
        } catch (BindingException e) {
            throw new ProductionException(e);
        }
        String outputDir = getOutputPath(productionRequest, productionId, "-L3-output");
        String[] l3MeanOutputDirs = new String[dateRanges.size()];

        Workflow workflow = new Workflow.Parallel();
        workflow.setSustainable(false);
        for (int i = 0; i < dateRanges.size(); i++) {
            DateRange dateRange = dateRanges.get(i);

            String singleRangeOutputDirMean = getOutputPath(productionRequest, productionId, "-L3-mean-" + (i + 1));
            l3MeanOutputDirs[i] = singleRangeOutputDirMean;

            Configuration l3Conf = createJobConfig(productionRequest);
            setDefaultProcessorParameters(processorProductionRequest, l3Conf);
            setRequestParameters(productionRequest, l3Conf);
            processorProductionRequest.configureProcessor(l3Conf);

            l3Conf.set(JobConfigNames.CALVALUS_INPUT_PATH_PATTERNS, productionRequest.getString("inputPath"));
            l3Conf.set(JobConfigNames.CALVALUS_INPUT_REGION_NAME, productionRequest.getRegionName());
            l3Conf.set(JobConfigNames.CALVALUS_INPUT_DATE_RANGES, dateRange.toString());

            l3Conf.set(JobConfigNames.CALVALUS_OUTPUT_DIR, singleRangeOutputDirMean);

            l3Conf.set(JobConfigNames.CALVALUS_L3_PARAMETERS, l3ConfigXmlStep1);
            l3Conf.set(JobConfigNames.CALVALUS_REGION_GEOMETRY,
                       regionGeometry != null ? regionGeometry.toString() : "");
            String date1Str = ProductionRequest.getDateFormat().format(dateRange.getStartDate());
            String date2Str = ProductionRequest.getDateFormat().format(dateRange.getStopDate());
            l3Conf.set(JobConfigNames.CALVALUS_MIN_DATE, date1Str);
            l3Conf.set(JobConfigNames.CALVALUS_MAX_DATE, date2Str);

            l3Conf.setInt(JobConfigNames.CALVALUS_L3_REDUCERS, 1);

            String wfName = productionName + " " + date1Str + " (L3)";
            WorkflowItem l3workflow = new L3WorkflowItem(getProcessingService(), productionRequest.getUserName(), wfName, l3Conf);


            Configuration l2tol3Conf = createJobConfig(productionRequest);
            setDefaultProcessorParameters(processorProductionRequest, l2tol3Conf);
            setRequestParameters(productionRequest, l2tol3Conf);
            processorProductionRequest.configureProcessor(l2tol3Conf);

            Date centerDate = getCenterDate(dateRange);
            DateRange centerRange = new DateRange(centerDate, centerDate);
            l2tol3Conf.set(JobConfigNames.CALVALUS_INPUT_PATH_PATTERNS, productionRequest.getString("inputPath"));
            l2tol3Conf.set(JobConfigNames.CALVALUS_INPUT_REGION_NAME, productionRequest.getRegionName());
            l2tol3Conf.set(JobConfigNames.CALVALUS_INPUT_DATE_RANGES, centerRange.toString());
            l2tol3Conf.set("calvalus.l2tol3.l3path", singleRangeOutputDirMean + "/part-r-00000");

            l2tol3Conf.set(JobConfigNames.CALVALUS_OUTPUT_DIR, outputDir);


            l2tol3Conf.set(JobConfigNames.CALVALUS_L3_PARAMETERS, l3ConfigXmlStep2);
            l2tol3Conf.set(JobConfigNames.CALVALUS_REGION_GEOMETRY,
                           regionGeometry != null ? regionGeometry.toString() : "");

            String centerDateStr = ProductionRequest.getDateFormat().format(centerDate);
            l2tol3Conf.set(JobConfigNames.CALVALUS_MIN_DATE, centerDateStr);
            l2tol3Conf.set(JobConfigNames.CALVALUS_MAX_DATE, centerDateStr);

            wfName = productionName + " " + date1Str + " (L2-to-L3)";
            WorkflowItem l2Tol3Workflow = new L2toL3WorkflowItem(getProcessingService(), productionRequest.getUserName(), wfName, l2tol3Conf);

            workflow.add(new Workflow.Sequential(l3workflow, l2Tol3Workflow));
        }

        if (productionRequest.getBoolean("outputMeanL3")) {
            Configuration formatJobConfig = createJobConfig(productionRequest);

            formatJobConfig.setStrings(JobConfigNames.CALVALUS_INPUT_DIR, l3MeanOutputDirs);
            formatJobConfig.set(JobConfigNames.CALVALUS_OUTPUT_DIR, outputDir);
            formatJobConfig.set(JobConfigNames.CALVALUS_OUTPUT_FORMAT, "NetCDF");
            formatJobConfig.set(JobConfigNames.CALVALUS_OUTPUT_COMPRESSION, "None");

            WorkflowItem formatItem = new L3FormatWorkflowItem(getProcessingService(),
                                                               productionRequest.getUserName(),
                                                               productionName + " Format", formatJobConfig);
            workflow = new Workflow.Sequential(workflow, formatItem);
        }

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

    static Date getCenterDate(DateRange dateRange) {
        Calendar calendar = ProductData.UTC.createCalendar();
        long timeDiff = dateRange.getStopDate().getTime() - dateRange.getStartDate().getTime();
        int timeDiffSecondsHalf = (int) timeDiff / 2000;
        calendar.setTime(dateRange.getStartDate());
        calendar.add(Calendar.SECOND, timeDiffSecondsHalf);
        return calendar.getTime();
    }

    @Override
    protected Staging createUnsubmittedStaging(Production production) throws IOException {
        return new CopyStaging(production,
                               getProcessingService().getJobClient(production.getProductionRequest().getUserName()).getConf(),
                               getStagingService().getStagingDir());
    }
}
