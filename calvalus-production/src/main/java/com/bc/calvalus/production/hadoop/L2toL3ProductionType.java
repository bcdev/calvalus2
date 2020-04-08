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
import com.bc.calvalus.commons.DateUtils;
import com.bc.calvalus.commons.Workflow;
import com.bc.calvalus.commons.WorkflowItem;
import com.bc.calvalus.inventory.FileSystemService;
import com.bc.calvalus.processing.JobConfigNames;
import com.bc.calvalus.processing.hadoop.HadoopProcessingService;
import com.bc.calvalus.processing.l2.L2WorkflowItem;
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
import com.bc.ceres.binding.PropertySet;
import org.locationtech.jts.geom.Geometry;
import org.apache.hadoop.conf.Configuration;
import org.esa.snap.binning.AggregatorConfig;
import org.esa.snap.binning.operator.BinningConfig;
import org.esa.snap.binning.operator.VariableConfig;
import org.esa.snap.core.datamodel.ProductData;

import java.io.IOException;
import java.text.DateFormat;
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
        public ProductionType create(FileSystemService fileSystemService, HadoopProcessingService processing, StagingService staging) {
            return new L2toL3ProductionType(fileSystemService, processing, staging);
        }
    }

    L2toL3ProductionType(FileSystemService fileSystemService, HadoopProcessingService processingService,
                         StagingService stagingService) {
        super("L2toL3", fileSystemService, processingService, stagingService);
    }

    @Override
    public Production createProduction(ProductionRequest productionRequest) throws ProductionException {

        final String productionId = Production.createId(productionRequest.getProductionType());
        String defaultProductionName = createProductionName("L2-to-L3 ", productionRequest);
        final String productionName = productionRequest.getProductionName(defaultProductionName);

        List<DateRange> dateRanges = L3ProductionType.getDateRanges(productionRequest, null);
        if (dateRanges.size() == 0) {
            throw new ProductionException("No time ranges specified.");
        }
        DateFormat dateFormat = ProductionRequest.getDateFormat();

        ProcessorProductionRequest processorProductionRequest = new ProcessorProductionRequest(productionRequest);

        Geometry regionGeometry = productionRequest.getRegionGeometry(null);
        String effectiveRegionGeometry = regionGeometry != null ? regionGeometry.toString() : "";

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
            AggregatorConfig[] aggregatorConfigs = binningConfig.getAggregatorConfigs();
            for (AggregatorConfig aggregatorConfig : aggregatorConfigs) {
                PropertySet propertySet = aggregatorConfig.asPropertySet();
                if (propertySet.isPropertyDefined("outputCounts")) {
                    propertySet.setValue("outputCounts", true);
                }
            }
            l3ConfigXmlStep2 = binningConfig.toXml();
        } catch (BindingException e) {
            throw new ProductionException(e);
        }
        String baseOutputPath = getOutputPath(productionRequest, productionId, "");
        String[] l3MeanOutputDirs = new String[dateRanges.size()];

        String l2OutputDir = baseOutputPath + "/L2";

        Configuration l2Conf = createJobConfig(productionRequest);
        setDefaultProcessorParameters(processorProductionRequest, l2Conf);
        setRequestParameters(productionRequest, l2Conf);
        processorProductionRequest.configureProcessor(l2Conf);

        setInputLocationParameters(productionRequest, l2Conf);
        l2Conf.set(JobConfigNames.CALVALUS_INPUT_REGION_NAME, productionRequest.getRegionName());
        DateRange maxDateRange = new DateRange(dateRanges.get(0).getStartDate(), dateRanges.get(dateRanges.size() - 1).getStopDate());
        l2Conf.set(JobConfigNames.CALVALUS_INPUT_DATE_RANGES, maxDateRange.toString());
        l2Conf.set(JobConfigNames.CALVALUS_REGION_GEOMETRY, effectiveRegionGeometry);
        l2Conf.setBoolean(JobConfigNames.CALVALUS_INPUT_FULL_SWATH, true);

        l2Conf.setBoolean(JobConfigNames.CALVALUS_OUTPUT_PRESERVE_DATE_TREE, true);
        l2Conf.set(JobConfigNames.CALVALUS_OUTPUT_DIR, l2OutputDir);

        String wfL2Name = String.format("%s (L2)", productionName);
        WorkflowItem l2workflow = new L2WorkflowItem(getProcessingService(), productionRequest.getUserName(), wfL2Name, l2Conf);
        Workflow sequential = new Workflow.Sequential();
        sequential.add(l2workflow);

        String l3InputDirPattern = l2OutputDir + "/${yyyy}/${MM}/${dd}/.*\\.(N1|nc|hdf|seq)$";

        Workflow parallel = new Workflow.Parallel();
        parallel.setSustainable(false);
        for (int i = 0; i < dateRanges.size(); i++) {
            DateRange dateRange = dateRanges.get(i);

            String singleRangeOutputDirMean = baseOutputPath + "/L3-mean-" + (i + 1);
            l3MeanOutputDirs[i] = singleRangeOutputDirMean;

            Configuration l3Conf = createJobConfig(productionRequest);
            setRequestParameters(productionRequest, l3Conf);

            l3Conf.set(JobConfigNames.CALVALUS_INPUT_PATH_PATTERNS, l3InputDirPattern);
            l3Conf.set(JobConfigNames.CALVALUS_INPUT_DATE_RANGES, dateRange.toString());
            l3Conf.set(JobConfigNames.CALVALUS_REGION_GEOMETRY, effectiveRegionGeometry);
            l3Conf.setBoolean(JobConfigNames.CALVALUS_INPUT_FULL_SWATH, true);

            l3Conf.set(JobConfigNames.CALVALUS_OUTPUT_DIR, singleRangeOutputDirMean);

            l3Conf.set(JobConfigNames.CALVALUS_L3_PARAMETERS, l3ConfigXmlStep1);
            l3Conf.setInt(JobConfigNames.CALVALUS_L3_REDUCERS, 1);


            String date1Str = dateFormat.format(dateRange.getStartDate());
            String date2Str = dateFormat.format(dateRange.getStopDate());
            l3Conf.set(JobConfigNames.CALVALUS_MIN_DATE, date1Str);
            l3Conf.set(JobConfigNames.CALVALUS_MAX_DATE, date2Str);

            l3Conf.unset(JobConfigNames.CALVALUS_OUTPUT_FORMAT);

            String wfName = String.format("%s (L3mean %s:%s)", productionName, date1Str, date2Str);
            WorkflowItem l3workflow = new L3WorkflowItem(getProcessingService(), productionRequest.getUserName(), wfName, l3Conf);

            //////////////////////////////////////////////////////////////////////////
            Date centerDate = getCenterDate(dateRange);
            DateRange centerRange = new DateRange(centerDate, centerDate);
            String centerDateStr = dateFormat.format(centerDate);
            //////////////////////////////////////////////////////////////////////////

            Configuration l2tol3RealtivConf = createJobConfig(productionRequest);
            setRequestParameters(productionRequest, l2tol3RealtivConf);

            l2tol3RealtivConf.set(JobConfigNames.CALVALUS_INPUT_PATH_PATTERNS, l3InputDirPattern);
            l2tol3RealtivConf.set(JobConfigNames.CALVALUS_INPUT_DATE_RANGES, centerRange.toString());
            l2tol3RealtivConf.set(JobConfigNames.CALVALUS_REGION_GEOMETRY, effectiveRegionGeometry);
            l2tol3RealtivConf.setBoolean(JobConfigNames.CALVALUS_INPUT_FULL_SWATH, true);

            l2tol3RealtivConf.set(JobConfigNames.CALVALUS_OUTPUT_DIR, baseOutputPath);

            l2tol3RealtivConf.set("calvalus.l2tol3.l3path", singleRangeOutputDirMean + "/part-r-00000");
            l2tol3RealtivConf.set(JobConfigNames.CALVALUS_L3_PARAMETERS, l3ConfigXmlStep2);
            l2tol3RealtivConf.set(JobConfigNames.CALVALUS_OUTPUT_PREFIX, "relative-mean");

            l2tol3RealtivConf.set(JobConfigNames.CALVALUS_MIN_DATE, centerDateStr);
            l2tol3RealtivConf.set(JobConfigNames.CALVALUS_MAX_DATE, centerDateStr);

            wfName = String.format("%s (L2-to-L3-rel %s)", productionName, centerDateStr);
            WorkflowItem l2Tol3RelWorkflow = new L2toL3WorkflowItem(getProcessingService(), productionRequest.getUserName(), wfName, l2tol3RealtivConf);

            //////////////////////////////////////////////////////////////////////////

            Configuration l2tol3AbsConf = createJobConfig(productionRequest);
            setRequestParameters(productionRequest, l2tol3AbsConf);

            l2tol3AbsConf.set(JobConfigNames.CALVALUS_INPUT_PATH_PATTERNS, l3InputDirPattern);
            l2tol3AbsConf.set(JobConfigNames.CALVALUS_INPUT_DATE_RANGES, centerRange.toString());
            l2tol3AbsConf.set(JobConfigNames.CALVALUS_REGION_GEOMETRY, effectiveRegionGeometry);
            l2tol3AbsConf.setBoolean(JobConfigNames.CALVALUS_INPUT_FULL_SWATH, true);

            l2tol3AbsConf.set(JobConfigNames.CALVALUS_OUTPUT_DIR, baseOutputPath);

            l2tol3AbsConf.set(JobConfigNames.CALVALUS_L3_PARAMETERS, l3ConfigXmlStep2);
            l2tol3AbsConf.set(JobConfigNames.CALVALUS_OUTPUT_PREFIX, "mean");

            l2tol3AbsConf.set(JobConfigNames.CALVALUS_MIN_DATE, centerDateStr);
            l2tol3AbsConf.set(JobConfigNames.CALVALUS_MAX_DATE, centerDateStr);

            wfName = String.format("%s (L2mean %s)", productionName, centerDateStr);
            WorkflowItem l2Tol3AbsWorkflow = new L2toL3WorkflowItem(getProcessingService(), productionRequest.getUserName(), wfName, l2tol3AbsConf);

            parallel.add(new Workflow.Sequential(l3workflow, l2Tol3RelWorkflow, l2Tol3AbsWorkflow));
        }
        sequential.add(parallel);

        if (productionRequest.getBoolean("outputMeanL3", false)) {
            Configuration formatJobConfig = createJobConfig(productionRequest);

            formatJobConfig.setStrings(JobConfigNames.CALVALUS_INPUT_DIR, l3MeanOutputDirs);
            formatJobConfig.set(JobConfigNames.CALVALUS_OUTPUT_DIR, baseOutputPath);
            formatJobConfig.set(JobConfigNames.CALVALUS_OUTPUT_FORMAT, "NetCDF");
            formatJobConfig.set(JobConfigNames.CALVALUS_OUTPUT_COMPRESSION, "None");

            String wfName = String.format("%s (Format)", productionName);
            WorkflowItem l3Format = new L3FormatWorkflowItem(getProcessingService(),
                                                             productionRequest.getUserName(),
                                                             wfName,
                                                             formatJobConfig);
            sequential.add(l3Format);
        }

        String stagingDir = productionRequest.getStagingDirectory(productionId);
        boolean autoStaging = productionRequest.isAutoStaging();
        return new Production(productionId,
                              productionName,
                              baseOutputPath,
                              stagingDir,
                              autoStaging,
                              productionRequest,
                              sequential);
    }

    static Date getCenterDate(DateRange dateRange) {
        Calendar calendar = DateUtils.createCalendar();
        long timeDiff = dateRange.getStopDate().getTime() - dateRange.getStartDate().getTime();
        int timeDiffSecondsHalf = (int) timeDiff / 2000;
        calendar.setTime(dateRange.getStartDate());
        calendar.add(Calendar.SECOND, timeDiffSecondsHalf);
        return calendar.getTime();
    }
}
