/*
 * Copyright (C) 2016 Brockmann Consult GmbH (info@brockmann-consult.de)
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
import com.bc.calvalus.commons.WorkflowItem;
import com.bc.calvalus.inventory.FileSystemService;
import com.bc.calvalus.processing.JobConfigNames;
import com.bc.calvalus.processing.hadoop.HadoopProcessingService;
import com.bc.calvalus.processing.ra.RAConfig;
import com.bc.calvalus.processing.ra.RAWorkflowItem;
import com.bc.calvalus.production.Production;
import com.bc.calvalus.production.ProductionException;
import com.bc.calvalus.production.ProductionRequest;
import com.bc.calvalus.production.ProductionType;
import com.bc.calvalus.staging.Staging;
import com.bc.calvalus.staging.StagingService;
import com.bc.ceres.binding.BindingException;
import com.vividsolutions.jts.geom.Geometry;
import org.apache.hadoop.conf.Configuration;
import org.esa.snap.core.util.StringUtils;
import org.geotools.index.CloseableIterator;

import java.io.IOException;
import java.util.ArrayList;
import java.util.List;

/**
 * Region analysis
 *
 * @author MarcoZ
 */
public class RAProductionType extends HadoopProductionType {

    public static class Spi extends HadoopProductionType.Spi {

        @Override
        public ProductionType create(FileSystemService fileSystemService, HadoopProcessingService processing, StagingService staging) {
            return new RAProductionType(fileSystemService, processing, staging);
        }
    }

    RAProductionType(FileSystemService fileSystemService, HadoopProcessingService processingService,
                     StagingService stagingService) {
        super("RA", fileSystemService, processingService, stagingService);
    }

    @Override
    public Production createProduction(ProductionRequest productionRequest) throws ProductionException {

        final String productionId = Production.createId(productionRequest.getProductionType());
        String defaultProductionName = createProductionName("Region analysis ", productionRequest);
        final String productionName = productionRequest.getProductionName(defaultProductionName);

        Configuration raJobConfig = createJobConfig(productionRequest);
        ProcessorProductionRequest processorProductionRequest = new ProcessorProductionRequest(productionRequest);
        setDefaultProcessorParameters(processorProductionRequest, raJobConfig);
        setRequestParameters(productionRequest, raJobConfig);
        processorProductionRequest.configureProcessor(raJobConfig);

        String outputDir = getOutputPath(productionRequest, productionId, "");
        String[] raConfigXmlAndRegion = getRAConfigXmlAndRegion(productionRequest);

        productionRequest.getInteger("periodLength"); // test, if set
        List<DateRange> dateRanges = L3ProductionType.getDateRanges(productionRequest, 10);
        if (dateRanges.size() == 0) {
            throw new ProductionException("No time ranges specified.");
        }

        setInputLocationParameters(productionRequest, raJobConfig);
        raJobConfig.set(JobConfigNames.CALVALUS_INPUT_REGION_NAME, productionRequest.getRegionName());
        raJobConfig.set(JobConfigNames.CALVALUS_INPUT_DATE_RANGES, StringUtils.join(dateRanges, ","));

        raJobConfig.set(JobConfigNames.CALVALUS_OUTPUT_DIR, outputDir);
        raJobConfig.set(JobConfigNames.CALVALUS_RA_PARAMETERS, raConfigXmlAndRegion[0]);

        raJobConfig.set(JobConfigNames.CALVALUS_REGION_GEOMETRY, raConfigXmlAndRegion[1]);
        WorkflowItem workflowItem = new RAWorkflowItem(getProcessingService(), productionRequest.getUserName(),
                                                       productionName, raJobConfig);

        String stagingDir = productionRequest.getStagingDirectory(productionId);
        boolean autoStaging = productionRequest.isAutoStaging();
        return new Production(productionId,
                              productionName,
                              outputDir,
                              stagingDir,
                              autoStaging,
                              productionRequest,
                              workflowItem);
    }

    @Override
    protected Staging createUnsubmittedStaging(Production production) throws IOException {
        return new CopyStaging(production,
                               getProcessingService().getJobClient(production.getProductionRequest().getUserName()).getConf(),
                               getStagingService().getStagingDir());
    }

    private String[] getRAConfigXmlAndRegion(ProductionRequest productionRequest) throws ProductionException {
        String raParametersXml = productionRequest.getString("calvalus.ra.parameters", null);
        if (raParametersXml == null) {
            throw new ProductionException("missing parameter 'calvalus.ra.parameters'");
//            RAConfig raConfig = getRAConfig(productionRequest);
//            raParametersXml = raConfig.toXml();
        } else {
            // Check MA XML before sending it to Hadoop
            CloseableIterator<RAConfig.NamedGeometry> regionsIterator = null;
            try {
                RAConfig raConfig = RAConfig.fromXml(raParametersXml);
                Configuration conf = getProcessingService().createJobConfig(productionRequest.getUserName());
                regionsIterator = raConfig.createNamedRegionIterator(conf);
                List<String> regionNames = new ArrayList<>();
                Geometry union = null;
                while (regionsIterator.hasNext()) {
                    RAConfig.NamedGeometry namedGeometry = regionsIterator.next();
                    if (union == null) {
                        union = namedGeometry.geometry;
                    } else {
                        union = union.union(namedGeometry.geometry);
                    }
                    regionNames.add(namedGeometry.name);
                }
                if (regionNames.isEmpty()) {
                    throw new ProductionException("No region defined");
                }
                if (union == null) {
                    throw new ProductionException("Can not build union from given regions");
                }
                raConfig.setInternalRegionNames(regionNames.toArray(new String[0]));
                return new String[]{raConfig.toXml(), union.toString()};
            } catch (BindingException | IOException e) {
                throw new ProductionException("Illegal Region-analysis configuration: " + e.getMessage(), e);
            } finally {
                if (regionsIterator != null) {
                    try {
                        regionsIterator.close();
                    } catch (IOException e) {
                        throw new ProductionException("Failed to remove temp file: " + e.getMessage(), e);
                    }
                }
            }
        }
    }

    static RAConfig getRAConfig(ProductionRequest productionRequest) throws ProductionException {
        RAConfig raConfig = new RAConfig();

//        String wkt = productionRequest.getString("regionWKT");
//        String name = productionRequest.getString("regionName", "region");
//        raConfig.setRegions(new RAConfig.Region(name, wkt));
//
//        String bandList = productionRequest.getParameter("bandList", true);
//        raConfig.setBandNames(bandList.split(","));
//        raConfig.setValidExpressions(productionRequest.getString("maskExpr", "true"));
        return raConfig;
    }

}
