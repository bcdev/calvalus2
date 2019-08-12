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
import com.bc.calvalus.processing.ra.RARegions;
import com.bc.calvalus.processing.ra.RAWorkflowItem;
import com.bc.calvalus.production.Production;
import com.bc.calvalus.production.ProductionException;
import com.bc.calvalus.production.ProductionRequest;
import com.bc.calvalus.production.ProductionType;
import com.bc.calvalus.staging.StagingService;
import com.bc.ceres.binding.BindingException;
import com.vividsolutions.jts.geom.Geometry;
import com.vividsolutions.jts.operation.union.CascadedPolygonUnion;
import org.apache.hadoop.conf.Configuration;
import org.esa.snap.core.util.StringUtils;

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

        List<DateRange> dateRanges = L3ProductionType.getDateRanges(productionRequest, null);
        if (dateRanges.size() == 0) {
            throw new ProductionException("No time ranges specified.");
        }

        setInputLocationParameters(productionRequest, raJobConfig);
        raJobConfig.set(JobConfigNames.CALVALUS_INPUT_REGION_NAME, productionRequest.getRegionName());
        raJobConfig.set(JobConfigNames.CALVALUS_INPUT_DATE_RANGES, StringUtils.join(dateRanges, ","));

        raJobConfig.set(JobConfigNames.CALVALUS_OUTPUT_DIR, outputDir);
        raJobConfig.set(JobConfigNames.CALVALUS_RA_PARAMETERS, raConfigXmlAndRegion[0]);

        if (productionRequest.getParameter("regionWKT", false) != null &&
                ! "POLYGON((-180 -90, 180 -90, 180 90, -180 90, -180 -90))".
                equals(productionRequest.getString("regionWKT"))) {
            raJobConfig.set(JobConfigNames.CALVALUS_REGION_GEOMETRY, productionRequest.getString("regionWKT"));
        } else if (raJobConfig.get(JobConfigNames.CALVALUS_REGION_GEOMETRY) != null &&
                ! "POLYGON((-180 -90, 180 -90, 180 90, -180 90, -180 -90))".
                equals(raJobConfig.get(JobConfigNames.CALVALUS_REGION_GEOMETRY))) {
            // keep this polygon
        } else {
            raJobConfig.set(JobConfigNames.CALVALUS_REGION_GEOMETRY, raConfigXmlAndRegion[1]);
        }
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

    private String[] getRAConfigXmlAndRegion(ProductionRequest productionRequest) throws ProductionException {
        String raParametersXml = productionRequest.getString("calvalus.ra.parameters", null);
        boolean withEnvelope = productionRequest.getBoolean("calvalus.ra.envelope", true);
        RAConfig raConfig;
        if (raParametersXml == null) {
            raConfig = getRaConfigFromRequest(productionRequest);
        } else {
            try {
                raConfig = RAConfig.fromXml(raParametersXml);
            } catch (BindingException e) {
                throw new ProductionException("Illegal Region-analysis configuration: " + e.getMessage(), e);
            }
        }
        // Check MA XML before sending it to Hadoop
        RARegions.RegionIterator regionsIterator = null;
        try {
            Configuration conf = getProcessingService().createJobConfig(productionRequest.getUserName());
            regionsIterator = raConfig.createNamedRegionIterator(conf);

            List<String> names = new ArrayList<>();
            List<Geometry> geometries = new ArrayList<>();
            while (regionsIterator.hasNext()) {
                RAConfig.NamedRegion namedRegion = regionsIterator.next();
                Geometry geometry = namedRegion.region;
                if (withEnvelope && geometry.getNumPoints() > 20) {
                    geometry = geometry.getEnvelope();
                }
                geometries.add(geometry);
                names.add(namedRegion.name.length() > 0 ? namedRegion.name : "noname");
            }
            regionsIterator.close();
            if (names.isEmpty()) {
                throw new ProductionException("No region defined");
            }
            Geometry union = CascadedPolygonUnion.union(geometries);
            if (union == null) {
                throw new ProductionException("Can not build union from given regions");
            }
            Geometry convexHull = union.convexHull();
            raConfig.setInternalRegionNames(names.toArray(new String[0]));
            return new String[]{raConfig.toXml(), convexHull.toString()};
        } catch (IOException e) {
            throw new ProductionException("Illegal Region-analysis configuration: " + e.getMessage(), e);
        } finally {
            if (regionsIterator != null) {
                try {
                    regionsIterator.close();
                } catch (Exception e) {
                    throw new ProductionException("Failed to remove temp file: " + e.getMessage(), e);
                }
            }
        }
    }

    private RAConfig getRaConfigFromRequest(ProductionRequest productionRequest) throws ProductionException {
        RAConfig raConfig;
        raConfig = new RAConfig();

        raConfig.setRegionSource(productionRequest.getString("regionSource"));
        raConfig.setRegionSourceAttributeName(productionRequest.getString("regionSourceAttributeName", null));
        raConfig.setRegionSourceAttributeFilter(productionRequest.getString("regionSourceAttributeFilter", null));

        raConfig.setGoodPixelExpression(productionRequest.getXmlDecodedString("goodPixelExpression", null));
        raConfig.setPercentiles(productionRequest.getString("percentiles", ""));
        raConfig.setWritePerRegion(productionRequest.getBoolean("writePerRegion", Boolean.TRUE));
        raConfig.setWriteSeparateHistogram(productionRequest.getBoolean("writeSeparateHistogram", Boolean.TRUE));
        raConfig.setWritePixelValues(productionRequest.getBoolean("writePixelValues", Boolean.FALSE));

        int bandCount = productionRequest.getInteger("statband.count");
        RAConfig.BandConfig[] bandConfigs = new RAConfig.BandConfig[bandCount];
        for (int i = 0; i < bandConfigs.length; i++) {
            String bandName = productionRequest.getString("statband." + i + ".name");
            Integer numbins = productionRequest.getInteger("statband." + i + ".numBins", 0);
            Double min = productionRequest.getDouble("statband." + i + ".min", 0.0);
            Double max = productionRequest.getDouble("statband." + i + ".max", 0.0);
            bandConfigs[i] = new RAConfig.BandConfig(bandName, numbins, min, max);
        }
        raConfig.setBandConfigs(bandConfigs);
        return raConfig;
    }
}
