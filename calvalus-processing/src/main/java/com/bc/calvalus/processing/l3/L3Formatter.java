/*
 * Copyright (C) 2010 Brockmann Consult GmbH (info@brockmann-consult.de)
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

package com.bc.calvalus.processing.l3;

import com.bc.calvalus.processing.JobConfigNames;
import com.bc.calvalus.processing.JobUtils;
import com.bc.calvalus.processing.hadoop.MetadataSerializer;
import com.bc.ceres.binding.BindingException;
import com.vividsolutions.jts.geom.Geometry;
import org.apache.hadoop.conf.Configuration;
import org.esa.beam.binning.PlanetaryGrid;
import org.esa.beam.binning.TemporalBinSource;
import org.esa.beam.binning.operator.BinningConfig;
import org.esa.beam.binning.operator.Formatter;
import org.esa.beam.binning.operator.FormatterConfig;
import org.esa.beam.framework.datamodel.MetadataElement;
import org.esa.beam.framework.datamodel.ProductData;

import java.text.ParseException;

/**
 * For formatting the results of a BEAM Level 3 Hadoop Job.
 */
public class L3Formatter {

    private final ProductData.UTC startTime;
    private final ProductData.UTC endTime;
    private final Configuration configuration;
    private final PlanetaryGrid planetaryGrid;
    private final String[] featureNames;
    private final MetadataSerializer metadataSerializer;
    private final BinningConfig binningConfig;
    private FormatterConfig formatterConfig;


    public L3Formatter(String dateStart, String dateStop, String outputFile, String outputFormat, Configuration conf) throws BindingException {
        binningConfig = HadoopBinManager.getBinningConfig(conf);
        planetaryGrid = binningConfig.createPlanetaryGrid();
        this.startTime = parseTime(dateStart);
        this.endTime = parseTime(dateStop);
        this.configuration = conf;

        featureNames = conf.getStrings(JobConfigNames.CALVALUS_L3_FEATURE_NAMES);
        String formatterXML = conf.get(JobConfigNames.CALVALUS_L3_FORMAT_PARAMETERS);
        if (formatterXML != null) {
            formatterConfig = FormatterConfig.fromXml(formatterXML);
        } else {
            formatterConfig = new FormatterConfig();
        }
        formatterConfig.setOutputType("Product");
        formatterConfig.setOutputFile(outputFile);
        formatterConfig.setOutputFormat(outputFormat);

        metadataSerializer = new MetadataSerializer();
    }

    public void format(TemporalBinSource temporalBinSource, String regionName, String regionWKT) throws Exception {
        Geometry regionGeometry = JobUtils.createGeometry(regionWKT);
        final String processingHistoryXml = configuration.get(JobConfigNames.PROCESSING_HISTORY);
        final MetadataElement processingGraphMetadata = metadataSerializer.fromXml(processingHistoryXml);
        // TODO maybe replace region information in metadata if overwritten in formatting request
        Formatter.format(planetaryGrid,
                temporalBinSource,
                featureNames,
                formatterConfig,
                regionGeometry,
                startTime,
                endTime,
                processingGraphMetadata);
    }

    private static ProductData.UTC parseTime(String timeString) {
        try {
            return ProductData.UTC.parse(timeString, "yyyy-MM-dd");
        } catch (ParseException e) {
            throw new IllegalArgumentException("Illegal date format.", e);
        }
    }

}
