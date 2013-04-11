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
import com.vividsolutions.jts.geom.Geometry;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.mapreduce.Mapper;
import org.esa.beam.binning.BinningContext;
import org.esa.beam.binning.TemporalBinSource;
import org.esa.beam.binning.operator.Formatter;
import org.esa.beam.framework.datamodel.MetadataAttribute;
import org.esa.beam.framework.datamodel.MetadataElement;
import org.esa.beam.framework.datamodel.ProductData;
import org.esa.beam.util.StringUtils;

/**
 * For formatting the results of a BEAM Level 3 Hadoop Job.
 */
public class L3Formatter {

    private final BinningContext binningContext;
    private final ProductData.UTC startTime;
    private final ProductData.UTC endTime;
    private final Configuration configuration;

    public L3Formatter(BinningContext binningContext, ProductData.UTC startTime,
                       ProductData.UTC endTime, Configuration configuration) {
        this.binningContext = binningContext;
        this.startTime = startTime;
        this.endTime = endTime;
        this.configuration = configuration;
    }

    public void format(TemporalBinSource temporalBinSource, L3FormatterConfig formatterConfig, String regionName, String regionWKT) throws Exception {
        Geometry regionGeometry = JobUtils.createGeometry(regionWKT);
        Formatter.format(binningContext,
                temporalBinSource, formatterConfig.getFormatterConfig(),
                regionGeometry,
                startTime,
                endTime,
                createConfigurationMetadataElement(regionName, regionWKT)
        );
    }

    private MetadataElement createConfigurationMetadataElement(String regionName, String regionWKT) {
        MetadataElement element = new MetadataElement("calvalus.configuration");
        addConfigElementToMetadataElement(element, JobConfigNames.CALVALUS_PRODUCTION_TYPE);
        addConfigElementToMetadataElement(element, JobConfigNames.CALVALUS_INPUT_DIR, ',');
        addConfigElementToMetadataElement(element, JobConfigNames.CALVALUS_INPUT_FORMAT);
        addConfigElementToMetadataElement(element, JobConfigNames.CALVALUS_MIN_DATE);
        addConfigElementToMetadataElement(element, JobConfigNames.CALVALUS_MAX_DATE);
        addElementToMetadataElement(element, JobConfigNames.CALVALUS_INPUT_REGION_NAME, regionName);
        addElementToMetadataElement(element, JobConfigNames.CALVALUS_REGION_GEOMETRY, regionWKT);
        addConfigElementToMetadataElement(element, JobConfigNames.CALVALUS_CALVALUS_BUNDLE);
        addConfigElementToMetadataElement(element, JobConfigNames.CALVALUS_BEAM_BUNDLE);
        addConfigElementToMetadataElement(element, JobConfigNames.CALVALUS_L2_BUNDLE);
        addConfigElementToMetadataElement(element, JobConfigNames.CALVALUS_L2_OPERATOR);
        addConfigElementToMetadataElement(element, JobConfigNames.CALVALUS_L2_PARAMETERS);
        addConfigElementToMetadataElement(element, JobConfigNames.CALVALUS_L3_PARAMETERS);
        addConfigElementToMetadataElement(element, JobConfigNames.CALVALUS_MA_PARAMETERS);
        addConfigElementToMetadataElement(element, JobConfigNames.CALVALUS_TA_PARAMETERS);
        addConfigElementToMetadataElement(element, JobConfigNames.CALVALUS_OUTPUT_DIR);
        addConfigElementToMetadataElement(element, JobConfigNames.CALVALUS_USER);
        addConfigElementToMetadataElement(element, JobConfigNames.CALVALUS_REQUEST);
        addConfigElementToMetadataElement(element, "mapred.job.classpath.files", ',');
        return element;
    }

    private void addConfigElementToMetadataElement(MetadataElement parent, String name, char sep) {
        String value = configuration.get(name);
        if (value != null) {
            String[] valueSplits = StringUtils.split(value, new char[]{sep}, true);
            MetadataElement inputPathsElement = new MetadataElement(name);
            for (int i = 0; i < valueSplits.length; i++) {
                inputPathsElement.addAttribute(
                        new MetadataAttribute(name + "." + i, ProductData.createInstance(valueSplits[i]), true));
            }
            parent.addElement(inputPathsElement);
        }
    }

    private void addConfigElementToMetadataElement(MetadataElement parent, String name) {
        addElementToMetadataElement(parent, name, configuration.get(name));
    }

    private void addElementToMetadataElement(MetadataElement parent, String name, String value) {
        if (value != null) {
            parent.addAttribute(new MetadataAttribute(name, ProductData.createInstance(value), true));
        }
    }
}
