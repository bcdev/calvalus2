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

import org.esa.beam.binning.BinningContext;
import org.esa.beam.binning.TemporalBinSource;
import com.bc.calvalus.processing.JobConfigNames;
import com.vividsolutions.jts.geom.Geometry;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.esa.beam.binning.operator.Formatter;
import org.esa.beam.framework.datamodel.MetadataAttribute;
import org.esa.beam.framework.datamodel.MetadataElement;
import org.esa.beam.framework.datamodel.ProductData;
import org.esa.beam.util.StringUtils;

/**
 * For formatting the results of a BEAM Level 3 Hadoop Job.
 */
public class L3Formatter {

    private Configuration configuration;

    public L3Formatter(Configuration configuration) {
        this.configuration = configuration;
    }

    public void format(BinningContext binningContext,
                       L3FormatterConfig formatterConfig,
                       Path partsDir,
                       Geometry roiGeometry,
                       ProductData.UTC startTime,
                       ProductData.UTC endTime) throws Exception {
        final TemporalBinSource temporalBinSource = new L3TemporalBinSource(configuration, partsDir);
        Formatter.format(binningContext,
                temporalBinSource, formatterConfig.getFormatterConfig(),
                roiGeometry,
                startTime,
                endTime,
                createConfigurationMetadataElement()
        );
    }

    private MetadataElement createConfigurationMetadataElement() {
        MetadataElement configurationElement = new MetadataElement("calvalus.configuration");
        addConfigElementToMetadataElement(configurationElement, JobConfigNames.CALVALUS_PRODUCTION_TYPE);
        addConfigElementToMetadataElement(configurationElement, JobConfigNames.CALVALUS_INPUT, ',');
        addConfigElementToMetadataElement(configurationElement, JobConfigNames.CALVALUS_INPUT_FORMAT);
        addConfigElementToMetadataElement(configurationElement, JobConfigNames.CALVALUS_MIN_DATE);
        addConfigElementToMetadataElement(configurationElement, JobConfigNames.CALVALUS_MAX_DATE);
        addConfigElementToMetadataElement(configurationElement, JobConfigNames.CALVALUS_REGION_NAME);
        addConfigElementToMetadataElement(configurationElement, JobConfigNames.CALVALUS_REGION_GEOMETRY);
        addConfigElementToMetadataElement(configurationElement, JobConfigNames.CALVALUS_CALVALUS_BUNDLE);
        addConfigElementToMetadataElement(configurationElement, JobConfigNames.CALVALUS_BEAM_BUNDLE);
        addConfigElementToMetadataElement(configurationElement, JobConfigNames.CALVALUS_L2_BUNDLE);
        addConfigElementToMetadataElement(configurationElement, JobConfigNames.CALVALUS_L2_OPERATOR);
        addConfigElementToMetadataElement(configurationElement, JobConfigNames.CALVALUS_L2_PARAMETERS);
        addConfigElementToMetadataElement(configurationElement, JobConfigNames.CALVALUS_L3_PARAMETERS);
        addConfigElementToMetadataElement(configurationElement, JobConfigNames.CALVALUS_MA_PARAMETERS);
        addConfigElementToMetadataElement(configurationElement, JobConfigNames.CALVALUS_TA_PARAMETERS);
        addConfigElementToMetadataElement(configurationElement, JobConfigNames.CALVALUS_OUTPUT_DIR);
        addConfigElementToMetadataElement(configurationElement, JobConfigNames.CALVALUS_USER);
        addConfigElementToMetadataElement(configurationElement, JobConfigNames.CALVALUS_REQUEST);
        addConfigElementToMetadataElement(configurationElement, "mapred.job.classpath.files", ',');
        return configurationElement;
    }

    private void addConfigElementToMetadataElement(MetadataElement parent, String name, char sep) {
        String value = configuration.get(name);
        if (value != null) {
            String[] valueSplits = StringUtils.split(value, new char[]{sep}, true);
            MetadataElement inputPathsElement = new MetadataElement(name);
            for (int i = 0; i < valueSplits.length; i++) {
                inputPathsElement.addAttribute(new MetadataAttribute(name + "." + i, ProductData.createInstance(valueSplits[i]), true));
            }
            parent.addElement(inputPathsElement);
        }
    }

    private void addConfigElementToMetadataElement(MetadataElement parent, String name) {
        String value = configuration.get(name);
        if (value != null) {
            parent.addAttribute(new MetadataAttribute(name, ProductData.createInstance(value), true));
        }
    }
}
