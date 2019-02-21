package com.bc.calvalus.processing.beam;

/*
 * Copyright (C) 2018 Brockmann Consult GmbH (info@brockmann-consult.de)
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

import com.bc.calvalus.commons.CalvalusLogger;
import com.bc.ceres.binding.BindingException;
import org.esa.snap.binning.ProductCustomizer;
import org.esa.snap.binning.ProductCustomizerConfig;
import org.esa.snap.binning.ProductCustomizerDescriptor;
import org.esa.snap.core.datamodel.Band;
import org.esa.snap.core.datamodel.Product;
import org.esa.snap.core.datamodel.ProductData;
import org.esa.snap.core.datamodel.ProductNodeGroup;
import org.esa.snap.core.gpf.GPF;
import org.esa.snap.core.gpf.annotations.Parameter;
import org.esa.snap.core.gpf.common.BandMathsOp;
import org.esa.snap.core.gpf.common.support.BandDescriptorDomConverter;

import javax.naming.ConfigurationException;
import java.util.HashMap;
import java.util.Map;

/**
 * TODO
 */
public class CustomBands extends ProductCustomizer {

    Config config;
    
    CustomBands(ProductCustomizerConfig config) {
        this.config = (Config) config;
    }

    @Override
    public void customizeProduct(Product product) {
        Band numObsBand = product.getBand("num_obs");
        if (numObsBand != null) {
            product.removeBand(numObsBand);
        }
        Band numPassesBand = product.getBand("num_passes");
        if (numPassesBand != null) {
            product.removeBand(numPassesBand);
        }
        for (BandMathsOp.BandDescriptor descriptor : config.targetBandDescriptors) {
            Band band = product.getBand(descriptor.name);
            if (band == null) {
                throw new IllegalArgumentException("Customizer config band " + descriptor.name + " not found in output");
            }
            ProductNodeGroup<Band> bandGroup = product.getBandGroup();
            int bandIndex = bandGroup.indexOf(band);
            product.removeBand(band);
            Band customisedBand = descriptor.type != null ? new Band(band.getName(), ProductData.getType(descriptor.type), band.getRasterWidth(), band.getRasterHeight()) : band;
            if (descriptor.scalingFactor != null) {
                customisedBand.setScalingFactor(descriptor.scalingFactor);
            }
            if (descriptor.scalingOffset != null) {
                customisedBand.setScalingOffset(descriptor.scalingOffset);
            }
            if (descriptor.noDataValue != null) {
                customisedBand.setNoDataValue(descriptor.noDataValue);
            }
            bandGroup.add(bandIndex, customisedBand);
            CalvalusLogger.getLogger().info("band " + band.getName() + " customised type " + ProductData.getTypeString(customisedBand.getDataType()));
        }
    }

    public static class Config extends ProductCustomizerConfig {
          @Parameter(alias = "targetBands", itemAlias = "targetBand",
                     domConverter = BandDescriptorDomConverter.class,
                     description = "List of descriptors defining the target bands.")
          private BandMathsOp.BandDescriptor[] targetBandDescriptors;
        public Config(String name) {
            super(name);
        }
    }

    public static class Descriptor implements ProductCustomizerDescriptor {

        public static final String NAME = "CustomBands";

        @Override
        public String getName() {
            return NAME;
        }

        @Override
        public ProductCustomizerConfig createConfig() {
            return new Config(NAME);
        }

        @Override
        public ProductCustomizer createProductCustomizer(ProductCustomizerConfig config) {
            return new CustomBands(config);
        }
    }
}
