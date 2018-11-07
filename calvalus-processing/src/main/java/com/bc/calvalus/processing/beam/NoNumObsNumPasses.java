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

import org.esa.snap.binning.ProductCustomizer;
import org.esa.snap.binning.ProductCustomizerConfig;
import org.esa.snap.binning.ProductCustomizerDescriptor;
import org.esa.snap.core.datamodel.Product;

/**
 * TODO
 */
public class NoNumObsNumPasses extends ProductCustomizer {
    @Override
    public void customizeProduct(Product product) {
        product.removeBand(product.getBand("num_obs"));
        product.removeBand(product.getBand("num_passes"));
    }

    public static class Descriptor implements ProductCustomizerDescriptor {

        public static final String NAME = "NoNumObsNumPasses";

        @Override
        public String getName() {
            return NAME;
        }

        @Override
        public ProductCustomizerConfig createConfig() {
            return new ProductCustomizerConfig(NAME);
        }

        @Override
        public ProductCustomizer createProductCustomizer(ProductCustomizerConfig productCustomizerConfig) {
            return new NoNumObsNumPasses();
        }
    }
}
