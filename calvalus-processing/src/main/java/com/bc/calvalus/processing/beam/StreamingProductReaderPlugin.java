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

package com.bc.calvalus.processing.beam;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.esa.beam.framework.dataio.DecodeQualification;
import org.esa.beam.framework.dataio.ProductReader;
import org.esa.beam.framework.dataio.ProductReaderPlugIn;
import org.esa.beam.util.io.BeamFileFilter;

import java.util.Locale;

/**
 * A plugin for the StreamingProductReader
 */
public class StreamingProductReaderPlugin implements ProductReaderPlugIn {

    @Override
    public DecodeQualification getDecodeQualification(Object input) {
        if (input instanceof PathConfiguration) {
            PathConfiguration pathConfiguration = (PathConfiguration) input;
            if (pathConfiguration.getPath().getName().toLowerCase().endsWith(".seq")) {
                return DecodeQualification.INTENDED;
            }
        }
        return DecodeQualification.UNABLE;
    }

    @Override
    public Class[] getInputTypes() {
        return new Class[]{PathConfiguration.class};
    }

    @Override
    public ProductReader createReaderInstance() {
        return new StreamingProductReader(this);
    }

    @Override
    public String[] getFormatNames() {
        return new String[]{"SEQ"};
    }

    @Override
    public String[] getDefaultFileExtensions() {
        return new String[]{"seq"};
    }

    @Override
    public String getDescription(Locale locale) {
        return "Hadoop Sequiential";
    }

    @Override
    public BeamFileFilter getProductFileFilter() {
        return null; // only used in UI
    }

    public static class PathConfiguration {
        private final Path path;
        private final Configuration configuration;

        public PathConfiguration(Path path, Configuration configuration) {
            this.path = path;
            this.configuration = configuration;
        }

        public Path getPath() {
            return path;
        }

        public Configuration getConfiguration() {
            return configuration;
        }
    }
}
