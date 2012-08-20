/*
 * Copyright (C) 2012 Brockmann Consult GmbH (info@brockmann-consult.de)
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

import com.bc.calvalus.processing.JobConfigNames;
import com.bc.calvalus.processing.ProcessorInstaller;
import com.bc.calvalus.processing.hadoop.HadoopProcessingService;
import org.apache.hadoop.conf.Configuration;

import java.io.IOException;

/**
 * Handles the installation of BEAM processors onto cluster nodes.
 *
 * @author MarcoZ
 */
public class BeamProcessorInstaller implements ProcessorInstaller {

    private final Configuration configuration;

    public BeamProcessorInstaller(Configuration configuration) {
        this.configuration = configuration;
    }

    @Override
    public void install() throws IOException {
        String bundle = configuration.get(JobConfigNames.CALVALUS_L2_BUNDLE);
        if (bundle != null) {
            HadoopProcessingService.addBundleToClassPath(bundle, configuration);
        }
    }
}
