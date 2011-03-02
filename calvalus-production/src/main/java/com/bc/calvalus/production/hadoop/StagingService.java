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

package com.bc.calvalus.production.hadoop;

import com.bc.calvalus.processing.beam.BeamL3FormattingService;
import com.bc.calvalus.processing.beam.FormatterL3Config;
import com.bc.calvalus.production.ProductionState;
import com.bc.calvalus.production.ProductionStatus;
import org.apache.hadoop.conf.Configuration;

import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.logging.Level;
import java.util.logging.Logger;

/**
 * A simple service for staging the results of a hadoop job.
 *
 * @author MarcoZ
 */
public class StagingService {

    private final ExecutorService executorService;
    private final Logger logger;

    public StagingService(Logger logger) {
        this.logger = logger;
        this.executorService = Executors.newSingleThreadExecutor();
    }

    public void stageProduction(final HadoopProduction production, final Configuration configuration) {
        executorService.submit(new Runnable() {
            @Override
            public void run() {
                BeamL3FormattingService beamL3FormattingService = new BeamL3FormattingService(logger, configuration);
                String outputDir = production.getOutputPath();
                FormatterL3Config formatConfig = new FormatterL3Config("Product",
                                                                       "outputFile.dim",
                                                                       production.getOutputFormat(),
                                                                       null,
                                                                       "2010-01-01",
                                                                       "2010-01-02");
                try {
                    beamL3FormattingService.format(formatConfig, outputDir, "wpsRequestXML");//TODO
                    production.setStagingStatus(new ProductionStatus(ProductionState.COMPLETED, "", 1));
                } catch (Exception e) {
                    logger.log(Level.WARNING, "formatting failed.", e);
                }
            }
        });
    }
}
