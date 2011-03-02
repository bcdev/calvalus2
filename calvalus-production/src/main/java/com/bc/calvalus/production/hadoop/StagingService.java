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
import org.esa.beam.framework.dataio.ProductIO;
import org.esa.beam.framework.dataio.ProductIOPlugInManager;

import java.util.concurrent.Callable;
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

    public StagingJob stageProduction(final HadoopProduction production, final Configuration configuration) {
        StagingJob task = new StagingJob(production, configuration);
        executorService.submit(task);
        return task;
    }

    class StagingJob implements Callable<StagingJob> {
        private final HadoopProduction production;
        private final Configuration hadoopConfiguration;
        private float progress;

        public StagingJob(HadoopProduction production, Configuration hadoopConfiguration) {
            this.production = production;
            this.hadoopConfiguration = hadoopConfiguration;
        }

        @Override
        public StagingJob call() throws Exception {
            progress = 0f;
            BeamL3FormattingService beamL3FormattingService = new BeamL3FormattingService(logger, hadoopConfiguration);
            String outputDir = "hdfs://cvmaster00:9000/calvalus/outputs/" + production.getOutputPath();
            FormatterL3Config formatConfig = new FormatterL3Config("Product",
                                                                   "calvalus-level3-" + production.getId() + ".dim",
                                                                   production.getOutputFormat(),
                                                                   null,
                                                                   "2010-01-01",
                                                                   "2010-01-02");
            try {

                beamL3FormattingService.format(formatConfig, outputDir, production.getWpsXml());
                progress = 1f;
                production.setStagingStatus(new ProductionStatus(ProductionState.COMPLETED, "", progress));
            } catch (Exception e) {
                production.setStagingStatus(new ProductionStatus(ProductionState.ERROR, e.getMessage(), progress));
                logger.log(Level.WARNING, "formatting failed.", e);
            }
            return this;
        }


    }
}
