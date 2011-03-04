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

import org.apache.hadoop.conf.Configuration;

import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.Future;
import java.util.logging.Logger;

/**
 * A simple service for staging the results of a hadoop job.
 *
 * @author MarcoZ
 */
public class StagingService {

    private final ExecutorService executorService;

    public StagingService() {
        this.executorService = Executors.newSingleThreadExecutor();
    }

    public L3StagingJob stageProduction(L3ProcessingRequest processingRequest,
                                      HadoopProduction production,
                                      Configuration configuration,
                                       String wpsXml,
                                      Logger logger) {
        L3StagingJob task = new L3StagingJob(processingRequest, production, configuration, wpsXml, logger);
        submitJob(task);
        return task;
    }

    public Future<L3StagingJob> submitJob(L3StagingJob task) {
        return executorService.submit(task);
    }

}
