/*
 * Copyright (C) 2013 Brockmann Consult GmbH (info@brockmann-consult.de)
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

import com.bc.calvalus.commons.CalvalusLogger;
import com.bc.calvalus.commons.ProcessState;
import com.bc.calvalus.commons.ProcessStatus;
import com.bc.calvalus.production.Production;
import com.bc.calvalus.production.ProductionStaging;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileStatus;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.FileUtil;
import org.apache.hadoop.fs.Path;
import org.esa.beam.util.io.FileUtils;

import java.io.File;
import java.io.IOException;
import java.util.logging.Level;
import java.util.logging.LogRecord;
import java.util.logging.Logger;

/**
 * The staging job for match-up analysis (MA) results.
 *
 * @author Norman
 */
class CopyStaging extends ProductionStaging {

    private static final Logger LOG = CalvalusLogger.getLogger();
    private static final long GIGABYTE = 1024L * 1024L * 1024L;

    private final Configuration hadoopConfiguration;
    private final File stagingDir;

    public CopyStaging(Production production,
                       Configuration hadoopConfiguration,
                       File stagingAreaPath) {
        super(production);
        this.hadoopConfiguration = hadoopConfiguration;
        this.stagingDir = new File(stagingAreaPath, production.getStagingPath());
    }

    @Override
    public void performStaging() throws Throwable {
        Production production = getProduction();
        production.setStagingStatus(new ProcessStatus(ProcessState.RUNNING, 0.0F, ""));
        if (!stagingDir.exists()) {
            stagingDir.mkdirs();
        }
        LOG.info("staging dir is: " + stagingDir);

        Path remoteOutputDir = new Path(production.getOutputPath());
        FileSystem fileSystem = remoteOutputDir.getFileSystem(hadoopConfiguration);

        // Simply copy entire content of remoteOutputDir
        FileStatus[] fileStatuses = fileSystem.globStatus(new Path(remoteOutputDir, "*.*"));
        long totalFilesSize = 0L;
        if (fileStatuses != null) {
            for (int i = 0; i < fileStatuses.length; i++) {
                FileStatus fileStatus = fileStatuses[i];
                Path path = fileStatus.getPath();
                LOG.info("copying: " + path);
                int attemptNo = 0;
                boolean copySuccess = false;
                while (!copySuccess && attemptNo < 3) {
                    File dst = new File(stagingDir, path.getName());
                    try {
                        FileUtil.copy(fileSystem, path, dst, false, hadoopConfiguration);
                        copySuccess = true;
                    } catch (IOException ioe) {
                        String msg = String.format("Attempt(%d) Problem while staging: %s: %s", attemptNo, path, ioe.getMessage());
                        LogRecord logRecord = new LogRecord(Level.FINE, msg);
                        logRecord.setThrown(ioe);
                        LOG.log(logRecord);
                        if (dst.exists()) {
                            dst.delete();
                        }
                    }
                }
                totalFilesSize += fileStatus.getLen();
                production.setStagingStatus(new ProcessStatus(ProcessState.RUNNING, (i + 1.0F) / fileStatuses.length, path.getName()));
            }
        }
        if (totalFilesSize < 2L * GIGABYTE) {
            String zipFilename = getSafeFilename(production.getName() + ".zip");
            zip(stagingDir, new File(stagingDir, zipFilename));
        }

        production.setStagingStatus(new ProcessStatus(ProcessState.COMPLETED, 1.0F, ""));
    }

    @Override
    public void cancel() {
        super.cancel();
        FileUtils.deleteTree(stagingDir);
        getProduction().setStagingStatus(new ProcessStatus(ProcessState.CANCELLED));
    }
}
