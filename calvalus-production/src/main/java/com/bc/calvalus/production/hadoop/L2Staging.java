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

import com.bc.calvalus.commons.ProcessState;
import com.bc.calvalus.commons.ProcessStatus;
import com.bc.calvalus.processing.beam.StreamingProductReader;
import com.bc.calvalus.processing.beam.StreamingProductReaderPlugin;
import com.bc.calvalus.processing.l2.L2WorkflowItem;
import com.bc.calvalus.production.Production;
import com.bc.calvalus.production.ProductionStaging;
import com.bc.calvalus.production.ProductionWriter;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileStatus;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.fs.PathFilter;
import org.esa.beam.framework.dataio.ProductIO;
import org.esa.beam.framework.datamodel.Product;
import org.esa.beam.util.io.FileUtils;

import java.io.File;

/**
 * The L2 staging job.
 *
 * @author MarcoZ
 */
class L2Staging extends ProductionStaging {

    private final Configuration hadoopConfiguration;
    private final File stagingDir;

    public L2Staging(Production production,
                     Configuration hadoopConfiguration,
                     File stagingAreaPath) {
        super(production);
        this.hadoopConfiguration = hadoopConfiguration;
        this.stagingDir = new File(stagingAreaPath, production.getStagingPath());
    }

    @Override
    public void performStaging() throws Throwable {
        Production production = getProduction();

        L2WorkflowItem workflow = (L2WorkflowItem) production.getWorkflow();
        String outputDir = workflow.getOutputDir();

        float progress = 0f;
        production.setStagingStatus(new ProcessStatus(ProcessState.RUNNING, progress, ""));
        Path outputPath = new Path(outputDir);
        FileSystem fileSystem = outputPath.getFileSystem(hadoopConfiguration);
        FileStatus[] seqFiles = fileSystem.listStatus(outputPath, new PathFilter() {
            @Override
            public boolean accept(Path path) {
                return path.getName().endsWith(".seq");
            }
        });
        if (!stagingDir.exists()) {
            stagingDir.mkdirs();
        }

        String outputFormat = production.getProductionRequest().getString("outputFormat", "BEAM-DIMAP");
        String extension;
        if (outputFormat.equals("BEAM-DIMAP")) {
            extension = ".dim";
        } else if (outputFormat.equals("NetCDF")) {
            extension = ".nc";
            outputFormat = "NetCDF-BEAM"; // use NetCDF with beam extensions
        } else if (outputFormat.equals("GeoTIFF")) {
            extension = ".tif";
        } else {
            extension = ".xxx"; // todo  what else to handle ?
        }

        int index = 0;
        for (FileStatus seqFile : seqFiles) {
            Path seqProductPath = seqFile.getPath();

            StreamingProductReader reader = new StreamingProductReader(new StreamingProductReaderPlugin());
            StreamingProductReaderPlugin.PathConfiguration pathConfiguration = new StreamingProductReaderPlugin.PathConfiguration(seqProductPath, hadoopConfiguration);
            Product product = reader.readProductNodes(pathConfiguration, null);

            File tmpDir = new File(stagingDir, "tmp");
            try {
                tmpDir.mkdir();
                String name = seqProductPath.getName();
                String productFileName = FileUtils.exchangeExtension(name, extension);
                File productFile = new File(tmpDir, productFileName);
                ProductIO.writeProduct(product, productFile, outputFormat, false);
                product.dispose();

                String zipFileName = FileUtils.exchangeExtension(name, ".zip");
                zip(tmpDir, new File(stagingDir, zipFileName));
            } finally {
                FileUtils.deleteTree(tmpDir);
            }
            index++;
            progress = (index + 1) / seqFiles.length;
            production.setStagingStatus(new ProcessStatus(ProcessState.RUNNING, progress, ""));
        }
        production.setStagingStatus(new ProcessStatus(ProcessState.COMPLETED, 1.0f, ""));
        // todo - zip or tar.gz all output DIMAPs to outputPath.getName() + ".zip" and remove outputPath.getName()
        new ProductionWriter(production).write(stagingDir);
    }
}
