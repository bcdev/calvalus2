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

package com.bc.calvalus.processing;


import com.bc.calvalus.processing.hadoop.HadoopProcessingService;
import com.bc.calvalus.processing.hadoop.HadoopWorkflowItem;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;

import java.io.IOException;

/**
 * Diverse utilities.
 *
 * @author MarcoZ
 */
public class JobUtils {

    public static void clearAndSetOutputDir(String outputDir, Job job, HadoopWorkflowItem hadoopWorkflowItem) throws IOException {
        HadoopProcessingService processingService = hadoopWorkflowItem.getProcessingService();
        String userName = hadoopWorkflowItem.getUserName();
        FileSystem fileSystem = processingService.getFileSystem(userName, outputDir);
        final Path outputPath = clearDir(outputDir, fileSystem);
        FileOutputFormat.setOutputPath(job, outputPath);
    }

    public static Path clearDir(String dir, FileSystem fileSystem) throws IOException {
        final Path dirPath = new Path(dir);
        if (fileSystem.exists(dirPath)) {
            fileSystem.delete(dirPath, true);
        }
        return dirPath;
    }
}
