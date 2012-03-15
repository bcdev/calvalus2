/*
 * Copyright (C) 2011 Brockmann Consult GmbH (info@brockmann-consult.de)
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
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.mapred.FileAlreadyExistsException;
import org.apache.hadoop.mapred.InvalidJobConfException;
import org.apache.hadoop.mapreduce.JobContext;
import org.apache.hadoop.mapreduce.OutputCommitter;
import org.apache.hadoop.mapreduce.RecordWriter;
import org.apache.hadoop.mapreduce.TaskAttemptContext;
import org.apache.hadoop.mapreduce.lib.output.FileOutputCommitter;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;

import java.io.IOException;

/**
 * A output format that only provides an output directory into which the job can writes its results.
 * In contrast to the {@link org.apache.hadoop.mapreduce.lib.output.NullOutputFormat},
 * it provides a {@link org.apache.hadoop.mapreduce.OutputCommitter}
 *
 * @author MarcoZ
 */
public class SimpleOutputFormat<K, V> extends FileOutputFormat<K, V> {

    private FileOutputCommitter committer = null;

    /**
     * The specification is the following:
     * <ol>
     * <li>The output directory has to be set.</li>
     * <li>Only in resume mode this director can exits.</li>
     * </ol>
     */
    @Override
    public void checkOutputSpecs(JobContext job) throws IOException {
        // Ensure that the output directory is set
        Path outDir = getOutputPath(job);
        if (outDir == null) {
            throw new InvalidJobConfException("Output directory not set.");
        }
        // Ensure that output directory is not already there, if not in "resume mode"
        Configuration jobConfig = job.getConfiguration();
        boolean resumeProcessing = jobConfig.getBoolean(JobConfigNames.CALVALUS_RESUME_PROCESSING, false);
        boolean exists = outDir.getFileSystem(jobConfig).exists(outDir);
        if (exists && !resumeProcessing) {
            throw new FileAlreadyExistsException("Output directory " + outDir + " already exists");
        }
    }

    @Override
    public RecordWriter<K, V> getRecordWriter(TaskAttemptContext job) throws IOException, InterruptedException {
        return new RecordWriter<K, V>() {
            public void write(K key, V value) {
            }

            public void close(TaskAttemptContext context) {
            }
        };
    }

    @Override
    public synchronized OutputCommitter getOutputCommitter(TaskAttemptContext context) throws IOException {
        if (committer == null) {
            committer = new SimpleOutputCommitter(getOutputPath(context), context);
        }
        return committer;
    }

    private class SimpleOutputCommitter extends FileOutputCommitter {
        public SimpleOutputCommitter(Path outputPath, TaskAttemptContext context) throws IOException {
            super(outputPath, context);
        }

        @Override
        public void cleanupJob(JobContext context) throws IOException {
            // Do nothing intentionally !!
            // The super class removes the whole "_temporay"-directory.
            // this leads to data loss if multiple job use the sme output directory.
        }
    }
}
