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

import com.bc.calvalus.commons.CalvalusLogger;
import org.apache.hadoop.fs.FileStatus;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.mapred.InvalidJobConfException;
import org.apache.hadoop.mapreduce.JobContext;
import org.apache.hadoop.mapreduce.JobStatus;
import org.apache.hadoop.mapreduce.OutputCommitter;
import org.apache.hadoop.mapreduce.RecordWriter;
import org.apache.hadoop.mapreduce.TaskAttemptContext;
import org.apache.hadoop.mapreduce.lib.output.FileOutputCommitter;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import sun.reflect.generics.reflectiveObjects.NotImplementedException;

import java.io.IOException;
import java.util.logging.Logger;

/**
 * An output format that provides the target directory as temp dir to avoid copying during commit.
 *
 * @author MB
 */
public class S3aCompatibleOutputFormat<K, V> extends FileOutputFormat<K, V> {

    private OutputCommitter committer = null;

    /**
     * Does not clear the output directory, we may add to it. Checks that the output path is set
     */
    @Override
    public void checkOutputSpecs(JobContext job) throws IOException {
        if (getOutputPath(job) == null) {
            throw new InvalidJobConfException("Output path not set.");
        }
    }

    /**
     * We do not use record writer for products in Calvalus
     */
    @Override
    public RecordWriter<K, V> getRecordWriter(TaskAttemptContext job) throws IOException, InterruptedException {
        return new RecordWriter<K, V>() {
            public void write(K key, V value) {}
            public void close(TaskAttemptContext context) {}
        };
    }

    @Override
    public synchronized OutputCommitter getOutputCommitter(TaskAttemptContext context) throws IOException {
        if (committer == null) {
            committer = new S3aCompatibleCommitter(getOutputPath(context), context);
        }
        return committer;
    }

    private static class S3aCompatibleCommitter extends FileOutputCommitter {
        private static final Logger LOG = CalvalusLogger.getLogger();

        private final Path outputPath;

        public S3aCompatibleCommitter(Path outputPath, TaskAttemptContext context) throws IOException {
            super(null, context);
            FileSystem fs = outputPath.getFileSystem(context.getConfiguration());
            this.outputPath = fs.makeQualified(outputPath);
        }

        /**
          * Get the directory that the task should write results into.
          * @return the work directory
          * @throws IOException
          */
        @Override
         public Path getWorkPath() throws IOException {
           return outputPath;
         }

        /**
         * Compute the path where the output of a task attempt is stored until
         * that task is committed.
         *
         * @param context        the context of the task attempt.
         * @param jobAttemptPath the pending job attempts path.
         * @return the path where a task attempt should be stored.
         */
        public static Path getPendingTaskAttemptPath(TaskAttemptContext context, Path jobAttemptPath) {
            return jobAttemptPath;
        }

        /**
         * Get the location of pending job attempts.
         */
        public static Path getPendingJobAttemptsPath(TaskAttemptContext context, Path out) {
            return out;
        }


        /**
         * For the framework to setup the job output during initialization.  This is
         * called from the application master process for the entire job. This will be
         * called multiple times, once per job attempt.
         */
        @Override
        public void setupJob(JobContext context) throws IOException {
            FileSystem fs = outputPath.getFileSystem(context.getConfiguration());
            if (!fs.mkdirs(outputPath)) {
                LOG.severe("Mkdirs failed to create " + outputPath);
            }
        }

        /**
         * No task setup required.
         */
        @Override
        public void setupTask(TaskAttemptContext context) throws IOException {}

        /**
         * Check whether task needs a commit.  This is called from each individual
         * task's process that will output to HDFS, and it is called just for that
         * task.
         */
        @Override
        public boolean needsTaskCommit(TaskAttemptContext taskContext) throws IOException {
            return false;
        }

        /**
         * To promote the task's temporary output to final output location.
         * If {@link #needsTaskCommit(TaskAttemptContext)} returns true and this
         * task is the task that the AM determines finished first, this method
         * is called to commit an individual task's output.  This is to mark
         * that tasks output as complete, as {@link #commitJob(JobContext)} will
         * also be called later on if the entire job finished successfully. This
         * is called from a task's process. This may be called multiple times for the
         * same task, but different task attempts.  It should be very rare for this to
         * be called multiple times and requires odd networking failures to make this
         * happen. In the future the Hadoop framework may eliminate this race.
         *
         * @param taskContext Context of the task whose output is being written.
         * @throws IOException if commit is not successful.
         */
        @Override
        public void commitTask(TaskAttemptContext taskContext) throws IOException {
            throw new IllegalStateException("commit not implemented for S3-like file systems");
        }

        /**
         * Discard the task output. This is called from a task's process to clean
         * up a single task's output that can not yet been committed. This may be
         * called multiple times for the same task, but for different task attempts.
         */
        @Override
        public void abortTask(TaskAttemptContext taskContext) throws IOException {
            throw new IllegalStateException("abort not implemented for S3-like file systems");
        }

        @Override
        public boolean isRecoverySupported() {
            return true;
        }

        @Override
        public void recoverTask(TaskAttemptContext taskContext) throws IOException {}

        /**
         * FileOutputCommitter
         * ----------------
         * $outputPath
         * $taskAttemptPath   = $outputPath/_temporary/$attemptID/_temporary/$taskAttemptID
         * $committedTaskPath = $outputPath/_temporary/$attemptID/$taskAttemptID
         * $jobAttemptPath    = $outputPath/_temporary/$attemptID/
         *
         * setupJob:   mkdirs  $jobAttemptPath
         * setupTask:  no-op
         * commitTask: mv $taskAttemptPath $committedTaskPath
         * abortTask:  rm $taskAttemptPath
         * commitJob:  foreach $committedTaskPaths: merge $committedTaskPath $outputPath
         * abortJob:   rm $outputPath/_temporary
         * recover:    mv $previousCommittedTaskPath $committedTaskPath
         *
         *
         * ConcurrentOutputCommitter
         * ----------------
         * $outputPath
         * $taskAttemptPath   = $outputPath/_temporary_$jobID/$taskAttemptID
         * $committedTaskPath = $outputPath/
         * $jobAttemptPath    = $outputPath/_temporary_$jobID/
         *
         * setupJob:   mkdirs  $jobAttemptPath
         * setupTask:  no-op
         * commitTask: merge $taskAttemptPath $$outputPath
         * abortTask:  rm $taskAttemptPath
         * commitJob:  no-op
         * abortJob:   rm $outputPath/_temporary_$jobID
         * recover :   no-op
         *
         * S3aCompatibleCommitter
         * -----------------
         * $outputPath
         * setupJob: mkdirs $outputPath
         * getOutputDir()   $outputPath
         * getTempDir()     $outputPath
         * all other        no-op
         */
    }
}
