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

package com.bc.calvalus.processing.snap;

import com.bc.calvalus.commons.CalvalusLogger;
import org.apache.hadoop.fs.FileStatus;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.mapred.InvalidJobConfException;
import org.apache.hadoop.mapreduce.*;
import org.apache.hadoop.mapreduce.lib.output.FileOutputCommitter;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;

import java.io.IOException;
import java.util.logging.Logger;

/**
 * A output format that only provides an output directory into which the job can write its results.
 * In contrast to the {@link org.apache.hadoop.mapreduce.lib.output.NullOutputFormat},
 * it provides a {@link org.apache.hadoop.mapreduce.OutputCommitter}
 *
 * @author MarcoZ
 */
public class SimpleOutputFormat<K, V> extends FileOutputFormat<K, V> {

    private OutputCommitter committer = null;

    /**
     * The specification is the following:
     * <ol>
     * <li>The output path must to be set.</li>
     * </ol>
     */
    @Override
    public void checkOutputSpecs(JobContext job) throws IOException {
        // Ensure that the output path is set
        if (getOutputPath(job) == null) {
            throw new InvalidJobConfException("Output path not set.");
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
            committer = new ConcurrentOutputCommitter(getOutputPath(context), context);
        }
        return committer;
    }

    private static class ConcurrentOutputCommitter extends FileOutputCommitter {
        private static final Logger LOG = CalvalusLogger.getLogger();

        private final Path outputPath;
        private final Path jobAttemptPath;
        private final Path taskAttemptPath;

        public ConcurrentOutputCommitter(Path outputPath, TaskAttemptContext context) throws IOException {
            super(null, context);
            FileSystem fs = outputPath.getFileSystem(context.getConfiguration());
            this.outputPath = fs.makeQualified(outputPath);
            this.jobAttemptPath = getPendingJobAttemptsPath(context, this.outputPath);
            this.taskAttemptPath = getPendingTaskAttemptPath(context, jobAttemptPath);
            LOG.info("PendingJobAttemptsPath " + jobAttemptPath);
            LOG.info("PendingTaskAttemptPath " + taskAttemptPath);
        }

        /**
          * Get the directory that the task should write results into.
          * @return the work directory
          * @throws IOException
          */
        @Override
         public Path getWorkPath() throws IOException {
           return taskAttemptPath;
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
            return new Path(jobAttemptPath, String.valueOf(context.getTaskAttemptID()));
        }

        /**
         * Get the location of pending job attempts.
         *
         * @param context the context of the task attempt.
         * @param out     the base output directory.
         * @return the location of pending job attempts.
         */
        public static Path getPendingJobAttemptsPath(TaskAttemptContext context, Path out) {
            return new Path(out, PENDING_DIR_NAME + "_" + context.getJobID());
        }


        /**
         * For the framework to setup the job output during initialization.  This is
         * called from the application master process for the entire job. This will be
         * called multiple times, once per job attempt.
         *
         * @param context Context of the job whose output is being written.
         * @throws java.io.IOException if temporary output could not be created
         */
        @Override
        public void setupJob(JobContext context) throws IOException {
            FileSystem fs = jobAttemptPath.getFileSystem(context.getConfiguration());
            if (!fs.mkdirs(jobAttemptPath)) {
                LOG.severe("Mkdirs failed to create " + jobAttemptPath);
            }
        }

        /**
         * No task setup required.
         */
        @Override
        public void setupTask(TaskAttemptContext context) throws IOException {
            // SimpleOutputCommitter's setupTask doesn't do anything. Because the
            // temporary task directory is created on demand when the
            // task is writing.
        }

        /**
         * Check whether task needs a commit.  This is called from each individual
         * task's process that will output to HDFS, and it is called just for that
         * task.
         *
         * @param taskContext the task context
         * @return true/false
         * @throws java.io.IOException
         */
        @Override
        public boolean needsTaskCommit(TaskAttemptContext taskContext) throws IOException {
            return true;
        }

        /**
         * To promote the task's temporary output to final output location.
         * If {@link #needsTaskCommit(org.apache.hadoop.mapreduce.TaskAttemptContext)} returns true and this
         * task is the task that the AM determines finished first, this method
         * is called to commit an individual task's output.  This is to mark
         * that tasks output as complete, as {@link #commitJob(org.apache.hadoop.mapreduce.JobContext)} will
         * also be called later on if the entire job finished successfully. This
         * is called from a task's process. This may be called multiple times for the
         * same task, but different task attempts.  It should be very rare for this to
         * be called multiple times and requires odd networking failures to make this
         * happen. In the future the Hadoop framework may eliminate this race.
         *
         * @param taskContext Context of the task whose output is being written.
         * @throws java.io.IOException if commit is not successful.
         */
        @Override
        public void commitTask(TaskAttemptContext taskContext) throws IOException {
            FileSystem fs = outputPath.getFileSystem(taskContext.getConfiguration());
            taskContext.progress();
            if (fs.exists(taskAttemptPath)) {
                mergePaths(fs, fs.getFileStatus(taskAttemptPath), outputPath);
            } else {
                LOG.warning("No Output found for " + taskContext.getTaskAttemptID());
            }
        }

        /**
         * Discard the task output. This is called from a task's process to clean
         * up a single task's output that can not yet been committed. This may be
         * called multiple times for the same task, but for different task attempts.
         *
         * @param taskContext the task context
         * @throws java.io.IOException
         */
        @Override
        public void abortTask(TaskAttemptContext taskContext) throws IOException {
            taskContext.progress();
            FileSystem fs = taskAttemptPath.getFileSystem(taskContext.getConfiguration());
            if (!fs.delete(taskAttemptPath, true)) {
                LOG.warning("Could not delete " + taskAttemptPath);
            }
        }

        /**
         * For committing job's output after successful job completion. Note that this
         * is invoked for jobs with final runstate as SUCCESSFUL.  This is called
         * from the application master process for the entire job. This is guaranteed
         * to only be called once.  If it throws an exception the entire job will
         * fail.
         *
         * @param jobContext Context of the job whose output is being written.
         * @throws IOException
         */
        public void commitJob(JobContext jobContext) throws IOException {
            cleanupJob(jobContext);

            // True if the job requires output.dir marked on successful job.
            // Note that by default it is set to true.
            if (jobContext.getConfiguration().getBoolean(SUCCESSFUL_JOB_OUTPUT_DIR_MARKER, true)) {
                FileSystem fs = outputPath.getFileSystem(jobContext.getConfiguration());
                Path markerPath = new Path(outputPath, SUCCEEDED_FILE_NAME);
                fs.create(markerPath).close();
            }
        }

        /**
         * For cleaning up the job's output after job completion.  This is called
         * from the application master process for the entire job. This may be called
         * multiple times.
         *
         * @param context Context of the job whose output is being written.
         * @throws IOException
         * @deprecated Use {@link #commitJob(JobContext)} and
         * {@link #abortJob(JobContext, JobStatus.State)} instead.
         */
        @Deprecated
        public void cleanupJob(JobContext context) throws IOException {
            FileSystem fs = jobAttemptPath.getFileSystem(context.getConfiguration());
            fs.delete(jobAttemptPath, true);
        }

        @Override
        public boolean isRecoverySupported() {
            return true;
        }

        @Override
        public void recoverTask(TaskAttemptContext taskContext)
                throws IOException {
            // Nothing to do for recovering the task.
        }

        /**
         * Merge two paths together.  Anything in from will be moved into to, if there
         * are any name conflicts while merging the files or directories in from win.
         *
         * taken from (Hadoop version 2.4.0)
         * @see org.apache.hadoop.mapreduce.lib.output.FileOutputCommitter#mergePaths(org.apache.hadoop.fs.FileSystem, org.apache.hadoop.fs.FileStatus, org.apache.hadoop.fs.Path)
         *
         * @param fs the File System to use
         * @param from the path data is coming from.
         * @param to the path data is going to.
         * @throws IOException on any error
         */
        private static void mergePaths(FileSystem fs, final FileStatus from, final Path to)
          throws IOException {
            LOG.info("Merging data from "+from+" to "+to);
           if(from.isFile()) {
             if(fs.exists(to)) {
               if(!fs.delete(to, true)) {
                 throw new IOException("Failed to delete "+to);
               }
             }

             if(!fs.rename(from.getPath(), to)) {
               throw new IOException("Failed to rename "+from+" to "+to);
             }
           } else if(from.isDirectory()) {
             if(fs.exists(to)) {
               FileStatus toStat = fs.getFileStatus(to);
               if(!toStat.isDirectory()) {
                 if(!fs.delete(to, true)) {
                   throw new IOException("Failed to delete "+to);
                 }
                 if(!fs.rename(from.getPath(), to)) {
                   throw new IOException("Failed to rename "+from+" to "+to);
                 }
               } else {
                 //It is a directory so merge everything in the directories
                 for(FileStatus subFrom: fs.listStatus(from.getPath())) {
                   Path subTo = new Path(to, subFrom.getPath().getName());
                   mergePaths(fs, subFrom, subTo);
                 }
               }
             } else {
               //it does not exist just rename
               if(!fs.rename(from.getPath(), to)) {
                 throw new IOException("Failed to rename "+from+" to "+to);
               }
             }
           }
        }



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
         */
    }
}
