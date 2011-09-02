package com.bc.calvalus.processing.ma;

import org.apache.hadoop.fs.FSDataOutputStream;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.mapreduce.TaskInputOutputContext;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;

import java.io.IOException;

/**
* Uses a task context to create new files in task's work output directory.
*
* @author Norman
*/
public class TaskOutputStreamFactory implements OutputStreamFactory {
    final TaskInputOutputContext context;

    public TaskOutputStreamFactory(TaskInputOutputContext context) {
        this.context = context;
    }

    @Override
    public FSDataOutputStream createOutputStream(String filePath) throws IOException, InterruptedException {
        return createOutputStream(context, filePath);
    }

    public static FSDataOutputStream createOutputStream(TaskInputOutputContext context, String filePath) throws IOException, InterruptedException {
        Path path = new Path(FileOutputFormat.getWorkOutputPath(context), filePath);
        return path.getFileSystem(context.getConfiguration()).create(path);
    }
}
