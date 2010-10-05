package com.bc.calvalus.experiments.add;

import com.bc.calvalus.hadoop.io.ByteArrayWritable;
import org.apache.hadoop.conf.Configured;
import org.apache.hadoop.fs.FSDataOutputStream;
import org.apache.hadoop.fs.FileStatus;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.hadoop.util.Tool;
import org.apache.hadoop.util.ToolRunner;

import java.io.IOException;
import java.text.MessageFormat;


// bin/hadoop jar hadoop-tests-1.0.jar com.bc.calvalus.hadoop.eodata.AddRastersTool input output

//
public class AddRastersTool extends Configured implements Tool {

    @Override
    public int run(String[] args) throws Exception {
        System.out.println("Submitting job...");

        Job job = new Job(getConf(), "SimpleHadoopTest");

        job.getConfiguration().setInt(RasterInputFormat.RASTER_WIDTH_PROPERTY, 10);
        job.getConfiguration().setInt(RasterInputFormat.RASTER_HEIGHT_PROPERTY, 10);

        job.setJarByClass(getClass());
        job.setInputFormatClass(RasterInputFormat.class);
        job.setOutputFormatClass(RasterOutputFormat.class);
        job.setOutputKeyClass(IntWritable.class);
        job.setOutputValueClass(ByteArrayWritable.class);
        job.setMapperClass(AddRastersMapper.class);
        job.setReducerClass(AddRastersReducer.class);

        Path input = new Path(args[0]);
        Path output = new Path(args[1]);

        FileInputFormat.addInputPath(job, input);
        FileOutputFormat.setOutputPath(job, output);

        createInputFiles(job, input);
        deleteOutputFiles(job, output);

        Path path = job.getWorkingDirectory();
        System.out.println("CWD: " + path);

        return job.waitForCompletion(true) ? 0 : 1;
    }

    private void deleteOutputFiles(Job job, Path output) throws IOException {
        FileSystem fs = output.getFileSystem(job.getConfiguration());
        fs.delete(output, true);
    }

    private void createInputFiles(Job job, Path input) throws IOException {
        FileSystem fs = input.getFileSystem(job.getConfiguration());
        FileStatus[] fileStati = fs.listStatus(input);
        for (FileStatus fileStatus : fileStati) {
            fs.delete(fileStatus.getPath(), false);
        }
        final int width = job.getConfiguration().getInt(RasterInputFormat.RASTER_WIDTH_PROPERTY, -1);
        final int height = job.getConfiguration().getInt(RasterInputFormat.RASTER_HEIGHT_PROPERTY, -1);
        final int numBands = 3;
        for (int band = 0; band < numBands; band++) {
            Path file = new Path(input, MessageFormat.format("input-raster-{0}x{1}x1-{2}", width, height, band));
            FSDataOutputStream stream = fs.create(file);
            try {
                for (int sample = 0; sample < width * height; sample++) {
                    stream.write(generateInputRasterPixel(band, sample));
                }
            } finally {
                stream.close();
            }
        }
    }

    static int generateInputRasterPixel(int band, int sample) {
        return (2 * band + sample) % 251;
    }

    public static void main(String[] args) throws Exception {
        System.exit(ToolRunner.run(new AddRastersTool(), args));
    }
}
