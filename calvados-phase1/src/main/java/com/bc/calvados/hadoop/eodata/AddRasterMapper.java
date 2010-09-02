package com.bc.calvados.hadoop.eodata;

import com.bc.calvados.hadoop.io.ByteArrayWritable;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.mapreduce.Mapper;

import java.io.IOException;

public class AddRasterMapper extends Mapper<IntWritable, ByteArrayWritable, IntWritable, ByteArrayWritable> {

    /**
     * Called once for each key/value pair in the input split. Most applications
     * should override this, but the default is the identity function.
     */
    @Override
    protected void map(IntWritable key, ByteArrayWritable value, Context context) throws IOException, InterruptedException {
        System.out.println(this + " received key " + key);
        context.write(key, value);
    }
}