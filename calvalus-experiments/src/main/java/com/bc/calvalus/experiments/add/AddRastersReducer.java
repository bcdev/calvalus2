package com.bc.calvalus.experiments.add;

import com.bc.calvalus.hadoop.io.ByteArrayWritable;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.mapreduce.Reducer;

import java.io.IOException;

public class AddRastersReducer extends Reducer<IntWritable, ByteArrayWritable, IntWritable, ByteArrayWritable> {

    /**
     * This method is called once for each key. Most applications will define
     * their reduce class by overriding this method. The default implementation
     * is an identity function.
     */
    @Override
    protected void reduce(IntWritable key, Iterable<ByteArrayWritable> values, Context context) throws IOException, InterruptedException {
        System.out.println(this + " received key " + key);
        ByteArrayWritable output = null;
        for (ByteArrayWritable input : values) {
            if (output == null) {
                output = new ByteArrayWritable(input.getLength());
            }
            byte[] a = output.getArray();
            byte[] b = input.getArray();
            for (int i = 0; i < a.length; i++) {
                a[i] += b[i];
            }
            context.progress();
        }
        context.write(key, output);
    }
}