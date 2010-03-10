package com.bc.calvados.hadoop.eodata;

import com.bc.calvados.hadoop.io.ByteArrayWritable;
import com.bc.calvados.hadoop.io.N1InputSplit;
import org.apache.hadoop.fs.FSDataInputStream;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.BytesWritable;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.mapreduce.InputSplit;
import org.apache.hadoop.mapreduce.Mapper;
import org.esa.beam.dataio.envisat.EnvisatProductReaderPlugIn;
import org.esa.beam.dataio.envisat.ProductFile;
import org.esa.beam.framework.dataio.ProductReader;
import org.esa.beam.framework.dataio.ProductSubsetDef;
import org.esa.beam.framework.datamodel.Product;

import javax.imageio.stream.FileCacheImageInputStream;
import javax.imageio.stream.ImageInputStream;
import java.io.File;
import java.io.IOException;


public abstract class AbstractN1ProductMapper<KEYOUT, VALUEOUT> extends Mapper<LongWritable, BytesWritable, KEYOUT, VALUEOUT> {

    private Product product;

    /**
     * Called once at the beginning of the task.
     */
    @Override
    protected void setup(Context context) throws IOException, InterruptedException {
        super.setup(context);
        final N1InputSplit split = (N1InputSplit) context.getInputSplit();
        final Path path = split.getPath();

        FileSystem fs = path.getFileSystem(context.getConfiguration());
        FSDataInputStream fileIn = fs.open(path);
        ImageInputStream imageInputStream = new FileCacheImageInputStream(fileIn, new File("."));

        final EnvisatProductReaderPlugIn plugIn = new EnvisatProductReaderPlugIn();
        final ProductReader productReader = plugIn.createReaderInstance();
        ProductSubsetDef subsetDef = new ProductSubsetDef();
        //subsetDef.setRegion(0, yStart, sceneWidth, height); TODO
        product = productReader.readProductNodes(imageInputStream, subsetDef);
    }

    /**
     * Called once for each key/value pair in the input split. Most applications
     * should override this, but the default is the identity function.
     */
    @Override
    protected void map(LongWritable key, BytesWritable value, Context context) throws IOException, InterruptedException {
        // TODO put data from value into product bands
        //mapImpl();
    }

    /**
     * Called once at the end of the task.
     */
    @Override
    protected void cleanup(Context context) throws IOException, InterruptedException {
        super.cleanup(context);
        product.dispose();
    }
}
