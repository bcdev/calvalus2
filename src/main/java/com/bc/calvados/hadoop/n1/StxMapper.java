package com.bc.calvados.hadoop.n1;

import com.bc.calvados.hadoop.io.FSImageInputStream;
import org.apache.hadoop.fs.FSDataInputStream;
import org.apache.hadoop.fs.FileStatus;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;
import org.esa.beam.dataio.envisat.EnvisatProductReaderPlugIn;
import org.esa.beam.dataio.envisat.ProductFile;
import org.esa.beam.framework.dataio.ProductReader;
import org.esa.beam.framework.dataio.ProductSubsetBuilder;
import org.esa.beam.framework.dataio.ProductSubsetDef;
import org.esa.beam.framework.datamodel.Product;
import org.esa.beam.framework.datamodel.Stx;

import javax.imageio.stream.ImageInputStream;
import java.io.IOException;


public class StxMapper extends Mapper<IntWritable, IntWritable, Text, StxWritable> {

    private Text resultKey = new Text();
    private StxWritable stxResult = new StxWritable();

    /**
     * Called once for each key/value pair in the input split. Most applications
     * should override this, but the default is the identity function.
     */
    @Override
    protected void map(IntWritable lineNumber, IntWritable recordCount, Mapper.Context context) throws IOException, InterruptedException {
        ProductSubsetDef subsetDef = new ProductSubsetDef();
        final Product product = getProduct();
        subsetDef.setRegion(0, lineNumber.get(), product.getSceneRasterWidth(), recordCount.get());
        final Product subset = ProductSubsetBuilder.createProductSubset(product, subsetDef, "n", "d");
        final Stx stx = subset.getBandAt(0).getStx();
        stxResult.setMin(stx.getMin());
        stxResult.setMax(stx.getMax());
        resultKey.set(product.getName());
        context.write(resultKey, stxResult);
    }

    private Product product;

    /**
     * Called once at the beginning of the task.
     */
    @Override
    protected void setup(Mapper.Context context) throws IOException, InterruptedException {
        final N1LineInputSplit split = (N1LineInputSplit) context.getInputSplit();
        final Path path = split.getPath();

        FileSystem fs = path.getFileSystem(context.getConfiguration());
        FSDataInputStream fileIn = fs.open(path);
        final FileStatus status = fs.getFileStatus(path);
        ImageInputStream imageInputStream = new FSImageInputStream(fileIn, status.getLen());

        final EnvisatProductReaderPlugIn plugIn = new EnvisatProductReaderPlugIn();
        final ProductReader productReader = plugIn.createReaderInstance();

        final ProductFile productFile = ProductFile.open(null, imageInputStream, true);

        product = productReader.readProductNodes(productFile, null);
    }

    /**
     * Called once at the end of the task.
     */
    @Override
    protected void cleanup(Mapper.Context context) throws IOException, InterruptedException {
        product.dispose();
    }

    protected Product getProduct() {
        return product;
    }
}
