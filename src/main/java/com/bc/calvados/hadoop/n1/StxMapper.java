package com.bc.calvados.hadoop.n1;

import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;
import org.esa.beam.framework.dataio.ProductSubsetBuilder;
import org.esa.beam.framework.dataio.ProductSubsetDef;
import org.esa.beam.framework.datamodel.Product;
import org.esa.beam.framework.datamodel.Stx;

import java.io.IOException;


public class StxMapper extends AbstractN1ProductMapper<IntWritable, Text> {

    private Text word = new Text();

    /**
     * Called once for each key/value pair in the input split. Most applications
     * should override this, but the default is the identity function.
     */
    @Override
    protected void map(IntWritable key, IntWritable value, Context context) throws IOException, InterruptedException {
        int lineNumber = key.get();
        ProductSubsetDef subsetDef = new ProductSubsetDef();
        final Product product = getProduct();
        subsetDef.setRegion(0, lineNumber, product.getSceneRasterWidth(), value.get());
        final Product subset = ProductSubsetBuilder.createProductSubset(product, subsetDef, "n", "d");
        final Stx stx = subset.getBandAt(0).getStx();
        word.set("min=" + stx.getMin() + "  max=" + stx.getMax());
        context.write(key, word);
    }
}
