package com.bc.calvalus.experiments.processing;

import com.bc.calvalus.hadoop.io.FSImageInputStream;
import com.bc.ceres.core.ProgressMonitor;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FSDataInputStream;
import org.apache.hadoop.fs.FileStatus;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.lib.input.FileSplit;
import org.esa.beam.dataio.envisat.EnvisatProductReaderPlugIn;
import org.esa.beam.dataio.envisat.ProductFile;
import org.esa.beam.framework.dataio.ProductReader;
import org.esa.beam.framework.datamodel.Product;
import org.esa.beam.gpf.operators.meris.NdviOp;
import org.esa.beam.gpf.operators.standard.WriteOp;

import javax.imageio.stream.ImageInputStream;
import java.io.File;
import java.io.IOException;

/**
 * TODO add API doc
 *
 * @author Martin Boettcher
 */
public class L2ProcessingMapper extends Mapper<NullWritable, NullWritable, Text /*N1 input name*/, Text /*split output name*/> {

    @Override
    public void run(Context context) throws IOException, InterruptedException {
        final FileSplit split = (FileSplit) context.getInputSplit();
        final Path path = split.getPath();

        Configuration conf = context.getConfiguration();
        FileSystem fs = path.getFileSystem(conf);
        FSDataInputStream fileIn = fs.open(path);
        final FileStatus status = fs.getFileStatus(path);
        ImageInputStream imageInputStream = new FSImageInputStream(fileIn, status.getLen());

        final EnvisatProductReaderPlugIn plugIn = new EnvisatProductReaderPlugIn();
        final ProductReader productReader = plugIn.createReaderInstance();

        final ProductFile productFile = ProductFile.open(null, imageInputStream, true);

        Product product = productReader.readProductNodes(productFile, null);

        /////////////////

        // for now: single split --> no subset
//        ProductSubsetDef subsetDef = new ProductSubsetDef();
//        subsetDef.setRegion(0, lineNumber.get(), product.getSceneRasterWidth(), recordCount.get());
//        final Product subset = ProductSubsetBuilder.createProductSubset(product, subsetDef, "n", "d");

//        final Product resultProduct = GPF.createProduct("NdviSample", Collections.<String, Object>emptyMap(), product);

        final NdviOp op = new NdviOp();
        op.setSourceProduct("inputProduct", product);
        final Product resultProduct = op.getTargetProduct();
        final String outName = "L2_of_" + path.getName();
        final WriteOp writeOp = new WriteOp(resultProduct, new File(outName + ".dim"), "BEAM-DIMAP");
        writeOp.writeProduct(ProgressMonitor.NULL);

        Path output = getOutputPath(conf);

        final Path dimPath = new Path(output, "L2_of_" + path.getName() + ".dim");
//        final Path dataPath = new Path("output", "L2_of_" + path.getName() + ".data");

        final Path path1 = new Path(new File(outName + ".dim").getAbsolutePath());
        fs.copyFromLocalFile(path1, output);

        // dont copy .data directories are tricky....
//        final Path path2 = new Path(new File(outName + ".data").getAbsolutePath());
//        fs.copyFromLocalFile(path2, output);


        final Text resultKey = new Text(path.getName());
        final Text resultValue = new Text(dimPath.toString());
        context.write(resultKey, resultValue);


    }

    public static Path getOutputPath(Configuration conf) {
    String name = conf.get("mapred.output.dir");
    return name == null ? null: new Path(name);
  }
}
