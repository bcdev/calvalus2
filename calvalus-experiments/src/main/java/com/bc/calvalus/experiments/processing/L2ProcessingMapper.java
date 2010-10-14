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
import org.esa.beam.framework.dataio.ProductSubsetBuilder;
import org.esa.beam.framework.dataio.ProductSubsetDef;
import org.esa.beam.framework.datamodel.Product;
import org.esa.beam.gpf.operators.meris.NdviOp;
import org.esa.beam.gpf.operators.standard.WriteOp;

import javax.imageio.stream.ImageInputStream;
import java.io.File;
import java.io.IOException;
import java.util.logging.ConsoleHandler;
import java.util.logging.Handler;
import java.util.logging.Level;
import java.util.logging.Logger;

/**
 * Mapper to perform L2 processing (NDVI) on a split. Handles cases A and E,
 * distinguishes them by the class of the split (N1InterleavedInputSplit).
 * Unless the product is a single split a subset of the product is processed.
 * Currently, the output is written to the local file system, and the .dim
 * file is copied to HDFS. The data directory is lost. The key-value pair
 * provided to the reducer is the pair of input file name and output file name.
 *
 * @author Martin Boettcher
 */
public class L2ProcessingMapper extends Mapper<NullWritable, NullWritable, Text /*N1 input name*/, Text /*split output name*/> {

    private static final Logger LOG = Logger.getLogger("com.bc.calvalus");
    static {
        Handler[] handlers = LOG.getHandlers();
        for (Handler handler : handlers) {
            LOG.removeHandler(handler);
        }
        Handler handler = new ConsoleHandler();
        handler.setFormatter(new LogFormatter());
        LOG.addHandler(handler);
        LOG.setLevel(Level.ALL);
    }

    @Override
    public void run(Context context) throws IOException, InterruptedException {
        final FileSplit split = (FileSplit) context.getInputSplit();

        // write initial log entry for runtime measurements
        LOG.info(context.getTaskAttemptID() + " starts processing of split " + split);

        // distinguish cases A to E (currently only A and E)
        final boolean lineInterleaved = split instanceof N1InterleavedInputSplit;
        final boolean singleSplit = ! lineInterleaved; // TODO refine to distinguish not only A from E

        // open the split as ImageInputStream and create a product via an Envisat product reader
        final Path path = split.getPath();
        Configuration conf = context.getConfiguration();
        FileSystem inputFileSystem = path.getFileSystem(conf);
        FSDataInputStream fileIn = inputFileSystem.open(path);
        final FileStatus status = inputFileSystem.getFileStatus(path);
        ImageInputStream imageInputStream = new FSImageInputStream(fileIn, status.getLen());
        final EnvisatProductReaderPlugIn plugIn = new EnvisatProductReaderPlugIn();
        final ProductReader productReader = plugIn.createReaderInstance();
        final ProductFile productFile = ProductFile.open(null, imageInputStream, lineInterleaved);
        Product product = productReader.readProductNodes(productFile, null);

        // for splits replace product by subset
        if (! singleSplit) {
            int yStart = ((N1InterleavedInputSplit) split).getStartRecord();
            int height = ((N1InterleavedInputSplit) split).getNumberOfRecords();
            ProductSubsetDef subsetDef = new ProductSubsetDef();
            subsetDef.setRegion(0, yStart, product.getSceneRasterWidth(), height);
            product = ProductSubsetBuilder.createProductSubset(product, subsetDef, "n", "d");
        }

        // apply operator to product
        final NdviOp op = new NdviOp();
        op.setSourceProduct("inputProduct", product);
        final Product resultProduct = op.getTargetProduct();

        // write product to files in DIMAP format
        final String outName = "L2_of_" + path.getName()
                + ((split instanceof N1InterleavedInputSplit) ? "_split" + ((N1InterleavedInputSplit) split).getStartRecord() : "");
        final WriteOp writeOp = new WriteOp(resultProduct, new File(outName + ".dim"), "BEAM-DIMAP");
        writeOp.writeProduct(ProgressMonitor.NULL);

        // copy .dim output file to output file system
        // TODO handle directory tree or use different output format
        Path output = getOutputPath(conf);
        final Path path1 = new Path(new File(outName + ".dim").getAbsolutePath());
        final Path dimPath = new Path(output, "L2_of_" + path.getName() + ".dim");
        FileSystem outputFileSystem = output.getFileSystem(conf);
        outputFileSystem.copyFromLocalFile(path1, output);

        // dont copy .data directories are tricky....
//        final Path dataPath = new Path("output", "L2_of_" + path.getName() + ".data");
//        final Path path2 = new Path(new File(outName + ".data").getAbsolutePath());
//        inputFileSystem.copyFromLocalFile(path2, output);

        // provide pair of input file name and output file name to reducer
        final Text resultKey = new Text(path.getName());
        final Text resultValue = new Text(dimPath.toString());
        context.write(resultKey, resultValue);

        // write final log entry for runtime measurements
        LOG.info(context.getTaskAttemptID() + " stops processing of split " + split);
    }

    public static Path getOutputPath(Configuration conf) {
        String name = conf.get("mapred.output.dir");
        return name == null ? null : new Path(name);
    }
}
