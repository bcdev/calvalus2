package com.bc.calvalus.experiments.processing;

import com.bc.calvalus.experiments.format.streaming.StreamingProductWriter;
import com.bc.calvalus.experiments.util.CalvalusLogger;
import com.bc.calvalus.hadoop.io.FSImageInputStream;
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
import org.esa.beam.dataio.envisat.RecordReader;
import org.esa.beam.framework.dataio.ProductReader;
import org.esa.beam.framework.dataio.ProductSubsetBuilder;
import org.esa.beam.framework.dataio.ProductSubsetDef;
import org.esa.beam.framework.datamodel.Product;
import org.esa.beam.framework.gpf.Operator;
import org.esa.beam.gpf.operators.meris.NdviOp;
import org.esa.beam.meris.radiometry.MerisRadiometryCorrectionOp;

import javax.imageio.stream.ImageInputStream;
import javax.media.jai.JAI;
import java.io.IOException;
import java.util.logging.Logger;

/**
 * Mapper to perform L2 processing (NDVI or Radiometry) on a split. Handles cases A to E,
 * distinguishes them by the class of the split (N1InterleavedInputSplit) and the splits.number job parameter.
 * Unless the product is a single split a subset of the product is processed.
 * The output is written to a sequence file containing the header and the data in tiles. The key-value pair
 * provided to the reducer is the pair of input file name and output file name.
 *
 * @author Martin Boettcher
 */
public class L2ProcessingMapper extends Mapper<NullWritable, NullWritable, Text /*N1 input name*/, Text /*split output name*/> {

    private static final int TILE_HEIGHT_DEFAULT = 64;
    public static final String TILE_HEIGHT_OPTION = "tileHeight";
    public static final String OPERATOR_OPTION = "operator";
    private static final Logger LOG = CalvalusLogger.getLogger();

    @Override
    public void run(Context context) throws IOException, InterruptedException {

        JAI.enableDefaultTileCache();
        JAI.getDefaultInstance().getTileCache().setMemoryCapacity(512 * 1024 * 1024); // 512 MB


        final FileSplit split = (FileSplit) context.getInputSplit();
        final int numSplits = context.getConfiguration().getInt(N1InputFormat.NUMBER_OF_SPLITS, 1);
        final String operatorName = context.getConfiguration().get(OPERATOR_OPTION, "ndvi").toLowerCase();
        final int tileHeight = context.getConfiguration().getInt(TILE_HEIGHT_OPTION, TILE_HEIGHT_DEFAULT);
        final String outputDir = context.getConfiguration().get("mapred.output.dir");

        // write initial log entry for runtime measurements
        LOG.info(context.getTaskAttemptID() + " starts processing of split " + split);
        long startTime = System.nanoTime();

        // distinguish cases A to E (currently only A and E)
        final boolean lineInterleaved = split instanceof N1InterleavedInputSplit;

        // open the split as ImageInputStream and create a product via an Envisat product reader
        final Path path = split.getPath();
        FileSystem inputFileSystem = path.getFileSystem(context.getConfiguration());
        FSDataInputStream fileIn = inputFileSystem.open(path);
        final FileStatus status = inputFileSystem.getFileStatus(path);
        ImageInputStream imageInputStream = new FSImageInputStream(fileIn, status.getLen());
        System.setProperty("beam.envisat.tileHeight", Integer.toString(tileHeight));
        final EnvisatProductReaderPlugIn plugIn = new EnvisatProductReaderPlugIn();
        final ProductReader productReader = plugIn.createReaderInstance();
        final ProductFile productFile = ProductFile.open(null, imageInputStream, lineInterleaved);
        Product product = productReader.readProductNodes(productFile, null);
        LOG.info(context.getTaskAttemptID() + " source product opened.");

        // for splits replace product by subset
        int yStart = 0;
        int height = 0;
        String splitNamePart = "";
        if (lineInterleaved) {
            N1InterleavedInputSplit n1Split = (N1InterleavedInputSplit) split;
            yStart = n1Split.getStartRecord();
            height = n1Split.getNumberOfRecords();
            splitNamePart = "_split" + yStart;
        } else if (numSplits > 1) {
            long start = split.getStart();
            long length = split.getLength();

            final RecordReader[] mdsRecordReaders = N1ProductAnatomy.getMdsRecordReaders(productFile);
            long headerSize = mdsRecordReaders[0].getDSD().getDatasetOffset();
            long granuleSize = N1ProductAnatomy.computeGranuleSize(mdsRecordReaders);

            if (start == 0) {
                yStart = 0;  // first split contains header, start at first record
            } else {
                yStart = (int) ((start - headerSize + granuleSize - 1) / granuleSize);
            }
            height = (int) ((start + length - headerSize + granuleSize - 1) / granuleSize - yStart);
            splitNamePart = "_split" + yStart;
            LOG.info("multiple split start=" + start + " length=" + length + " yStart=" + yStart + " height=" + height);
        }
        if (height != 0) {
            ProductSubsetDef subsetDef = new ProductSubsetDef();
            subsetDef.setRegion(0, yStart, product.getSceneRasterWidth(), height);
            product = ProductSubsetBuilder.createProductSubset(product, subsetDef, "n", "d");
            // subset builder does not set "preferred tile size"
            product.setPreferredTileSize(product.getSceneRasterWidth(), tileHeight);
            LOG.info(context.getTaskAttemptID() + " subset created: y0=" + yStart + ", h=" + height);
        }

        // apply operator to product
        final Operator op;
        if ("ndvi".equals(operatorName)) {
            op = new NdviOp();
            op.setSourceProduct("inputProduct", product);
        } else if ("radiometry".equals(operatorName)) {
            op = new MerisRadiometryCorrectionOp();
            op.setSourceProduct("sourceProduct", product);
        } else {
            throw new IllegalArgumentException("unknown operator " + operatorName + ". One of ndvi, radiometry expected");
        }
        final Product resultProduct = op.getTargetProduct();
        LOG.info(context.getTaskAttemptID() + " target product created.");

        // write product in the streaming product format
        final String outputFileName = "L2_of_" + path.getName() + splitNamePart + ".seq";
        final Path outputProductPath = new Path(outputDir, outputFileName);
        StreamingProductWriter.writeProduct(resultProduct, outputProductPath, context.getConfiguration(), tileHeight);

        // provide pair of input file name and output file name to reducer
        // commented to try to avoid reduce task at all, which is not successful
//        final Text resultKey = new Text(path.getName());
//        final Text resultValue = new Text(outputProductPath.toString());
//        context.write(resultKey, resultValue);

        // write final log entry for runtime measurements
        long stopTime = System.nanoTime();
        LOG.info(context.getTaskAttemptID() + " stops processing of split " + split + " after " + ((stopTime - startTime) / 1E9) + " sec");
    }
}
