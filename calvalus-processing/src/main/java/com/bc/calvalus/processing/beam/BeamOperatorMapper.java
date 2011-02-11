package com.bc.calvalus.processing.beam;

import com.bc.calvalus.commons.CalvalusLogger;
import com.bc.calvalus.processing.hadoop.FSImageInputStream;
import com.bc.calvalus.processing.shellexec.ProcessorException;
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
import org.esa.beam.framework.dataio.ProductReader;
import org.esa.beam.framework.datamodel.Product;
import org.esa.beam.framework.gpf.GPF;
import org.esa.beam.util.SystemUtils;

import javax.imageio.stream.ImageInputStream;
import javax.media.jai.JAI;
import java.io.IOException;
import java.util.Map;
import java.util.logging.Level;
import java.util.logging.Logger;

/**
 * Processor adapter for BEAM L2 operators.
 * <ul>
 * <li>transforms request to parameter objects</li>
 * <li>instantiates and calls operator</li>
 * </ul>
 *
 * @author Boe
 */
public class BeamOperatorMapper extends Mapper<NullWritable, NullWritable, Text /*N1 input name*/, Text /*split output name*/> {

    private static final Logger LOG = CalvalusLogger.getLogger();

    //TODO make this an option
    private static final int TILE_HEIGHT = 128;
    private static final int TILE_CACHE_SIZE_M = 512;  // 512 MB

    static {
        SystemUtils.init3rdPartyLibs(BeamOperatorMapper.class.getClassLoader());
        JAI.enableDefaultTileCache();
        JAI.getDefaultInstance().getTileCache().setMemoryCapacity(TILE_CACHE_SIZE_M * 1024 * 1024);
        GPF.getDefaultInstance().getOperatorSpiRegistry().loadOperatorSpis();
    }

    /**
     * Mapper implementation method. See class comment.
     *
     * @param context task "configuration"
     * @throws java.io.IOException  if installation or process initiation raises it
     * @throws InterruptedException if processing is interrupted externally
     * @throws com.bc.calvalus.processing.shellexec.ProcessorException
     *                              if processing fails
     */
    @Override
    public void run(Context context) throws IOException, InterruptedException, ProcessorException {

        try {
            final FileSplit split = (FileSplit) context.getInputSplit();
            final Path input = split.getPath();

            // parse request
            Configuration configuration = context.getConfiguration();
            final String requestContent = configuration.get("calvalus.request");
            BeamOperatorConfiguration opConfig = new BeamOperatorConfiguration(requestContent);

            final String operatorName = opConfig.getOperatorName();
            final String requestOutputPath = opConfig.getRequestOutputDir();

            // transform request into parameter objects
            Map<String, Object> parameters = opConfig.getOperatorParameters();

            // write initial log entry for runtime measurements
            LOG.info(context.getTaskAttemptID() + " starts processing of split " + split);
            final long startTime = System.nanoTime();

            // set up input reader
            final Product sourceProduct = readProduct(input, configuration);

            // set up operator and target product
            final Product targetProduct = GPF.createProduct(operatorName, parameters, sourceProduct);
            LOG.info(context.getTaskAttemptID() + " target product created");

            // process input and write target product
            final String outputFileName = "L2_of_" + input.getName() + ".seq";
            final Path outputProductPath = new Path(requestOutputPath, outputFileName);
            StreamingProductWriter.writeProduct(targetProduct, outputProductPath, context, TILE_HEIGHT);

            // write final log entry for runtime measurements
            final long stopTime = System.nanoTime();
            LOG.info(context.getTaskAttemptID() + " stops processing of split " + split + " after " + ((stopTime - startTime) / 1E9) + " sec");

        } catch (ProcessorException e) {
            LOG.warning(e.getMessage());
            throw e;
        } catch (Exception e) {
            LOG.log(Level.SEVERE, "ExecutablesMapper exception: " + e.toString(), e);
            throw new ProcessorException("ExecutablesMapper exception: " + e.toString(), e);
        }
    }

    /**
     * Reads a product from the distributed file system.
     *
     * @param input         The input path
     * @param configuration the configuration
     * @return The product
     * @throws IOException
     */
    private Product readProduct(Path input, Configuration configuration) throws IOException {
        final FileSystem fs = input.getFileSystem(configuration);
        final FileStatus status = fs.getFileStatus(input);
        final FSDataInputStream in = fs.open(input);
        final ImageInputStream imageInputStream = new FSImageInputStream(in, status.getLen());
        System.setProperty("beam.envisat.tileHeight", Integer.toString(TILE_HEIGHT));
        final EnvisatProductReaderPlugIn plugIn = new EnvisatProductReaderPlugIn();
        final ProductReader productReader = plugIn.createReaderInstance();
        return productReader.readProductNodes(imageInputStream, null);
    }


}
