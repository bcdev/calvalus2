package com.bc.calvalus.processing.beam;

import com.bc.calvalus.commons.CalvalusLogger;
import com.bc.calvalus.processing.shellexec.ProcessorException;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.lib.input.FileSplit;
import org.esa.beam.framework.datamodel.Product;

import java.io.IOException;
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
        BeamUtils.initGpf();

        final FileSplit split = (FileSplit) context.getInputSplit();

        // write initial log entry for runtime measurements
        LOG.info(context.getTaskAttemptID() + " starts processing of split " + split);
        final long startTime = System.nanoTime();

        try {
            final Path inputPath = split.getPath();

            // parse request
            Configuration configuration = context.getConfiguration();
            final String requestOutputPath = configuration.get(JobConfNames.CALVALUS_OUTPUT);


            // set up input reader
            Product sourceProduct = BeamUtils.readProduct(inputPath, configuration);

            String roiWkt = configuration.get(JobConfNames.CALVALUS_ROI_WKT);
            Product subsetProduct = null;
            if (roiWkt != null && !roiWkt.isEmpty()) {
                subsetProduct = BeamUtils.createSubsetProduct(sourceProduct, roiWkt);
                if (subsetProduct == null) {
                    sourceProduct.dispose();
                    LOG.info("Product not used");
                    return;
                }
                sourceProduct = subsetProduct;
            }
            // set up operator and target product
            String level2OperatorName = configuration.get(JobConfNames.CALVALUS_L2_OPERATOR);
            String level2Parameters = configuration.get(JobConfNames.CALVALUS_L2_PARAMETER);
            final Product targetProduct = BeamUtils.getProcessedProduct(sourceProduct, level2OperatorName, level2Parameters);
            LOG.info(context.getTaskAttemptID() + " target product created");

            // process input and write target product
            final String outputFileName = "L2_of_" + inputPath.getName() + ".seq";
            final Path outputProductPath = new Path(requestOutputPath, outputFileName);
            StreamingProductWriter.writeProduct(targetProduct, outputProductPath, context, BeamUtils.getTileHeight(configuration));

        } catch (ProcessorException e) {
            LOG.warning(e.getMessage());
            throw e;
        } catch (Exception e) {
            LOG.log(Level.SEVERE, "ExecutablesMapper exception: " + e.toString(), e);
            throw new ProcessorException("ExecutablesMapper exception: " + e.toString(), e);
        } finally {
            // write final log entry for runtime measurements
            final long stopTime = System.nanoTime();
            LOG.info(context.getTaskAttemptID() + " stops processing of split " + split + " after " + ((stopTime - startTime) / 1E9) + " sec");
        }
    }



}
