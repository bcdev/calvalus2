package com.bc.calvalus.processing.beam;

import com.bc.calvalus.commons.CalvalusLogger;
import com.bc.calvalus.processing.JobConfigNames;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.esa.beam.framework.datamodel.Product;
import org.esa.beam.util.io.FileUtils;

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

    private static final String COUNTER_GROUP_NAME_PRODUCTS = "Products";
    private static final Logger LOG = CalvalusLogger.getLogger();

    /**
     * Mapper implementation method. See class comment.
     *
     * @param context task "configuration"
     * @throws java.io.IOException  if installation or process initiation raises it
     * @throws InterruptedException if processing is interrupted externally
     */
    @Override
    public void run(Context context) throws IOException, InterruptedException {
        Configuration jobConfig = context.getConfiguration();
        ProcessorAdapter processorAdapter = ProcessorAdapterFactory.create(context);
        try {
            Path inputPath = processorAdapter.getInputPath();
            String inputFilename = inputPath.getName();
            String outputFilename = "L2_of_" + FileUtils.exchangeExtension(inputFilename, ".seq");
            FileSystem fileSystem = FileSystem.get(jobConfig);
            if (jobConfig.getBoolean(JobConfigNames.CALVALUS_RESUME_PROCESSING, false)) {
                Path outputProductPath = new Path(FileOutputFormat.getOutputPath(context), outputFilename);
                if (fileSystem.exists(outputProductPath)) {
                    LOG.info("resume: target product already exist, skip processing");
                    context.getCounter(COUNTER_GROUP_NAME_PRODUCTS, "Product exist").increment(1);
                    return;
                }
            }
            Product product = processorAdapter.getProcessedProduct();

            if (product == null || product.getSceneRasterWidth() == 0 || product.getSceneRasterHeight() == 0) {
                LOG.warning("target product is empty, skip writing.");
                context.getCounter(COUNTER_GROUP_NAME_PRODUCTS, "Product is empty").increment(1);
            } else {
                LOG.info(context.getTaskAttemptID() + " target product created");
                // process input and write target product
                processorAdapter.saveProcessedProduct(context, outputFilename);
                context.getCounter(COUNTER_GROUP_NAME_PRODUCTS, "Product processed").increment(1);
            }
        } catch (Exception e) {
            LOG.log(Level.SEVERE, "BEAM exception: " + e.toString(), e);
            throw new IOException("BEAM exception: " + e.toString(), e);
        } finally {
            processorAdapter.dispose();
        }
    }
}
