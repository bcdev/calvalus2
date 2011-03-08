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
import org.esa.beam.framework.gpf.GPF;

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
        BeamProductHandler.init();
        try {
            final FileSplit split = (FileSplit) context.getInputSplit();
            final Path input = split.getPath();

            // parse request
            Configuration configuration = context.getConfiguration();
            ProcessingConfiguration processingConfiguration = new ProcessingConfiguration(configuration);

            final String operatorName = processingConfiguration.getLevel2OperatorName();
            final String requestOutputPath = processingConfiguration.getOutputPath();

            // transform request into parameter objects
            Map<String, Object> parameters = processingConfiguration.getLevel2ParameterMap();

            // write initial log entry for runtime measurements
            LOG.info(context.getTaskAttemptID() + " starts processing of split " + split);
            final long startTime = System.nanoTime();

            // set up input reader
            BeamProductHandler beamProductHandler = new BeamProductHandler();
            final Product sourceProduct = beamProductHandler.readProduct(input, configuration);

            // set up operator and target product
            final Product targetProduct = GPF.createProduct(operatorName, parameters, sourceProduct);
            LOG.info(context.getTaskAttemptID() + " target product created");

            // process input and write target product
            final String outputFileName = "L2_of_" + input.getName() + ".seq";
            final Path outputProductPath = new Path(requestOutputPath, outputFileName);
            StreamingProductWriter.writeProduct(targetProduct, outputProductPath, context, beamProductHandler.getTileHeight());

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



}
