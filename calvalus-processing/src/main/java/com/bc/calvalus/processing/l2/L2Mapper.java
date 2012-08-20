package com.bc.calvalus.processing.l2;

import com.bc.calvalus.commons.CalvalusLogger;
import com.bc.calvalus.processing.JobConfigNames;
import com.bc.calvalus.processing.JobUtils;
import com.bc.calvalus.processing.ProcessorAdapter;
import com.bc.calvalus.processing.ProcessorFactory;
import com.vividsolutions.jts.geom.Geometry;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;

import java.awt.Rectangle;
import java.io.IOException;
import java.util.logging.Level;
import java.util.logging.Logger;

/**
 * Processor adapter for L2 operators.
 * <ul>
 * <li>transforms request to parameter objects</li>
 * <li>instantiates and calls operator</li>
 * </ul>
 *
 * @author Boe
 */
public class L2Mapper extends Mapper<NullWritable, NullWritable, Text /*N1 input name*/, Text /*split output name*/> {

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
        ProcessorAdapter processorAdapter = ProcessorFactory.createAdapter(context);
        try {
            if (jobConfig.getBoolean(JobConfigNames.CALVALUS_RESUME_PROCESSING, false)) {
                LOG.info("resume: testing for target products");
                FileSystem fileSystem = FileSystem.get(jobConfig);
                String[] processedProductPaths = processorAdapter.getProcessedProductPathes();
                if (processedProductPaths != null && processedProductPaths.length > 0) {
                    boolean allExist = true;
                    Path outputPath = FileOutputFormat.getOutputPath(context);
                    for (String processedProductPath : processedProductPaths) {
                        Path outputProductPath = new Path(outputPath, processedProductPath);
                        boolean exists = fileSystem.exists(outputProductPath);
                        if (exists) {
                            LOG.info("resume: target product exist: '" + outputProductPath + "'.");
                        } else {
                            LOG.info("resume: target product is missing: '" + outputProductPath + "'.");
                        }
                        allExist = allExist && exists;
                    }
                    if (allExist) {
                        // nothing to compute, result exists
                        LOG.info("resume: all target product exist.");
                        context.getCounter(COUNTER_GROUP_NAME_PRODUCTS, "Product exist").increment(1);
                        return;
                    }
                }
            }
            Geometry regionGeometry = JobUtils.createGeometry(jobConfig.get(JobConfigNames.CALVALUS_REGION_GEOMETRY));
            Rectangle sourceRectangle = processorAdapter.computeIntersection(regionGeometry);
            if (sourceRectangle.isEmpty()) {
                LOG.warning("product does not cover region, skip processing.");
                context.getCounter(COUNTER_GROUP_NAME_PRODUCTS, "Product is empty").increment(1);
            } else {
                // process input and write target product
                boolean success = processorAdapter.processSourceProduct(sourceRectangle);
                if (success) {
                    LOG.info(context.getTaskAttemptID() + " target product created");
                    processorAdapter.saveProcessedProducts();
                    context.getCounter(COUNTER_GROUP_NAME_PRODUCTS, "Product processed").increment(1);
                } else {
                    LOG.warning("product has not been processed.");
                    context.getCounter(COUNTER_GROUP_NAME_PRODUCTS, "Product not processed").increment(1);
                }
            }
        } catch (Exception e) {
            LOG.log(Level.SEVERE, "BEAM exception: " + e.toString(), e);
            throw new IOException("BEAM exception: " + e.toString(), e);
        } finally {
            processorAdapter.dispose();
        }
    }
}
