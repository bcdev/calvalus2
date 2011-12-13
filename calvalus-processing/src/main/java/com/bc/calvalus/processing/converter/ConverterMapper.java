package com.bc.calvalus.processing.converter;

import com.bc.calvalus.commons.CalvalusLogger;
import com.bc.calvalus.processing.shellexec.ProcessorException;
import com.bc.calvalus.processing.shellexec.XmlDoc;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.lib.input.FileSplit;
import org.esa.beam.framework.dataio.ProductIO;

import java.io.File;
import java.io.IOException;
import java.util.logging.Level;
import java.util.logging.Logger;

/**
 * Processor adapter for executables.
 * <ul>
 * <li>Checks and maybe installs processor</li>
 * <li>transforms request to command line call string and optionally parameter file(s)</li>
 * <li>calls executable</li>
 * <li>handles return code and stderr/stdout</li>
 * </ul>
 *
 * @author Boe
 */
@Deprecated
public class ConverterMapper extends Mapper<NullWritable, NullWritable, Text /*N1 input name*/, Text /*split output name*/> {

    private static final Logger LOG = CalvalusLogger.getLogger();

    private static final String TYPE_XPATH = "/Execute/Identifier";
    private static final String OUTPUT_DIR_XPATH = "/Execute/DataInputs/Input[Identifier='calvalus.output.dir']/Data/Reference/@href";
    private static final String CONVERTER_CLASS_XPATH = "/Execute/DataInputs/Input[Identifier='calvalus.converter']/Data/LiteralData";
    private static final String TARGET_FORMAT_XPATH = "/Execute/DataInputs/Input[Identifier='calvalus.targetFormat']/Data/LiteralData";
    private static final String ARCHIVE_ROOT_DIR = "/mnt/hdfs";

    private static class ProgressReporter extends Thread {
        Context context;
        private boolean haltFlag = false;
        public synchronized void setHaltFlag() {
            haltFlag = true;
            notify();
        }
        public ProgressReporter(Context context) {
            super("progess-reporter");
            this.context = context;
            setDaemon(true);
        }
        public void run() {
            while (! haltFlag) {
                try {
                    context.progress();
                    synchronized (this) {
                        wait(60000);
                    }
                } catch (InterruptedException _) {
                    // intentionally fall through
                }
            }
        }
    }

    /**
     * Mapper implementation method. See class comment.
     * @param context  task "configuration"
     * @throws java.io.IOException  if installation or process initiation raises it
     * @throws InterruptedException   if processing is interrupted externally
     * @throws com.bc.calvalus.processing.shellexec.ProcessorException  if processing fails
     */
    @Override
    public void run(Context context) throws IOException, InterruptedException, ProcessorException {

        try {
            final FileSplit split = (FileSplit) context.getInputSplit();
            final Path inputPath = split.getPath();

            // write initial log entry for runtime measurements
            LOG.info(context.getTaskAttemptID() + " starts conversion of " + split);
            final long startTime = System.nanoTime();

            // parse request
            final String requestContent = context.getConfiguration().get("calvalus.request");
            final XmlDoc request = new XmlDoc(requestContent);
            final String outputUri = request.getString(OUTPUT_DIR_XPATH);
            final String converterClassName = request.getString(CONVERTER_CLASS_XPATH);

            // ensure that output dir exists
//            final String outputPath = (outputUri.startsWith("hdfs:"))
//                    ? ARCHIVE_ROOT_DIR + File.separator + new Path(outputUri).toUri().getPath()
//                    : new Path(outputUri).toUri().getPath();
//            final File outputDir = new File(outputPath);
//            outputDir.mkdirs();

            // start concurrent thread to report progress
            ProgressReporter progressReporter = new ProgressReporter(context);
            try {
                progressReporter.start();

                // convert
                final FormatConverter converter = (FormatConverter) Class.forName(converterClassName).newInstance();
                converter.convert(context.getTaskAttemptID().toString(),
                                  inputPath,
                                  outputUri,
                                  request.getString(TARGET_FORMAT_XPATH, ProductIO.DEFAULT_FORMAT_NAME),
                                  context.getConfiguration());

            } finally {
                progressReporter.setHaltFlag();
            }

            // write final log entry for runtime measurements
            final long stopTime = System.nanoTime();
            LOG.info(context.getTaskAttemptID() + " stops conversion of split " + split + " after " + ((stopTime - startTime) / 1E9) + " sec");

        } catch (ProcessorException e) {
            LOG.warning(e.getMessage());
            throw e;
        } catch (Exception e) {
            LOG.log(Level.SEVERE, "ConverterMapper exception: " + e.toString(), e);
            throw new ProcessorException("ConverterMapper exception: " + e.toString(), e);
        }
    }
}

