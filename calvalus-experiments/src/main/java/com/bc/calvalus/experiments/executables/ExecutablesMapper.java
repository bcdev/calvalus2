package com.bc.calvalus.experiments.executables;

import com.bc.calvalus.experiments.format.streaming.StreamingProductWriter;
import com.bc.calvalus.experiments.processing.N1InputFormat;
import com.bc.calvalus.experiments.processing.N1InterleavedInputSplit;
import com.bc.calvalus.experiments.processing.N1ProductAnatomy;
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
import org.esa.beam.meris.case2.MerisCase2IOPOperator;
import org.esa.beam.meris.radiometry.MerisRadiometryCorrectionOp;
import org.w3c.dom.NodeList;
import org.xml.sax.InputSource;

import javax.imageio.stream.ImageInputStream;
import javax.media.jai.JAI;
import javax.xml.parsers.DocumentBuilderFactory;
import javax.xml.transform.Source;
import javax.xml.transform.Transformer;
import javax.xml.transform.TransformerFactory;
import javax.xml.transform.dom.DOMSource;
import javax.xml.transform.stream.StreamResult;
import javax.xml.transform.stream.StreamSource;
import javax.xml.xpath.XPath;
import javax.xml.xpath.XPathConstants;
import javax.xml.xpath.XPathFactory;
import java.io.File;
import java.io.FileInputStream;
import java.io.FileReader;
import java.io.IOException;
import java.io.InputStream;
import java.io.OutputStream;
import java.io.Reader;
import java.io.StringReader;
import java.io.StringWriter;
import java.io.Writer;
import java.util.logging.Level;
import java.util.logging.Logger;

import static java.util.logging.Logger.*;

/**
 * Processor adapter for executables.
 * <ul>
 * <li>Checks and maybe installs processor</li>
 * <li>transforms request to call string and parameter file(s)</li>
 * <li>calls executable</li>
 * <li>handles return code and stderr/stdout</li>
 * </ul>
 *
 * @author Boe
 */
public class ExecutablesMapper extends Mapper<NullWritable, NullWritable, Text /*N1 input name*/, Text /*split output name*/> {

    private static final Logger LOG = CalvalusLogger.getLogger();

    private static final String TYPE_XPATH = "/wps:Execute/ows:Identifier";
    private static final String OUTPUT_DIR_XPATH = "/wps:Execute/wps:DataInputs/wps:Input[ows:Identifier='calvalus.output.dir']/wps:Data/wps:LiteralData";
    private static final String PACKAGE_XPATH = "/wps:Execute/wps:DataInputs/wps:Input[ows:Identifier='calvalus.processor.package']/wps:Data/wps:LiteralData";
    private static final String VERSION_XPATH = "/wps:Execute/wps:DataInputs/wps:Input[ows:Identifier='calvalus.processor.version']/wps:Data/wps:LiteralData";

    @Override
    public void run(Context context) throws IOException, InterruptedException, ProcessorException {

        try {
            final FileSplit split = (FileSplit) context.getInputSplit();

            // parse request
            final String requestContent = context.getConfiguration().get("calvalus.request");
            final XmlDoc request = new XmlDoc(requestContent);
            final String requestType = request.getString(TYPE_XPATH);
            final String requestOutputDir = request.getString(OUTPUT_DIR_XPATH);
            final String processorPackage = request.getString(PACKAGE_XPATH);
            final String processorVersion = request.getString(VERSION_XPATH);

            // check for and maybe install processor package
            // TODO
            String requestTransformationPath = "xxx.xsl";
            
            // transform request into command line, write parameter files as side effect
            XslTransformer xsl = new XslTransformer(new File(requestTransformationPath));
            xsl.setParameter("calvalus.input", split.getPath().getName());
            String commandLine = xsl.transform(request.getDocument());
            LOG.info("command line to be executed: " + commandLine);

            // write initial log entry for runtime measurements
            LOG.info(context.getTaskAttemptID() + " starts processing of split " + split);
            long startTime = System.nanoTime();

            // run process for command line
            ProcessBuilder command = new ProcessBuilder(commandLine.split(" "));
            command.redirectErrorStream();
            Process process = command.start();
            // read output until process terminates
            StringBuffer output = new StringBuffer();
            int c;
            while ((c = process.getInputStream().read()) != -1) {
                output.append((char) c);
                if (c == '\n') context.progress();
            }
            // check for termination and handle exit code
            process.waitFor();
            int returnCode = process.exitValue();
            if (returnCode == 0) {
                LOG.info("execution successful: " + output.toString());
            } else {
                throw new ProcessorException("execution of " + commandLine + " failed: " + output.toString())
            }

            // write final log entry for runtime measurements
            long stopTime = System.nanoTime();
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
