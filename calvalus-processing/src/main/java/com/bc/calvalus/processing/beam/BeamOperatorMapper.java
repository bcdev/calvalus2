package com.bc.calvalus.processing.beam;

import com.bc.calvalus.commons.CalvalusLogger;
import com.bc.calvalus.processing.hadoop.FSImageInputStream;
import com.bc.calvalus.processing.shellexec.ProcessorException;
import com.bc.calvalus.processing.shellexec.XmlDoc;
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
import org.esa.beam.framework.gpf.GPF;
import org.w3c.dom.Node;
import org.w3c.dom.NodeList;

import javax.imageio.stream.ImageInputStream;
import java.io.IOException;
import java.util.HashMap;
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

    private static final String TYPE_XPATH = "/Execute/Identifier";
    private static final String OUTPUT_DIR_XPATH = "/Execute/DataInputs/Input[Identifier='calvalus.output.dir']/Data/Reference/@href";
    private static final String INPUTS_XPATH = "/Execute/DataInputs/Input[not(starts-with(Identifier,'calvalus.'))]";
    private static final String INPUT_IDENTIFIER_XPATH = "Identifier";
    private static final String INPUT_LITERAL_DATA_XPATH = "Data/LiteralData";
    private static final String INPUT_COMPLEX_DATA_XPATH = "Data/ComplexData";

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
            final String requestContent = context.getConfiguration().get("calvalus.request");
            final XmlDoc request = new XmlDoc(requestContent);
            final String requestType = request.getString(TYPE_XPATH);
            final String requestOutputPath = request.getString(OUTPUT_DIR_XPATH);

            // transform request into parameter objects
            Map<String, Object> parameters = new HashMap<String, Object>();
            NodeList nodes = request.getNodes(INPUTS_XPATH);
            for (int i = 0; i < nodes.getLength(); ++i) {
                Node node = nodes.item(i);
                String name = request.getString(INPUT_IDENTIFIER_XPATH, node);
                String literalValue = request.getString(INPUT_LITERAL_DATA_XPATH, node, null);
                if (literalValue != null) {
                    parameters.put(name, literalValue);
                } else {
                    // TODO replace with proper conversion of complex nodes
                    //NodeList complexNodes = request.getNodes(INPUT_COMPLEX_DATA_XPATH, node);
                    // ...
                    String complexValue = request.getString(INPUT_COMPLEX_DATA_XPATH, node);
                    parameters.put(name, complexValue);
                }
            }

            // write initial log entry for runtime measurements
            LOG.info(context.getTaskAttemptID() + " starts processing of split " + split);
            final long startTime = System.nanoTime();

            // set up input reader
            final FileSystem        fs               = input.getFileSystem(context.getConfiguration());
            final FileStatus        status           = fs.getFileStatus(input);
            final FSDataInputStream in               = fs.open(input);
            final ImageInputStream  imageInputStream = new FSImageInputStream(in, status.getLen());
            //System.setProperty("beam.envisat.tileHeight", Integer.toString(tileHeight));  // TODO handle tile height or use default
            final EnvisatProductReaderPlugIn plugIn  = new EnvisatProductReaderPlugIn();
            final ProductReader     productReader    = plugIn.createReaderInstance();
            final ProductFile       productFile      = ProductFile.open(null, imageInputStream);
            final Product           sourceProduct    = productReader.readProductNodes(productFile, null);

            // set up operator and target product
            final Product targetProduct = GPF.createProduct(requestType, parameters, sourceProduct);
            LOG.info(context.getTaskAttemptID() + " target product created");

            // process input and write target product
            final String outputFileName = "L2_of_" + input.getName() + ".seq";
            final Path outputProductPath = new Path(requestOutputPath, outputFileName);
            //StreamingProductWriter.writeProduct(targetProduct, outputProductPath, context, tileHeight);
            StreamingProductWriter.writeProduct(targetProduct, outputProductPath, context, 1024);  // TODO use proper writer, maybe use request parameter to select output format

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
