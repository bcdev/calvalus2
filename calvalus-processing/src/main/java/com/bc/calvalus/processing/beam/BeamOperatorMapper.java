package com.bc.calvalus.processing.beam;

import com.bc.calvalus.commons.CalvalusLogger;
import com.bc.calvalus.processing.hadoop.FSImageInputStream;
import com.bc.calvalus.processing.shellexec.ProcessorException;
import com.bc.calvalus.processing.shellexec.XmlDoc;
import com.bc.ceres.binding.ConversionException;
import com.bc.ceres.binding.PropertyContainer;
import com.bc.ceres.binding.PropertySet;
import com.bc.ceres.binding.ValidationException;
import com.bc.ceres.binding.dom.DefaultDomConverter;
import com.bc.ceres.binding.dom.DomElement;
import com.thoughtworks.xstream.io.xml.xppdom.Xpp3Dom;
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
import org.esa.beam.framework.gpf.Operator;
import org.esa.beam.framework.gpf.OperatorSpi;
import org.esa.beam.framework.gpf.annotations.ParameterDescriptorFactory;
import org.esa.beam.gpf.operators.standard.BandMathsOp;
import org.w3c.dom.NamedNodeMap;
import org.w3c.dom.Node;
import org.w3c.dom.NodeList;

import javax.imageio.stream.ImageInputStream;
import javax.xml.parsers.DocumentBuilderFactory;
import javax.xml.xpath.XPathExpressionException;
import java.io.IOException;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
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
    private static final int TILE_HEIGHT = 512;

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
            GPF.getDefaultInstance().getOperatorSpiRegistry().loadOperatorSpis();

            final FileSplit split = (FileSplit) context.getInputSplit();
            final Path input = split.getPath();

            // parse request
            final String requestContent = context.getConfiguration().get("calvalus.request");
            final XmlDoc request = new XmlDoc(requestContent);
            final String operatorName = request.getString(TYPE_XPATH);
            final String requestOutputPath = request.getString(OUTPUT_DIR_XPATH);

            // transform request into parameter objects
            Map<String, Object> parameters = getOperatorParameters(request);

            // write initial log entry for runtime measurements
            LOG.info(context.getTaskAttemptID() + " starts processing of split " + split);
            final long startTime = System.nanoTime();

            // set up input reader
            final FileSystem        fs               = input.getFileSystem(context.getConfiguration());
            final FileStatus        status           = fs.getFileStatus(input);
            final FSDataInputStream in               = fs.open(input);
            final ImageInputStream  imageInputStream = new FSImageInputStream(in, status.getLen());
            System.setProperty("beam.envisat.tileHeight", Integer.toString(TILE_HEIGHT));
            final EnvisatProductReaderPlugIn plugIn  = new EnvisatProductReaderPlugIn();
            final ProductReader     productReader    = plugIn.createReaderInstance();
            final Product           sourceProduct    = productReader.readProductNodes(imageInputStream, null);

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
     * Transforms request into parameter objects.
     *
     * @param request the XML request
     * @return map of opererator parameters
     */
    static Map<String, Object> getOperatorParameters(XmlDoc request) throws XPathExpressionException, ValidationException, ConversionException {
        Map<String, Object> parameterMap = new HashMap<String, Object>();
        NodeList calvalusNodes = request.getNodes(INPUTS_XPATH);
        for (int i = 0; i < calvalusNodes.getLength(); ++i) {
            Node node = calvalusNodes.item(i);
            String name = request.getString(INPUT_IDENTIFIER_XPATH, node);
            String literalValue = request.getString(INPUT_LITERAL_DATA_XPATH, node);
            // TODO literalValue is never null !!!! mz
            if (literalValue != null && !literalValue.isEmpty()) {
                parameterMap.put(name, literalValue);
            } else {
                // TODO replace with proper conversion of complex nodes
                //NodeList complexNodes = request.getNodes(INPUT_COMPLEX_DATA_XPATH, node);
                // ...

                Node complexNode = request.getNode(INPUT_COMPLEX_DATA_XPATH, node);
                String operatorName = request.getString(TYPE_XPATH);
                OperatorSpi operatorSpi = GPF.getDefaultInstance().getOperatorSpiRegistry().getOperatorSpi(operatorName);
                Class<? extends Operator> operatorClass = operatorSpi.getOperatorClass();
                DomElement complexDomElement = new NodeDomElement(complexNode);
                ParameterDescriptorFactory parameterDescriptorFactory = new ParameterDescriptorFactory();
                PropertySet parameterSet = PropertyContainer.createMapBacked(parameterMap, operatorClass, parameterDescriptorFactory);
                DefaultDomConverter domConverter = new DefaultDomConverter(operatorClass, parameterDescriptorFactory);
                domConverter.convertDomToValue(complexDomElement, parameterSet);
            }
        }


        return parameterMap;
    }

    private static class NodeDomElement implements DomElement {
        private final Node node;

        public NodeDomElement(Node node) {
            this.node = node;
        }

        @Override
        public void setParent(DomElement parent) {
            throw new IllegalStateException("NodeDomElement is immutable.");
        }

        @Override
        public int getChildCount() {
            return node.getChildNodes().getLength();
        }

        @Override
        public DomElement getChild(int index) {
            return new NodeDomElement(node.getChildNodes().item(index));
        }

        @Override
        public void setAttribute(String name, String value) {
            throw new IllegalStateException("NodeDomElement is immutable.");
        }

        @Override
        public DomElement createChild(String name) {
            throw new IllegalStateException("NodeDomElement is immutable.");
        }

        @Override
        public void addChild(DomElement childElement) {
            throw new IllegalStateException("NodeDomElement is immutable.");
        }

        @Override
        public void setValue(String value) {
            throw new IllegalStateException("NodeDomElement is immutable.");
        }

        @Override
        public String toXml() {
            return "";
        }

        @Override
        public String getName() {
            return node.getNodeName();
        }

        @Override
        public String getValue() {
            return node.getNodeValue();
        }

        @Override
        public String getAttribute(String attributeName) {
            return node.getAttributes().getNamedItem(attributeName).getNodeValue();
        }

        @Override
        public String[] getAttributeNames() {
            NamedNodeMap attributes = node.getAttributes();
            String[] attributeNames = new String[attributes.getLength()];
            for (int i = 0; i < attributeNames.length; i++) {
                attributeNames[i] = attributes.item(i).getNodeName();
            }
            return attributeNames;
        }

        @Override
        public DomElement getParent() {
            return new NodeDomElement(node.getParentNode());
        }

        @Override
        public DomElement getChild(String elementName) {
            NodeList childNodes = node.getChildNodes();
            for (int i = 0; i < childNodes.getLength(); i++) {
                Node childNode = childNodes.item(i);
                if (childNode.getNodeName().equals(elementName)) {
                    return new NodeDomElement(childNode);
                }
            }
            return null;
        }

        @Override
        public DomElement[] getChildren() {
            DomElement[] children = new DomElement[getChildCount()];
            for (int i = 0; i < children.length; i++) {
                children[i] = getChild(i);
            }
            return children;
        }

        @Override
        public DomElement[] getChildren(String elementName) {
            NodeList childNodes = node.getChildNodes();
            List<DomElement> children = new ArrayList<DomElement>(childNodes.getLength());
            for (int i = 0; i < childNodes.getLength(); i++) {
                Node child = childNodes.item(i);
                if (child.getNodeName().equals(elementName)) {
                    children.add(new NodeDomElement(child));
                }
            }
            return children.toArray(new NodeDomElement[children.size()]);
        }
    }
}
