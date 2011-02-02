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
import com.bc.ceres.binding.dom.DefaultDomElement;
import com.bc.ceres.binding.dom.DomElement;
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
import org.w3c.dom.NamedNodeMap;
import org.w3c.dom.Node;
import org.w3c.dom.NodeList;

import javax.imageio.stream.ImageInputStream;
import javax.xml.xpath.XPathExpressionException;
import java.io.IOException;
import java.text.MessageFormat;
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

    private static final String OUTPUT_DIR_XPATH = "/Execute/DataInputs/Input[Identifier='calvalus.output.dir']/Data/Reference/@href";
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
            final String operatorName = request.getString("/Execute/Identifier");
            final String requestOutputPath = request.getString(OUTPUT_DIR_XPATH);

            // transform request into parameter objects
            Map<String, Object> parameters = getProcessingParameters(request);

            // write initial log entry for runtime measurements
            LOG.info(context.getTaskAttemptID() + " starts processing of split " + split);
            final long startTime = System.nanoTime();

            // set up input reader
            final FileSystem fs = input.getFileSystem(context.getConfiguration());
            final FileStatus status = fs.getFileStatus(input);
            final FSDataInputStream in = fs.open(input);
            final ImageInputStream imageInputStream = new FSImageInputStream(in, status.getLen());
            System.setProperty("beam.envisat.tileHeight", Integer.toString(TILE_HEIGHT));
            final EnvisatProductReaderPlugIn plugIn = new EnvisatProductReaderPlugIn();
            final ProductReader productReader = plugIn.createReaderInstance();
            final Product sourceProduct = productReader.readProductNodes(imageInputStream, null);

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
    static Map<String, Object> getProcessingParameters(XmlDoc request) throws ValidationException, ConversionException {

        DomElement parametersElement = getProcessingParametersElement(request);
        String operatorName = null;
        try {
            operatorName = request.getString("/Execute/Identifier");
        } catch (XPathExpressionException e) {
            throw new IllegalStateException("Illegal XPath expression", e);
        }

        OperatorSpi operatorSpi = GPF.getDefaultInstance().getOperatorSpiRegistry().getOperatorSpi(operatorName);
        if (operatorSpi == null) {
            throw new ConversionException(MessageFormat.format("Unknown operator ''{0}''", operatorName));
        }
        Class<? extends Operator> operatorClass = operatorSpi.getOperatorClass();

        Map<String, Object> parameterMap = new HashMap<String, Object>();
        ParameterDescriptorFactory parameterDescriptorFactory = new ParameterDescriptorFactory();
        PropertySet parameterSet = PropertyContainer.createMapBacked(parameterMap, operatorClass, parameterDescriptorFactory);
        DefaultDomConverter domConverter = new DefaultDomConverter(operatorClass, parameterDescriptorFactory);
        domConverter.convertDomToValue(parametersElement, parameterSet);

        return parameterMap;
    }

    static DomElement getProcessingParametersElement(XmlDoc request) {
        try {
            DefaultDomElement parametersElement = new DefaultDomElement("parameters");
            NodeList nodeList = request.getNodes("/Execute/DataInputs/Input[not(starts-with(Identifier,'calvalus.'))]");
            for (int i = 0; i < nodeList.getLength(); i++) {
                Node node = nodeList.item(i);
                String name = request.getString("Identifier", node);
                Node complexValue = request.getNode("Data/ComplexData/*", node);
                if (complexValue != null) {
                    DomElement child = new NodeDomElement(complexValue);
                    parametersElement.addChild(child);
                } else {
                    String literalValue = request.getString("Data/LiteralData", node, null);
                    if (literalValue != null) {
                        DomElement child = parametersElement.createChild(name);
                        child.setValue(literalValue);
                    }
                }
            }
            return parametersElement;
        } catch (XPathExpressionException e) {
            throw new IllegalStateException("Illegal XPath expression", e);
        }
    }

    /**
     * Note: this class is not thread save, nor may the input node be changed.
     */
    private static class NodeDomElement implements DomElement {
        private final Node node;
        private DomElement parent;
        private ArrayList<NodeDomElement> elementList;

        public NodeDomElement(Node node) {
            this.node = node;
        }

        @Override
        public String getName() {
            return node.getNodeName();
        }

        @Override
        public String getValue() {
            return node.getTextContent();
        }

        @Override
        public void setParent(DomElement parent) {
            this.parent = parent;
        }

        @Override
        public int getChildCount() {
            return getElementList().size();
        }

        @Override
        public DomElement getChild(int index) {
            return getElementList().get(index);
        }

        @Override
        public String toXml() {
            return "";
        }

        @Override
        public String getAttribute(String attributeName) {
            NamedNodeMap attributes = node.getAttributes();
            if (attributes == null) {
                return null;
            }
            Node namedItem = attributes.getNamedItem(attributeName);
            if (namedItem == null) {
                return null;
            }
            return namedItem.getNodeValue();
        }

        @Override
        public String[] getAttributeNames() {
            NamedNodeMap attributes = node.getAttributes();
            if (attributes == null) {
                return new String[0];
            }
            String[] attributeNames = new String[attributes.getLength()];
            for (int i = 0; i < attributeNames.length; i++) {
                attributeNames[i] = attributes.item(i).getNodeName();
            }
            return attributeNames;
        }

        @Override
        public DomElement getParent() {
            return parent;
        }

        @Override
        public DomElement getChild(String elementName) {
            List<NodeDomElement> elementList = getElementList();
            for (NodeDomElement nodeDomElement : elementList) {
                if (nodeDomElement.getName().equals(elementName)) {
                    return nodeDomElement;
                }
            }
            return null;
        }

        @Override
        public DomElement[] getChildren() {
            List<NodeDomElement> elementList = getElementList();
            return elementList.toArray(new DomElement[elementList.size()]);
        }

        @Override
        public DomElement[] getChildren(String elementName) {
            List<NodeDomElement> elementList = getElementList();
            List<DomElement> children = new ArrayList<DomElement>(elementList.size());
            for (NodeDomElement element : elementList) {
                if (element.getName().equals(elementName)) {
                    children.add(element);
                }
            }
            return children.toArray(new DomElement[children.size()]);
        }

        private List<NodeDomElement> getElementList() {
            if (elementList == null) {
                elementList = new ArrayList<NodeDomElement>();
                NodeList childNodes = node.getChildNodes();
                for (int i = 0; i < childNodes.getLength(); i++) {
                    Node childNode = childNodes.item(i);
                    short nodeType = childNode.getNodeType();
                    if (nodeType == 1) {
                        elementList.add(new NodeDomElement(childNode));
                    }
                }
            }
            return elementList;
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

    }
}
