package com.bc.calvalus.processing.beam;

import com.bc.calvalus.commons.CalvalusLogger;
import com.bc.calvalus.commons.DateUtils;
import com.bc.calvalus.processing.JobConfigNames;
import com.bc.calvalus.processing.ProcessorFactory;
import com.bc.calvalus.processing.executable.PropertiesHandler;
import com.bc.calvalus.processing.l2.ProductFormatter;
import com.bc.calvalus.processing.xml.XmlConvertible;
import com.bc.ceres.binding.BindingException;
import com.bc.ceres.binding.ConversionException;
import com.bc.ceres.binding.PropertyContainer;
import com.bc.ceres.binding.ValidationException;
import com.bc.ceres.binding.dom.DefaultDomConverter;
import com.bc.ceres.binding.dom.DomConverter;
import com.bc.ceres.binding.dom.DomElement;
import com.bc.ceres.binding.dom.XppDomElement;
import com.bc.ceres.core.ProgressMonitor;
import com.bc.ceres.resource.ReaderResource;
import com.bc.ceres.resource.Resource;
import com.bc.ceres.resource.ResourceEngine;
import com.thoughtworks.xstream.io.xml.xppdom.XppDom;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileStatus;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.fs.PathFilter;
import org.apache.hadoop.mapreduce.MapContext;
import org.apache.velocity.VelocityContext;
import org.esa.snap.core.datamodel.MetadataAttribute;
import org.esa.snap.core.datamodel.MetadataElement;
import org.esa.snap.core.datamodel.Product;
import org.esa.snap.core.gpf.Operator;
import org.esa.snap.core.gpf.OperatorException;
import org.esa.snap.core.gpf.annotations.Parameter;
import org.esa.snap.core.gpf.annotations.ParameterBlockConverter;
import org.esa.snap.core.gpf.annotations.ParameterDescriptorFactory;
import org.esa.snap.core.gpf.graph.Graph;
import org.esa.snap.core.gpf.graph.GraphContext;
import org.esa.snap.core.gpf.graph.GraphException;
import org.esa.snap.core.gpf.graph.GraphIO;
import org.esa.snap.core.gpf.graph.GraphProcessor;
import org.esa.snap.core.gpf.graph.Header;
import org.esa.snap.core.gpf.graph.HeaderSource;
import org.esa.snap.core.gpf.graph.HeaderTarget;
import org.esa.snap.core.gpf.graph.Node;
import org.esa.snap.core.gpf.graph.NodeContext;
import org.jdom2.Element;
import org.jdom2.output.Format;
import org.jdom2.output.XMLOutputter;

import java.io.BufferedInputStream;
import java.io.BufferedOutputStream;
import java.io.BufferedReader;
import java.io.File;
import java.io.FileInputStream;
import java.io.FileReader;
import java.io.IOException;
import java.io.InputStream;
import java.io.InputStreamReader;
import java.io.OutputStream;
import java.io.Reader;
import java.io.StringReader;
import java.text.DateFormat;
import java.text.ParseException;
import java.util.*;
import java.util.logging.Logger;
import java.util.zip.ZipEntry;
import java.util.zip.ZipOutputStream;

/**
 * A processor adapter that uses a SNAP GPF {@code Graph} to process input products.
 *
 * @author MarcoZ
 */
public class SnapGraphAdapter extends SubsetProcessorAdapter {

    private static final DateFormat N1_TIME_FORMAT = DateUtils.createDateFormat("yyyyMMdd_HHmmss");
    private static final DateFormat YMD_DIR_FORMAT = DateUtils.createDateFormat("yyyy/MM/dd");

    private GraphContext graphContext;
    private Product targetProduct;
    private boolean shallSaveTarget;

    public SnapGraphAdapter(MapContext mapContext) {
        super(mapContext);

        if (getConfiguration().get(JobConfigNames.CALVALUS_REGION_GEOMETRY) != null) {
            if (getConfiguration().get("calvalus.system.snap.dataio.reader.tileHeight") == null) {
                System.setProperty("snap.dataio.reader.tileHeight", "64");
                getLogger().info("Setting tileHeight to 64 for graph subsetting");
            }
            if ((getConfiguration().get("calvalus.system.snap.dataio.reader.tileWidth") == null
                    || "*".equals(getConfiguration().get("calvalus.system.snap.dataio.reader.tileWidth")))
                    && !getConfiguration().getBoolean(JobConfigNames.CALVALUS_INPUT_FULL_SWATH, false)) {
                System.setProperty("snap.dataio.reader.tileWidth", "64");
                getLogger().info("Setting tileWidth to 64 for graph subsetting");
            }
        }
    }

    @Override
    public boolean supportsPullProcessing() {
        return true;
    }

    @Override
    public boolean processSourceProduct(MODE mode, ProgressMonitor pm) throws IOException {
        pm.setSubTaskName("SNAP Level 2");

        try {
            Graph graph = createGraph();
            if (graph == null) {
                getLogger().info("Skip processing");
                return false;
            }
            Header header = graph.getHeader();
            graphContext = buildGraphContext(graph, header);
            logGraphState();
            HeaderTarget target = header.getTarget();
            XppDom calvalusAppData = graph.getApplicationData("calvalus");

            if (mode == MODE.EXECUTE && calvalusAppData != null) {
                boolean success = executeGraphAndCollectOutput(graph, calvalusAppData, pm);
                Product[] outputProducts;
                try {
                    outputProducts = graphContext.getOutputProducts();
                } catch (NullPointerException e) {
                    outputProducts = new Product[0];
                }
                if (success && target != null && target.getNodeId() != null && outputProducts.length > 0) {
                    //graphContext.getOutputProducts()[0];
                    targetProduct = getTargetProductFromGraph(graph, target.getNodeId());
                    return postprocessTargetProduct();
                }
                return success;
            } else {
                if (target == null || target.getNodeId() == null) {
                    throw new IllegalArgumentException("no 'target' product given in graph.");
                }
                shallSaveTarget = true;
                targetProduct = getTargetProductFromGraph(graph, target.getNodeId());
                return postprocessTargetProduct();
            }
        } catch (Exception e) {
            throw new IOException("Error executing Graph: " + e.getMessage(), e);
        } finally {
            pm.done();
        }
    }

    private void logGraphState() {
        for (Iterator<NodeContext> iterator = graphContext.getInitNodeContextDeque().descendingIterator(); iterator.hasNext(); ) {
            NodeContext nodeContext = iterator.next();
            CalvalusProductIO.printProductOnStdout(nodeContext.getTargetProduct(),
                                                   "computed by node " + nodeContext.getNode().getId());
        }
    }

    private boolean executeGraphAndCollectOutput(Graph graph, XppDom calvalusAppData, ProgressMonitor pm) throws Exception {
        CalvalusGraphApplicationData appData = CalvalusGraphApplicationData.fromDom(calvalusAppData);
        getLogger().info("Executing graph");
        GraphProcessor processor = new GraphProcessor();
        processor.executeGraph(graphContext, pm);
        graphContext.dispose();

        // collect results files
        List<File> outputFileList = new ArrayList<>();
        if (appData.outputFiles != null) {
            for (String outputFile : appData.outputFiles) {
                File file = new File(outputFile);
                if (file.exists()) {
                    outputFileList.add(file);
                } else {
                    getLogger().warning("outputFile '" + outputFile + "' does not exist.");
                }
            }
        }
        if (appData.outputNodes != null) {
            for (OutputNodeRef ref : appData.outputNodes) {
                Node node = graph.getNode(ref.nodeId);
                if (node != null) {
                    DomElement configuration = node.getConfiguration();
                    if (configuration != null) {
                        DomElement domElement = configuration.getChild(ref.parameter);
                        if (domElement != null) {
                            File file = new File(domElement.getValue());
                            if (file.exists()) {
                                outputFileList.add(file);
                            } else {
                                getLogger().warning("outputNode referenced file '" + ref.nodeId + "." + ref.parameter + "' does not exist.");
                            }
                        } else {
                            getLogger().warning("outputNode parameter '" + ref.nodeId + "." + ref.parameter + "' does not exist.");
                        }
                    } else {
                        getLogger().warning("outputNode '" + ref.nodeId + "' has no configuration.");
                    }
                } else {
                    getLogger().warning("outputNode '" + ref.nodeId + "' does not exist.");
                }
            }
        }
        MapContext mapContext = getMapContext();
        if (appData.archiveFile != null) {
            // create zip with all files
            Path workPath = new Path(getWorkOutputDirectoryPath(), appData.archiveFile);
            OutputStream outputStream = workPath.getFileSystem(mapContext.getConfiguration()).create(workPath);
            ZipOutputStream zipOutputStream = new ZipOutputStream(new BufferedOutputStream(outputStream));
            zipOutputStream.setMethod(ZipEntry.DEFLATED);
            try {
                for (File outputFile : outputFileList) {
                    getLogger().info("adding to zip archive: " + outputFile.getName());
                    ZipEntry zipEntry = new ZipEntry(outputFile.getName().replace('\\', '/'));
                    FileInputStream inputStream = new FileInputStream(outputFile);
                    try {
                        zipOutputStream.putNextEntry(zipEntry);
                        ProductFormatter.copy(inputStream, zipOutputStream, mapContext);
                        zipOutputStream.closeEntry();
                    } finally {
                        inputStream.close();
                    }
                }
            } finally {
                zipOutputStream.close();
            }
        } else {
            // archive individual files
            for (File outputFile : outputFileList) {
                getLogger().info("copying to HDFS: " + outputFile.getName());
                InputStream is = new BufferedInputStream(new FileInputStream(outputFile));
                Path workPath = new Path(getWorkOutputDirectoryPath(), outputFile.getName());
                OutputStream os = workPath.getFileSystem(mapContext.getConfiguration()).create(workPath);
                ProductFormatter.copyAndClose(is, os, mapContext);
            }
        }
        return true;
    }

    private boolean postprocessTargetProduct() throws IOException {
        if (getConfiguration().getBoolean(JobConfigNames.CALVALUS_OUTPUT_SUBSETTING, false)) {
            getLogger().info("output subsetting of split " + getInputPath());
            targetProduct = createSubsetFromOutput(targetProduct);
        }
        if (targetProduct.getSceneRasterWidth() == 0 || targetProduct.getSceneRasterHeight() == 0) {
            getLogger().info("Skip processing");
            return false;
        }
        getLogger().info(String.format("Processed product width = %d height = %d",
                                       targetProduct.getSceneRasterWidth(),
                                       targetProduct.getSceneRasterHeight()));
        if (hasInvalidStartAndStopTime(targetProduct)) {
            copySceneRasterStartAndStopTime(getInputProduct(), targetProduct, null);
        }
        return true;
    }

    private Product getTargetProductFromGraph(Graph graph, String targetNodeId) {
        Node targetNode = graph.getNode(targetNodeId);
        if (targetNode == null) {
            throw new IllegalArgumentException("Specified targetNode '" + targetNodeId + "' does not exist in graph.");
        }
        return graphContext.getNodeContext(targetNode).getTargetProduct();
    }

    private GraphContext buildGraphContext(Graph graph, Header header) throws IOException, GraphException {
        List<HeaderSource> sources = header.getSources();
        Path inputPath = getInputPath();
        Path qualifiedInputPath = inputPath.getFileSystem(getConfiguration()).makeQualified(inputPath);
        Operator sourceProducts = new SourceProductContainerOperator();
        for (HeaderSource headerSource : sources) {
            String sourceId = headerSource.getName();
            Path sourcePath = new Path(headerSource.getLocation());

            String inputFormat = null;
            if (headerSource.getDescription() != null && headerSource.getDescription().startsWith("format:")) {
                inputFormat = headerSource.getDescription().substring("format:".length());
            }
            Path qualifiedSourcePath = sourcePath.getFileSystem(getConfiguration()).makeQualified(sourcePath);
            Product sourceProduct;
            if (qualifiedInputPath.equals(qualifiedSourcePath) ) {
                // main input, maybe re-read with specific input format
                if (inputFormat == null) {
                    sourceProduct = getInputProduct();
                } else {
                    getConfiguration().set(JobConfigNames.CALVALUS_INPUT_FORMAT, inputFormat);
                    sourceProduct = CalvalusProductIO.readProduct(sourcePath, getConfiguration(), inputFormat);
                }
                if (getConfiguration().getBoolean(JobConfigNames.CALVALUS_INPUT_SUBSETTING, true)) {
                    getLogger().info("input subsetting of split " + sourcePath);
                    sourceProduct = createSubsetFromInput(sourceProduct);
                }
            } else {
                // other source product
                sourceProduct = CalvalusProductIO.readProduct(sourcePath, getConfiguration(), inputFormat);
            }
            sourceProducts.setSourceProduct(sourceId, sourceProduct);
        }
        return new GraphContext(graph, sourceProducts);
    }

    @Override
    public Product openProcessedProduct() {
        return targetProduct;
    }

    @Override
    public void saveProcessedProducts(ProgressMonitor pm) throws IOException {
        if (shallSaveTarget) {
            saveTargetProduct(targetProduct, pm);
        }
    }

    @Override
    public void dispose() {
        super.dispose();
        if (graphContext != null) {
            graphContext.dispose();
            graphContext = null;
        }
        if (targetProduct != null) {
            targetProduct.dispose();
            targetProduct = null;
        }
    }

    public Graph createGraph() throws GraphException, IOException {
        Path inputPath = getInputPath();
        CalvalusLogger.getLogger().info("Creating graph for input: " + inputPath);
        Configuration conf = getConfiguration();
        String processorParameters = conf.get(JobConfigNames.CALVALUS_L2_PARAMETERS);

        ResourceEngine resourceEngine = new ResourceEngine();
        VelocityContext velocityContext = resourceEngine.getVelocityContext();
        final Properties processingParameters = PropertiesHandler.asProperties(processorParameters);
        for (int i = 0; i < getInputParameters().length; i += 2) {
            if (!"output".equals(getInputParameters()[i])) {
                processingParameters.put(getInputParameters()[i], getInputParameters()[i + 1]);
            }
        }
        for (String key : processingParameters.stringPropertyNames()) {
            velocityContext.put(key, processingParameters.get(key));
        }
        velocityContext.put("system", System.getProperties());
        velocityContext.put("configuration", conf);
        velocityContext.put("parameterText", processorParameters);
        velocityContext.put("parameters", processingParameters);

        velocityContext.put("inputPath", inputPath);
        velocityContext.put("outputPath", getOutputDirectoryPath());
        velocityContext.put("workOutputPath", getWorkOutputDirectoryPath());
        velocityContext.put("GlobalFunctions", new GlobalFunctions(getLogger()));

        String graphPathAsString = conf.get(ProcessorFactory.CALVALUS_L2_PROCESSOR_FILES);
        Path graphPath = new Path(graphPathAsString);
        InputStream inputStream = graphPath.getFileSystem(conf).open(graphPath);
        Reader inputReader = new BufferedReader(new InputStreamReader(inputStream));
        Resource processedGraph = resourceEngine.processResource(new ReaderResource(graphPathAsString, inputReader));
        String graphAsText = processedGraph.getContent();
        System.out.println("graphAsText = \n" + graphAsText);
        if (graphAsText.contains("CALVALUS_SKIP_PROCESSING yes")) {
            return null;
        }
        StringReader graphReader = new StringReader(graphAsText);
        try {
            return GraphIO.read(graphReader);
        } finally {
            inputReader.close();
            graphReader.close();
        }
    }

    public static class CalvalusGraphApplicationData implements XmlConvertible {

        @Parameter(itemAlias = "outputFile")
        String[] outputFiles;

        @Parameter(itemAlias = "outputNode", domConverter = OutputNodeDomConverter.class)
        OutputNodeRef[] outputNodes;

        @Parameter()
        String archiveFile;

        public static CalvalusGraphApplicationData fromXml(String xml) throws BindingException {
            return createConverter().convertXmlToObject(xml, new CalvalusGraphApplicationData());
        }

        public static CalvalusGraphApplicationData fromDom(XppDom calvalusAppDataDom) throws ValidationException, ConversionException {
            if (calvalusAppDataDom.getName().equals("applicationData")) {
                calvalusAppDataDom = calvalusAppDataDom.getChild("executeParameters");
            }
            ParameterDescriptorFactory parameterDescriptorFactory = new ParameterDescriptorFactory();
            CalvalusGraphApplicationData appData = new CalvalusGraphApplicationData();
            PropertyContainer propertySet = PropertyContainer.createObjectBacked(appData, parameterDescriptorFactory);
            propertySet.setDefaultValues();
            DefaultDomConverter domConverter = new DefaultDomConverter(appData.getClass(), parameterDescriptorFactory);
            domConverter.convertDomToValue(new XppDomElement(calvalusAppDataDom), propertySet);
            return appData;
        }

        @Override
        public String toXml() {
            try {
                return new ParameterBlockConverter().convertObjectToXml(this);
            } catch (ConversionException e) {
                throw new RuntimeException(e);
            }
        }

        private static ParameterBlockConverter createConverter() {
            return new ParameterBlockConverter(new ParameterDescriptorFactory(), "executeParameters");
        }
    }

    public static class OutputNodeRef {
        public final String nodeId;
        public final String parameter;

        public OutputNodeRef(String nodeId, String parameter) {
            this.nodeId = nodeId;
            this.parameter = parameter;
        }
    }

    public static class OutputNodeDomConverter implements DomConverter {
        @Override
        public Class<?> getValueType() {
            return OutputNodeRef[].class;
        }

        @Override
        public Object convertDomToValue(DomElement parentElement, Object value) throws ConversionException, ValidationException {
            DomElement[] outputNodes = parentElement.getChildren("outputNode");
            List<OutputNodeRef> outputNodeRefList = new ArrayList<>();
            for (DomElement outputNode : outputNodes) {
                String id = outputNode.getAttribute("id");
                String parameter = outputNode.getAttribute("parameter");
                if (id != null && parameter != null) {
                    outputNodeRefList.add(new OutputNodeRef(id, parameter));
                }
            }
            return outputNodeRefList.toArray(new OutputNodeRef[0]);
        }

        @Override
        public void convertValueToDom(Object value, DomElement parentElement) throws ConversionException {
            throw new IllegalStateException("Conversion to DOM no implemented");
        }
    }


    /**
     * This operator is only a container for the source product of the graph.
     */
    private static class SourceProductContainerOperator extends Operator {

        @Override
        public void initialize() throws OperatorException {
            // do nothing by intention
        }
    }

    // TODO mz/nf 2013-11-13 use some sort of plugin concept to add new functions to the velocity context
    public static class GlobalFunctions {

        private final Logger logger;

        public GlobalFunctions(Logger logger) {
            this.logger = logger;
        }

        public Path createPath(String pathString) {
            return new Path(pathString);
        }

        // MER_RR__1PRACR20030601_092632_000026422016_00480_06546_0000.N1
        public Path findCoveringN1(String master, String archiveRoot, FileSystem fileSystem) {
            Path result = null;
            try {
                final String masterStartString = master.substring(14, 29);
                final String masterDurationString = master.substring(30, 38);
                final long masterStart = N1_TIME_FORMAT.parse(masterStartString).getTime();
                final long masterStop = masterStart + (Long.parseLong("1" + masterDurationString) - 100000000) * 1000;
                logger.info("looking for slave of " + master + " in " + archiveRoot);
                final Calendar calendar = DateUtils.createCalendar();
                calendar.setTimeInMillis(masterStart);
                final String thisDayDir = YMD_DIR_FORMAT.format(calendar.getTime());
                calendar.add(Calendar.DAY_OF_MONTH, -1);
                final String previousDayDir = YMD_DIR_FORMAT.format(calendar.getTime());
                calendar.add(Calendar.DAY_OF_MONTH, 2);
                final String nextDayDir = YMD_DIR_FORMAT.format(calendar.getTime());
                final FileStatus[] slaveFiles = fileSystem.listStatus(new Path[]{
                        new Path(archiveRoot, previousDayDir),
                        new Path(archiveRoot, thisDayDir),
                        new Path(archiveRoot, nextDayDir)
                }, new PathFilter() {
                    @Override
                    public boolean accept(Path path) {
                        return path.getName().length() == 62 && path.getName().endsWith(".N1");
                    }
                });
                for (FileStatus slaveFile : slaveFiles) {
                    final String slave = slaveFile.getPath().getName();
                    final String slaveStartString = slave.substring(14, 29);
                    final String slaveDurationString = slave.substring(30, 38);
                    final long slaveStart = N1_TIME_FORMAT.parse(slaveStartString).getTime();
                    final long slaveStop = slaveStart + (Long.parseLong("1" + slaveDurationString) - 100000000) * 1000;
                    if (masterStart >= slaveStart && masterStop <= slaveStop) {
                        logger.info("covering slave  " + slave + " found");
                        return slaveFile.getPath();
                    } else if ((masterStart + masterStop) / 2 >= slaveStart && (masterStart + masterStop) / 2 <= slaveStop) {
                        result = slaveFile.getPath();
                    }
                }
            } catch (ParseException e) {
                throw new RuntimeException("failed to parse date in " + master, e);
            } catch (IOException e) {
                throw new RuntimeException("failed to read dirs below " + archiveRoot, e);
            }
            if (result != null) {
                logger.info("intersecting slave  " + result.getName() + " found");
            } else {
                logger.info("no slave found for " + master);
            }
            return result;
        }

        public Calendar getCalendar() {
            return DateUtils.createCalendar();
        }

        public String formatDate(String format, Date date) {
            return DateUtils.createDateFormat(format).format(date);
        }

        public Date parseDate(String format, String source) throws ParseException {
            return DateUtils.createDateFormat(format).parse(source);
        }

        public String xmlEncode(String s) {
            if (s == null) {
                return null;
            }
            StringBuilder sb = new StringBuilder();
            for (int i = 0; i < s.length(); i++) {
                char c = s.charAt(i);
                switch (c) {
                    case '<':
                        sb.append("&lt;");
                        break;
                    case '>':
                        sb.append("&gt;");
                        break;
                    case '&':
                        sb.append("&amp;");
                        break;
//                    case '\"':
//                        sb.append("&quot;");
//                        break;
//                    case '\'':
//                        sb.append("&apos;");
//                        break;
                    default:
                        if (c > 0x7e) {
                            sb.append("&#" + ((int) c) + ";");
                        } else
                            sb.append(c);
                }
            }
            return sb.toString();
        }

        public String convertProcessingGraphToXML(Product product) {
            final MetadataElement metadataRoot = product.getMetadataRoot();
            if (metadataRoot != null) {
                final MetadataElement processingGraph = metadataRoot.getElement("Processing_Graph");
                if (processingGraph != null) {
                    return convertMetadataToXML(processingGraph);
                }
            }
            return "";
        }

        public String convertMetadataToXML(MetadataElement metadataElement) {
            Element domElement = convertMetadataToDOM(metadataElement);
            final XMLOutputter outputter = new XMLOutputter(Format.getPrettyFormat());
            return outputter.outputString(domElement);
        }

        public Element convertMetadataToDOM(MetadataElement metadataElement) {
            final Element domElement = new Element(metadataElement.getName());
            for (MetadataAttribute attribute : metadataElement.getAttributes()) {
                domElement.addContent(new Element(attribute.getName()).setText(attribute.getData().getElemString()));
            }
            for (MetadataElement element : metadataElement.getElements()) {
                domElement.addContent(convertMetadataToDOM(element));
            }
            return domElement;
        }

        public String configToString(Configuration jobConfig) {
            StringBuilder accu = new StringBuilder();
            for (Map.Entry<String,String> entry : jobConfig) {
                accu.append(entry.getKey());
                accu.append("=");
                accu.append(xmlEncode(entry.getValue()).replaceAll("\n", " "));
                accu.append("\n");
            }
            return accu.toString();
        }
    }

    public static void main(String[] args) throws IOException {

        ResourceEngine resourceEngine = new ResourceEngine();
        VelocityContext velocityContext = resourceEngine.getVelocityContext();
        velocityContext.put("system", System.getProperties());
        Configuration conf = new Configuration();
        conf.set("fs.defaultFS", "hdfs://calvalus");
        conf.set("dfs.nameservices", "calvalus");
        conf.set("dfs.ha.namenodes.calvalus", "nn1,nn2");
        conf.set("dfs.namenode.rpc-address.calvalus.nn1", "master00:8020");
        conf.set("dfs.namenode.rpc-address.calvalus.nn2", "master01:8020");
        conf.set("dfs.client.failover.proxy.provider.calvalus", "org.apache.hadoop.hdfs.server.namenode.ha.ConfiguredFailoverProxyProvider");

        velocityContext.put("configuration", conf);

        Path inputPath = new Path("hdfs://calvalus/calvalus/eodata/MER_RR__1P/r03/2008/06/01/MER_RR__1PRACR20080601_122734_000026432069_00081_32700_0000.N1");
        velocityContext.put("inputPath", inputPath);
        velocityContext.put("GlobalFunctions", new GlobalFunctions(Logger.getLogger("test")));

        Reader inputReader = new FileReader(args[0]);
        Resource processedGraph = resourceEngine.processResource(new ReaderResource(args[0], inputReader));
        String graphAsText = processedGraph.getContent();
        System.out.println("graphAsText = \n" + graphAsText.trim());

    }

}
