package com.bc.calvalus.processing.beam;

import com.bc.calvalus.commons.CalvalusLogger;
import com.bc.calvalus.processing.JobConfigNames;
import com.bc.calvalus.processing.ProcessorFactory;
import com.bc.calvalus.processing.executable.PropertiesHandler;
import com.bc.ceres.core.ProgressMonitor;
import com.bc.ceres.resource.ReaderResource;
import com.bc.ceres.resource.Resource;
import com.bc.ceres.resource.ResourceEngine;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileStatus;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.fs.PathFilter;
import org.apache.hadoop.mapreduce.MapContext;
import org.apache.velocity.VelocityContext;
import org.esa.snap.core.dataio.ProductIO;
import org.esa.snap.core.datamodel.Product;
import org.esa.snap.core.datamodel.ProductData;
import org.esa.snap.core.gpf.Operator;
import org.esa.snap.core.gpf.OperatorException;
import org.esa.snap.core.gpf.graph.*;

import java.io.*;
import java.text.ParseException;
import java.text.SimpleDateFormat;
import java.util.*;
import java.util.logging.Logger;

/**
 * A processor adapter that uses a SNAP GPF {@code Graph} to process input products.
 *
 * @author MarcoZ
 */
public class SnapGraphAdapter extends SubsetProcessorAdapter {

    private static final SimpleDateFormat N1_TIME_FORMAT = new SimpleDateFormat("yyyyMMdd_HHmmss");
    private static final SimpleDateFormat YMD_DIR_FORMAT = new SimpleDateFormat("yyyy/MM/dd");

    static {
        N1_TIME_FORMAT.setTimeZone(TimeZone.getTimeZone("UTC"));
        YMD_DIR_FORMAT.setTimeZone(TimeZone.getTimeZone("UTC"));
    }

    private GraphContext graphContext;
    private Product targetProduct;

    public SnapGraphAdapter(MapContext mapContext) {
        super(mapContext);

        if (getConfiguration().get(JobConfigNames.CALVALUS_REGION_GEOMETRY) != null) {
            if (getConfiguration().get("calvalus.system.snap.dataio.reader.tileHeight") == null) {
                System.setProperty("snap.dataio.reader.tileHeight", "64");
                getLogger().info("Setting tileHeight to 64 for graph subsetting");
            }
            if ((getConfiguration().get("calvalus.system.snap.dataio.reader.tileWidth") == null
                 || "*".equals(getConfiguration().get("calvalus.system.snap.dataio.reader.tileWidth")))
                && ! getConfiguration().getBoolean(JobConfigNames.CALVALUS_INPUT_FULL_SWATH, false)) {
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
    public int processSourceProduct(ProgressMonitor pm) throws IOException {
        pm.setSubTaskName("SNAP Level 2");

        try {
            Graph graph = createGraph();
            if (graph == null) {
                getLogger().info("Skip processing");
                return 0;
            }
            Header header = graph.getHeader();
            List<HeaderSource> sources = header.getSources();
            Path inputPath = getInputPath();
            Operator sourceProducts = new SourceProductContainerOperator();
            for (HeaderSource headerSource : sources) {
                String sourceId = headerSource.getName();
                Path sourcePath = new Path(headerSource.getLocation());

                File inputFile = getInputFile();
                String inputFormat = null;
                if (headerSource.getDescription() != null && headerSource.getDescription().startsWith("format:")) {
                    inputFormat = headerSource.getDescription().substring("format:".length());
                }
                Product sourceProduct;
                if (sourcePath.equals(inputPath) && inputFile != null) {
                    // if inputFile is set and path is the inputPath us the (local)file instead
                    System.out.println("sourcePath equals inputPath, inputFile set, use it=" + inputFile);
                    sourceProduct = ProductIO.readProduct(inputFile, inputFormat);
                } else {
                    Path qualifiedSourcePath = sourcePath.getFileSystem(getConfiguration()).makeQualified(sourcePath);
                    Path qualifiedInputPath = inputPath.getFileSystem(getConfiguration()).makeQualified(inputPath);
                    if (qualifiedInputPath.equals(qualifiedSourcePath)) {
                        sourceProduct = getInputProduct();
                    } else {
                        sourceProduct = CalvalusProductIO.readProduct(sourcePath, getConfiguration(), inputFormat);
                    }
                }
                sourceProducts.setSourceProduct(sourceId, sourceProduct);
            }
            graphContext = new GraphContext(graph, sourceProducts);
            HeaderTarget target = header.getTarget();
            if (target == null || target.getNodeId() == null) {
                throw new IllegalArgumentException("No 'target' specified in graph header.");
            }
            Node targetNode = graph.getNode(target.getNodeId());
            if (targetNode == null) {
                throw new IllegalArgumentException("Specified targetNode '" + target.getNodeId() + "' does not exist in graph.");
            }
            NodeContext targetNodeContext = graphContext.getNodeContext(targetNode);
            targetProduct = createSubset(targetNodeContext.getTargetProduct());
        } catch (GraphException e) {
            throw new IOException("Error executing Graph: " + e.getMessage(), e);
        } finally {
            pm.done();
        }
        if (targetProduct.getSceneRasterWidth() == 0 || targetProduct.getSceneRasterHeight() == 0) {
            getLogger().info("Skip processing");
            return 0;
        }
        getLogger().info(String.format("Processed product width = %d height = %d",
                                       targetProduct.getSceneRasterWidth(),
                                       targetProduct.getSceneRasterHeight()));
        if (hasInvalidStartAndStopTime(targetProduct)) {
            copySceneRasterStartAndStopTime(getInputProduct(), targetProduct, null);
        }
        return 1;
    }

    @Override
    public Product openProcessedProduct() {
        return targetProduct;
    }

    @Override
    public void saveProcessedProducts(ProgressMonitor pm) throws IOException {
        saveTargetProduct(targetProduct, pm);
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
        for (int i=0; i<getInputParameters().length; i+=2) {
            if (! "output".equals(getInputParameters()[i])) {
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
        velocityContext.put("GlobalFunctions", new GlobalsFunctions(getLogger()));

        String graphPathAsString = conf.get(ProcessorFactory.CALVALUS_L2_PROCESSOR_FILES);
        Path graphPath = new Path(graphPathAsString);
        InputStream inputStream = graphPath.getFileSystem(conf).open(graphPath);
        Reader inputReader = new InputStreamReader(inputStream);
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
    public static class GlobalsFunctions {

        private final Logger logger;

        public GlobalsFunctions(Logger logger) {
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
                final GregorianCalendar calendar = new GregorianCalendar(TimeZone.getTimeZone("UTC"));
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
            return ProductData.UTC.createCalendar();
        }

        public String formatDate(String format, Date date) {
            return ProductData.UTC.createDateFormat(format).format(date);
        }

        public Date parseDate(String format, String source) throws ParseException {
            return ProductData.UTC.createDateFormat(format).parse(source);
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
        velocityContext.put("GlobalFunctions", new GlobalsFunctions(Logger.getLogger("test")));

        Reader inputReader = new FileReader(args[0]);
        Resource processedGraph = resourceEngine.processResource(new ReaderResource(args[0], inputReader));
        String graphAsText = processedGraph.getContent();
        System.out.println("graphAsText = \n" + graphAsText.trim());

    }

}
