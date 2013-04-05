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
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.mapreduce.MapContext;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.velocity.VelocityContext;
import org.esa.beam.framework.datamodel.Product;
import org.esa.beam.framework.gpf.Operator;
import org.esa.beam.framework.gpf.OperatorException;
import org.esa.beam.framework.gpf.graph.Graph;
import org.esa.beam.framework.gpf.graph.GraphContext;
import org.esa.beam.framework.gpf.graph.GraphException;
import org.esa.beam.framework.gpf.graph.GraphIO;
import org.esa.beam.framework.gpf.graph.Header;
import org.esa.beam.framework.gpf.graph.HeaderSource;
import org.esa.beam.framework.gpf.graph.HeaderTarget;
import org.esa.beam.framework.gpf.graph.Node;
import org.esa.beam.framework.gpf.graph.NodeContext;

import java.io.IOException;
import java.io.InputStream;
import java.io.InputStreamReader;
import java.io.Reader;
import java.io.StringReader;
import java.util.List;

/**
 * A processor adapter that uses a BEAM GPF {@code Graph} to process input products.
 *
 * @author MarcoZ
 */
public class BeamGraphAdapter extends IdentityProcessorAdapter {

    private GraphContext graphContext;
    private Product targetProduct;

    public BeamGraphAdapter(MapContext mapContext) {
        super(mapContext);
    }

    @Override
    public boolean supportsPullProcessing() {
        return true;
    }

    @Override
    public int processSourceProduct(ProgressMonitor pm) throws IOException {
        pm.setSubTaskName("BEAM Level 2");

        try {
            Graph graph = createGraph();
            Header header = graph.getHeader();
            List<HeaderSource> sources = header.getSources();
            Operator sourceProducts = new SourceProductContainerOperator();
            for (HeaderSource headerSource : sources) {
                String sourceId = headerSource.getName();
                String sourceFilePath = headerSource.getLocation();

                Product product = readProduct(new Path(sourceFilePath), null);
                sourceProducts.setSourceProduct(sourceId, product);
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
            targetProduct = targetNodeContext.getTargetProduct();
        } catch (GraphException e) {
            throw new IOException("GraphException", e);
        }
        if (targetProduct.getSceneRasterWidth() == 0 || targetProduct.getSceneRasterHeight() == 0) {
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
    }

    public Graph createGraph() throws GraphException, IOException {
        Path inputPath = getInputPath();
        CalvalusLogger.getLogger().info("Creating graph for input: " + inputPath);
        Configuration conf = getConfiguration();
        String processorParameters = conf.get(JobConfigNames.CALVALUS_L2_PARAMETERS);

        ResourceEngine resourceEngine = new ResourceEngine();
        VelocityContext velocityContext = resourceEngine.getVelocityContext();
        velocityContext.put("system", System.getProperties());
        velocityContext.put("configuration", conf);
        velocityContext.put("parameterText", processorParameters);
        velocityContext.put("parameters", PropertiesHandler.asProperties(processorParameters));

        Path outputPath = FileOutputFormat.getOutputPath(getMapContext());
        velocityContext.put("inputPath", inputPath);
        velocityContext.put("outputPath", outputPath);
        velocityContext.put("GlobalFunctions", new GlobalsFunctions());

        String graphPathAsString = conf.get(ProcessorFactory.CALVALUS_L2_PROCESSOR_FILES);
        FileSystem fs = FileSystem.get(conf);
        InputStream inputStream = fs.open(new Path(graphPathAsString));
        Reader inputReader = new InputStreamReader(inputStream);
        Resource processedGraph = resourceEngine.processResource(new ReaderResource(graphPathAsString, inputReader));
        String graphAsText = processedGraph.getContent();
        System.out.println("graphAsText = \n" + graphAsText);
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

    public class GlobalsFunctions {
        public Path createPath(String pathString) {
            return new Path(pathString);
        }
    }
}
