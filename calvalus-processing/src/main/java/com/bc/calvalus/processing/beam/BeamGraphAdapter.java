package com.bc.calvalus.processing.beam;

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
import org.esa.beam.framework.gpf.graph.Graph;
import org.esa.beam.framework.gpf.graph.GraphContext;
import org.esa.beam.framework.gpf.graph.GraphException;
import org.esa.beam.framework.gpf.graph.GraphIO;

import java.io.IOException;
import java.io.InputStream;
import java.io.InputStreamReader;
import java.io.Reader;
import java.io.StringReader;

/**
 * A processor adapter that uses a BEAM GPF {@code Graph} to process input products.
 *
 * @author MarcoZ
 */
public class BeamGraphAdapter extends IdentityProcessorAdapter {

    private GraphContext graphContext;
    private Product[] targetProducts;

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
            graphContext = new GraphContext(graph);
        } catch (GraphException e) {
            throw new IOException("GraphException", e);
        }
        // TODO: add reading nodes
        targetProducts = graphContext.getOutputProducts();
        // TODO handle parameters

//        targetProduct = getProcessedProduct(subsetProduct, processorName, processorParameters);

        if (targetProducts.length == 0 ||
                targetProducts[0] == null ||
                targetProducts[0].getSceneRasterWidth() == 0 ||
                targetProducts[0].getSceneRasterHeight() == 0) {
            return 0;
        }
        getLogger().info(String.format("Processed product width = %d height = %d",
                                       targetProducts[0].getSceneRasterWidth(),
                                       targetProducts[0].getSceneRasterHeight()));
        if (hasInvalidStartAndStopTime(targetProducts[0])) {
            copySceneRasterStartAndStopTime(getInputProduct(), targetProducts[0], null);
        }
        return targetProducts.length;
    }

    @Override
    public Product openProcessedProduct() {
        return targetProducts[0];
    }

    @Override
    public void saveProcessedProducts(ProgressMonitor pm) throws IOException {
        saveTargetProduct(targetProducts[0], pm);
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
        Configuration conf = getConfiguration();
        String graphName = conf.get(JobConfigNames.CALVALUS_L2_OPERATOR);
        String processorParameters = conf.get(JobConfigNames.CALVALUS_L2_PARAMETERS);

        ResourceEngine resourceEngine = new ResourceEngine();
        VelocityContext velocityContext = resourceEngine.getVelocityContext();
        velocityContext.put("system", System.getProperties());
        velocityContext.put("configuration", conf);
        velocityContext.put("parameterText", processorParameters);
        velocityContext.put("parameters", PropertiesHandler.asProperties(processorParameters));

        Path inputPath = getInputPath();
        Path outputPath = FileOutputFormat.getOutputPath(getMapContext());
        velocityContext.put("inputPath", inputPath);
        velocityContext.put("outputPath", outputPath);

        String graphPathAsString = conf.get(ProcessorFactory.CALVALUS_L2_PROCESSOR_FILES);
        FileSystem fs = FileSystem.get(conf);
        InputStream inputStream = fs.open(new Path(graphPathAsString));
        Reader inputReader = new InputStreamReader(inputStream);
        Resource processedGraph = resourceEngine.processResource(new ReaderResource(graphPathAsString, inputReader));
        StringReader graphReader = new StringReader(processedGraph.getContent());

        try {
            return GraphIO.read(graphReader);
        } finally {
            inputReader.close();
            graphReader.close();
        }
    }

}
