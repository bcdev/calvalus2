/*
 * Copyright (C) 2014 Brockmann Consult GmbH (info@brockmann-consult.de)
 *
 * This program is free software; you can redistribute it and/or modify it
 * under the terms of the GNU General Public License as published by the Free
 * Software Foundation; either version 3 of the License, or (at your option)
 * any later version.
 * This program is distributed in the hope that it will be useful, but WITHOUT
 * ANY WARRANTY; without even the implied warranty of MERCHANTABILITY or
 * FITNESS FOR A PARTICULAR PURPOSE. See the GNU General Public License for
 * more details.
 *
 * You should have received a copy of the GNU General Public License along
 * with this program; if not, see http://www.gnu.org/licenses/
 */

package com.bc.calvalus.processing.fex;

import com.bc.calvalus.processing.beam.GpfUtils;
import com.bc.calvalus.processing.hadoop.ProductSplit;
import com.bc.calvalus.processing.hadoop.ProductSplitProgressMonitor;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.TaskInputOutputContext;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.esa.beam.framework.gpf.GPF;
import org.esa.beam.framework.gpf.Operator;
import org.esa.beam.framework.gpf.OperatorSpi;
import org.esa.beam.framework.gpf.OperatorSpiRegistry;
import org.esa.beam.framework.gpf.graph.Graph;
import org.esa.beam.framework.gpf.graph.GraphContext;
import org.esa.beam.framework.gpf.graph.GraphException;
import org.esa.beam.framework.gpf.graph.GraphIO;
import org.esa.beam.framework.gpf.graph.GraphProcessor;
import org.esa.beam.framework.gpf.graph.Node;
import org.esa.beam.framework.gpf.graph.NodeContext;
import org.esa.pfa.fe.PFAApplicationDescriptor;
import org.esa.pfa.fe.PFAApplicationRegistry;
import org.esa.pfa.fe.op.FeatureWriterResult;
import org.esa.pfa.fe.op.PatchResult;

import java.io.File;
import java.io.IOException;
import java.io.InputStreamReader;
import java.io.OutputStream;
import java.io.Reader;
import java.nio.file.Files;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

/**
 * Processes a fez and some features from a EO data product.
 */
public class FexMapper extends Mapper<NullWritable, NullWritable, Text, Text> {

    @Override
    public void run(Context context) throws IOException, InterruptedException {
        Configuration conf = context.getConfiguration();
        GpfUtils.init(conf);
        ProductSplit productSplit = (ProductSplit) context.getInputSplit();
        Path productPath = productSplit.getPath();


        File targetDir = new File(".", "targetDir");
        if (!targetDir.mkdir()) {
            throw new IOException("Failed to create 'targetDir'");
        }
        Map<String, String> variables = new HashMap<>();
        variables.put("targetDir", targetDir.getAbsolutePath());
        variables.put("sourcePath", productPath.toString());

        PFAApplicationDescriptor applicationDescriptor = getApplicationDescriptor(conf);
        try {
            exchangeReadOpsInRegistry(conf);
            Graph graph = getGraph(applicationDescriptor, variables);
            processGraph(graph, applicationDescriptor, productPath.getName(), context);
            copyFezToHDFS(targetDir, context);
        } catch (GraphException e) {
            e.printStackTrace();
            throw new IOException("Failed to process product " + productPath, e);
        }
    }

    static PFAApplicationDescriptor getApplicationDescriptor(Configuration conf) {
        String applicationName = conf.get("calvalus.fex.applicationName");
        PFAApplicationRegistry registry = PFAApplicationRegistry.getInstance();
        PFAApplicationDescriptor descriptor = registry.getDescriptor(applicationName);
        if (descriptor == null) {
            String msg = String.format("No descriptor with name '%s' available.", applicationName);
            throw new IllegalArgumentException(msg);
        }
        return descriptor;
    }

    private void copyFezToHDFS(File targetDir, Context context) throws IOException, InterruptedException {
        for (File fezFile : targetDir.listFiles((file) -> file.getName().endsWith(".fex.zip"))) {
            try (OutputStream os = createOutputStream(context, fezFile.getName())) {
                Files.copy(fezFile.toPath(), os);
            }
        }
    }

    private static void exchangeReadOpsInRegistry(Configuration configuration) {
        OperatorSpiRegistry operatorRegistry = GPF.getDefaultInstance().getOperatorSpiRegistry();
        OperatorSpi originalReadSpi = operatorRegistry.getOperatorSpi("Read");
        if (originalReadSpi != null) {
            // maybe not needed
            operatorRegistry.removeOperatorSpi(originalReadSpi);
        }
        CalvalusReadOp.Spi calvalusReadSpi = new CalvalusReadOp.Spi(configuration);
        operatorRegistry.addOperatorSpi(calvalusReadSpi);
    }

    private static void processGraph(Graph graph,
                                     PFAApplicationDescriptor applicationDescriptore,
                                     String productName,
                                     Context context) throws IOException, GraphException, InterruptedException {


        GraphContext graphContext = new GraphContext(graph);
        ProductSplitProgressMonitor pm = new ProductSplitProgressMonitor(context);
        new GraphProcessor().executeGraph(graphContext, pm);
        Node fexOpNode = graph.getNode(applicationDescriptore.getFeatureWriterNodeName());
        NodeContext fexOpNodeCtx = graphContext.getNodeContext(fexOpNode);
        Operator fexOp = fexOpNodeCtx.getOperator();

        Object targetProperty = fexOp.getTargetProperty(applicationDescriptore.getFeatureWriterPropertyName());
        if (targetProperty instanceof FeatureWriterResult) {
            FeatureWriterResult featureWriterResult = (FeatureWriterResult) targetProperty;
            List<PatchResult> patchResults = featureWriterResult.getPatchResults();

            Text key = new Text();
            Text value = new Text();
            for (PatchResult patchResult : patchResults) {
                key.set(productName + ":" + patchResult.getPatchX() + ":" + patchResult.getPatchY());
                value.set(patchResult.getFeaturesText());
                context.write(key, value);
            }

        }
        graphContext.dispose();
    }

    private static Graph getGraph(PFAApplicationDescriptor applicationDescriptor, Map<String, String> variables) throws IOException, GraphException {
        try (Reader graphReader = new InputStreamReader(applicationDescriptor.getGraphFileAsStream())) {
            return GraphIO.read(graphReader, variables);
        }
    }

    public static OutputStream createOutputStream(TaskInputOutputContext<?, ?, ?, ?> context, String filename) throws
            IOException, InterruptedException {

        Path workPath = new Path(FileOutputFormat.getWorkOutputPath(context), filename);
        FileSystem fileSystem = FileSystem.get(context.getConfiguration());
        return fileSystem.create(workPath, (short) 1);
    }

}
