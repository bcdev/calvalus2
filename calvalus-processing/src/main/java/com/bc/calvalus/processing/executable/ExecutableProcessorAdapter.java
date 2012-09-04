/*
 * Copyright (C) 2012 Brockmann Consult GmbH (info@brockmann-consult.de)
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

package com.bc.calvalus.processing.executable;

import com.bc.calvalus.processing.JobConfigNames;
import com.bc.calvalus.processing.ProcessorAdapter;
import com.bc.calvalus.processing.l2.ProductFormatter;
import com.bc.ceres.core.ProcessObserver;
import com.bc.ceres.core.ProgressMonitor;
import com.bc.ceres.resource.ReaderResource;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.mapreduce.MapContext;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.velocity.VelocityContext;
import org.esa.beam.framework.dataio.ProductIO;
import org.esa.beam.framework.datamodel.Product;
import org.w3c.dom.Document;
import org.w3c.dom.Element;
import org.w3c.dom.Node;
import org.w3c.dom.NodeList;
import org.xml.sax.InputSource;
import org.xml.sax.SAXException;

import javax.xml.parsers.DocumentBuilder;
import javax.xml.parsers.DocumentBuilderFactory;
import javax.xml.parsers.ParserConfigurationException;
import java.awt.Rectangle;
import java.io.BufferedInputStream;
import java.io.File;
import java.io.FileInputStream;
import java.io.IOException;
import java.io.InputStream;
import java.io.InputStreamReader;
import java.io.OutputStream;
import java.io.Reader;
import java.io.StringReader;
import java.util.Collection;
import java.util.Properties;

/**
 * A processor adapter that uses an executable to process an input product.
 *
 * @author MarcoZ
 */
public class ExecutableProcessorAdapter extends ProcessorAdapter {

    private String[] outputFilesNames;
    private final File cwd;

    public ExecutableProcessorAdapter(MapContext mapContext) {
        super(mapContext);
        this.cwd = new File(".");
    }

    @Override
    public int processSourceProduct(ProgressMonitor pm) throws IOException {
        pm.setSubTaskName("Exec Level 2");
        Configuration conf = getConfiguration();
        String bundle = conf.get(JobConfigNames.CALVALUS_L2_BUNDLE);
        String executable = conf.get(JobConfigNames.CALVALUS_L2_OPERATOR);
        String processorParameters = conf.get(JobConfigNames.CALVALUS_L2_PARAMETERS);

        Rectangle inputRectangle = getInputRectangle();
        File inputFile = copyProductToLocal(getInputPath());
        File outputDir = new File(cwd, "output");
        outputDir.mkdirs();

        ScriptGenerator scriptGenerator = new ScriptGenerator(executable);
        VelocityContext velocityContext = scriptGenerator.getVelocityContext();
        velocityContext.put("system", System.getProperties());
        velocityContext.put("configuration", conf);
        velocityContext.put("inputFile", inputFile);
        velocityContext.put("inputRectangle", inputRectangle);
        velocityContext.put("outputDir", outputDir);
        velocityContext.put("outputPath", FileOutputFormat.getOutputPath(getMapContext()).toString());
        velocityContext.put("parameters", asProperties(processorParameters));

        addScriptResources(conf, scriptGenerator);
        scriptGenerator.writeScriptFiles(cwd);

        Process process = Runtime.getRuntime().exec(scriptGenerator.getCommandLine());
        String processLogName = bundle + "-" + executable;
        KeywordHandler keywordHandler = new KeywordHandler(processLogName, getMapContext());

        new ProcessObserver(process).
                setName(processLogName).
                setProgressMonitor(pm).
                setHandler(keywordHandler).
                start();

        outputFilesNames = keywordHandler.getOutputFiles();
        return outputFilesNames.length;
    }

    /**
     * Maybe this could be integrated into {@link com.bc.ceres.resource.Resource}
     */
    static Properties asProperties(String processorParameters) throws IOException {
        Properties properties = new Properties();
        Reader reader = new StringReader(processorParameters);
        try {
            if (isXml(processorParameters)) {
                DocumentBuilder documentBuilder = DocumentBuilderFactory.newInstance().newDocumentBuilder();
                Document document = documentBuilder.parse(new InputSource(reader));
                Element documentElement = document.getDocumentElement();
                documentElement.normalize();
                NodeList nodeList = documentElement.getChildNodes();
                if (nodeList != null && nodeList.getLength() > 0) {
                    for (int i = 0; i < nodeList.getLength(); i++) {
                        Node node = nodeList.item(i); //Take the node from the list
                        NodeList valueNode = node.getChildNodes(); // get the children of the node
                        if (valueNode.item(0) != null) {
                            String nodeName = node.getNodeName();
                            String nodeValue = valueNode.item(0).getNodeValue();
                            if (nodeName != null && nodeValue != null) {
                                properties.put(nodeName.trim(), nodeValue.trim());
                            }
                        }
                    }
                }
            } else {
                properties.load(reader);
            }
        } catch (ParserConfigurationException e) {
            throw new IOException(e);
        } catch (SAXException e) {
            throw new IOException(e);
        } finally {
            reader.close();
        }
        return properties;
    }


    static boolean isXml(String textContent) {
        String t = textContent.trim();
        return t.startsWith("<?xml ") || t.startsWith("<?XML ") || (t.startsWith("<") && t.endsWith(">"));
    }

    private void addScriptResources(Configuration conf, ScriptGenerator scriptGenerator) throws IOException {
        Collection<String> scriptFiles = conf.getStringCollection("calvalus.l2.scriptFiles");
        FileSystem fs = FileSystem.get(conf);
        for (String scriptFile : scriptFiles) {
            Path scriptFilePath = new Path(scriptFile);
            InputStream inputStream = fs.open(scriptFilePath);
            Reader reader = new InputStreamReader(inputStream);
            scriptGenerator.addResource(new ReaderResource(scriptFile, reader));
        }
    }

    @Override
    public Product openProcessedProduct() throws IOException {
        if (outputFilesNames != null && outputFilesNames.length > 0) {
            return ProductIO.readProduct(new File(cwd, outputFilesNames[0]));
        }
        return null;
    }

    @Override
    public void saveProcessedProducts(ProgressMonitor pm) throws Exception {
        if (outputFilesNames != null && outputFilesNames.length > 0) {
            MapContext mapContext = getMapContext();
            for (String outputFileName : outputFilesNames) {
                InputStream inputStream = new BufferedInputStream(new FileInputStream(new File(cwd, outputFileName)));
                OutputStream outputStream = ProductFormatter.createOutputStream(mapContext, outputFileName);
                ProductFormatter.copyAndClose(inputStream, outputStream, mapContext);
            }
        }
    }

}
