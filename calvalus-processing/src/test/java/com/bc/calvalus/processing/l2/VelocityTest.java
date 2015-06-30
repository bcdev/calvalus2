package com.bc.calvalus.processing.l2;

import org.apache.velocity.VelocityContext;
import org.apache.velocity.app.VelocityEngine;
import org.apache.velocity.runtime.RuntimeConstants;
import org.apache.velocity.runtime.log.NullLogChute;
import org.esa.snap.framework.datamodel.MetadataAttribute;
import org.esa.snap.framework.datamodel.MetadataElement;
import org.esa.snap.framework.datamodel.Product;
import org.esa.snap.framework.datamodel.ProductData;
import org.junit.Test;

import java.io.StringWriter;

import static org.junit.Assert.*;


/**
 * @author Marco Peters
 */
public class VelocityTest {

    @Test
    public void testTheStuff() throws Exception {


        // This snippet is from the Freshmon template
        String velocityTemplate =
                "#set( $processingGraph = $targetProduct.getMetadataRoot().getElement(\"Processing_Graph\"))\n" +
                "<metadata>\n" +
                "    <steps>\n" +
                "#foreach($element in $processingGraph.getElements())\n" +
                "        <step>\n" +
                "            <processor>\n" +
                "                <name>$element.getAttributeString(\"operator\")</name>\n" +
                "                <version>$element.getAttributeString(\"version\")</version>\n" +
                "            </processor>\n" +
                "#if ($element.getElement(\"parameters\").getAttributeNames().size() != 0)\n" +
                "            <parameters>\n" +
                "#foreach($paramName in $element.getElement(\"parameters\").getAttributeNames())\n" +
                "                <$paramName>$element.getElement(\"parameters\").getAttributeString($paramName)</$paramName>\n" +
                "#end\n" +
                "            </parameters>\n" +
                "#end\n" +
                "        </step>\n" +
                "#end\n" +
                "    </steps>\n" +
                "\n" +
                "</metadata>\n";

        Product targetProduct = new Product("blah", "BLAHAAA", 10, 10);
        MetadataElement processingGraph = new MetadataElement("Processing_Graph");
        processingGraph.addElement(createProcessingStep("node_1", "Subset", "1.3"));
        processingGraph.addElement(createProcessingStep("node_2", "Freshmon", "1.5"));
        processingGraph.addElement(createProcessingStep("node_3", "reproject", "2.5-SNAPSHOT"));
        targetProduct.getMetadataRoot().addElement(processingGraph);

        VelocityContext vc = new VelocityContext();
        vc.put("targetProduct", targetProduct);
        VelocityEngine velocityEngine = new VelocityEngine();
        velocityEngine.setProperty(RuntimeConstants.RUNTIME_LOG_LOGSYSTEM_CLASS, NullLogChute.class);
        velocityEngine.init();

        StringWriter writer = new StringWriter();
        velocityEngine.evaluate(vc, writer, "velocityTemplate", velocityTemplate);

        String expected = "<metadata>\n" +
                          "    <steps>\n" +
                          "        <step>\n" +
                          "            <processor>\n" +
                          "                <name>Subset</name>\n" +
                          "                <version>1.3</version>\n" +
                          "            </processor>\n" +
                          "            <parameters>\n" +
                          "                <p0>egal</p0>\n" +
                          "                <p1>egal</p1>\n" +
                          "                <p2>egal</p2>\n" +
                          "            </parameters>\n" +
                          "        </step>\n" +
                          "        <step>\n" +
                          "            <processor>\n" +
                          "                <name>Freshmon</name>\n" +
                          "                <version>1.5</version>\n" +
                          "            </processor>\n" +
                          "            <parameters>\n" +
                          "                <p0>egal</p0>\n" +
                          "                <p1>egal</p1>\n" +
                          "                <p2>egal</p2>\n" +
                          "            </parameters>\n" +
                          "        </step>\n" +
                          "        <step>\n" +
                          "            <processor>\n" +
                          "                <name>reproject</name>\n" +
                          "                <version>2.5-SNAPSHOT</version>\n" +
                          "            </processor>\n" +
                          "            <parameters>\n" +
                          "                <p0>egal</p0>\n" +
                          "                <p1>egal</p1>\n" +
                          "                <p2>egal</p2>\n" +
                          "            </parameters>\n" +
                          "        </step>\n" +
                          "    </steps>\n" +
                          "\n" +
                          "</metadata>\n";
        assertEquals(expected, writer.toString());
    }

    private MetadataElement createProcessingStep(String elemName, String opName, String opVersion) {
        MetadataElement element = new MetadataElement(elemName);
        element.addAttribute(new MetadataAttribute("operator", ProductData.createInstance(opName), true));
        element.addAttribute(new MetadataAttribute("version", ProductData.createInstance(opVersion), true));
        MetadataElement parameters = new MetadataElement("parameters");
        for (int i = 0; i < 3; i++) {
            parameters.addAttribute(new MetadataAttribute("p" + i, ProductData.createInstance("egal"), true));
        }
        element.addElement(parameters);
        return element;
    }
}