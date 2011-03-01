package com.bc.calvalus.production.hadoop;

import com.bc.calvalus.processing.beam.BeamL3Config;
import org.apache.velocity.Template;
import org.apache.velocity.VelocityContext;
import org.apache.velocity.app.VelocityEngine;
import org.junit.Test;

import java.io.StringWriter;
import java.util.Properties;

import static org.junit.Assert.*;

public class HadoopProductionServiceTest {

    @Test
    public void testVelocityL3Template() throws Exception {

        Properties properties = new Properties();
        properties.setProperty("resource.loader", "class");
        properties.setProperty("class.resource.loader.class", "org.apache.velocity.runtime.resource.loader.ClasspathResourceLoader");

        VelocityEngine ve = new VelocityEngine();
        ve.init(properties);

        VelocityContext context = new VelocityContext();
        context.put("productionId", "12345");
        context.put("processorPackage", "beam-lkn");
        context.put("processorVersion", "1.0-SNAPSHOT");
        context.put("outputDir", "output-4");
        context.put("inputFiles", new String[]{"A.N1", "B.N1", "C.N1"});
        context.put("l2OperatorName", "Case2R");
        context.put("l2OperatorParameters", "a=6");
        context.put("bbox", "-60.0, 13.4, -20.0, 23.4");
        context.put("superSampling", "1");
        context.put("numRows", "1024");
        context.put("maskExpr", "!l2_flags.INVALID && l2_flags.WATER");
        context.put("variables", new BeamL3Config.VariableConfiguration[]{
                new BeamL3Config.VariableConfiguration("ndvi", "(reflec_10 - reflec_6) / (reflec_10 + reflec_6)")
        });
        context.put("aggregators", new BeamL3Config.AggregatorConfiguration[]{
                new BeamL3Config.AggregatorConfiguration("AVG", "chl", 0.5),
                new BeamL3Config.AggregatorConfiguration("AVG", "tsm", 0.0),
        });


        Template temp = ve.getTemplate("com/bc/calvalus/portal/server/level3-wps-request.xml.vm");
        assertNotNull(temp);
        StringWriter writer = new StringWriter();
        temp.merge(context, writer);
        String wpsXml = writer.toString();
        assertNotNull(wpsXml);


        System.out.println(wpsXml);
    }


    @Test
    public void testL3NumRows() {
        assertEquals(2160, HadoopProductionService.getNumRows(9.28));
        assertEquals(2160 * 2, HadoopProductionService.getNumRows(9.28 / 2));
        assertEquals(2160 / 2, HadoopProductionService.getNumRows(9.28 * 2));
    }

}
