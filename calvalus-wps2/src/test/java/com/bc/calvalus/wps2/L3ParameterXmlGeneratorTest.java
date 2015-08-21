package com.bc.calvalus.wps2;

import com.bc.calvalus.wps2.jaxb.AggregatorConfig;
import com.bc.calvalus.wps2.jaxb.Aggregators;
import com.bc.calvalus.wps2.jaxb.CellProcessorConfig;
import com.bc.calvalus.wps2.jaxb.CompositingType;
import com.bc.calvalus.wps2.jaxb.L3Parameters;
import com.bc.calvalus.wps2.jaxb.TimeFilterMethod;
import com.bc.calvalus.wps2.jaxb.VariableConfig;
import com.bc.calvalus.wps2.jaxb.Variables;
import org.junit.*;

import java.util.ArrayList;
import java.util.List;

/**
 * Created by hans on 19/08/2015.
 */
public class L3ParameterXmlGeneratorTest {

    @Test
    public void testCreateXml() throws Exception {
        L3Parameters l3Parameters = new L3Parameters();
        l3Parameters.setNumRows(1);
        l3Parameters.setPlanetaryGrid("planetaryGrid");
        l3Parameters.setMaskExpr("maskExpr");
        l3Parameters.setCompositingType(CompositingType.BINNING);
        l3Parameters.setTimeFilterMethod(TimeFilterMethod.SPATIOTEMPORAL_DATA_DAY);

        Variables variables = new Variables();
        VariableConfig variableConfig = new VariableConfig();
        variableConfig.setName("var1");
        variableConfig.setExpr("expr1");
        variables.getVariable().add(variableConfig);
        l3Parameters.setVariables(variables);

        Aggregators aggregators = new Aggregators();
        AggregatorConfig aggregatorConfig1 = new AggregatorConfig();
        aggregatorConfig1.setType("AVG");
        aggregatorConfig1.setVarName("tsm");
        AggregatorConfig aggregatorConfig2 = new AggregatorConfig();
        aggregatorConfig2.setType("MIN_MAX");
        aggregatorConfig2.setVarName("chl_conc");
        aggregators.getAggregator().add(aggregatorConfig1);
        aggregators.getAggregator().add(aggregatorConfig2);
        l3Parameters.setAggregators(aggregators);

        CellProcessorConfig postProcessor = new CellProcessorConfig();
        postProcessor.setVarName("postProcessor");
        postProcessor.setType("type");
        l3Parameters.setPostProcessorConfig(postProcessor);

        L3ParameterXmlGenerator xmlGenerator = new L3ParameterXmlGenerator(l3Parameters);
        String xml = xmlGenerator.createXml();
        System.out.println("xml = " + xml);
    }
}