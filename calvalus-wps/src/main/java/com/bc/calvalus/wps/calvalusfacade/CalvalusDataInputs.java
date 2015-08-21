package com.bc.calvalus.wps.calvalusfacade;

import com.bc.calvalus.wps.utility.XmlProcessor;
import org.deegree.services.wps.ProcessletInputs;
import org.deegree.services.wps.input.ComplexInput;
import org.deegree.services.wps.input.LiteralInput;
import org.deegree.services.wps.input.ProcessletInput;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import javax.xml.stream.XMLStreamException;
import javax.xml.stream.XMLStreamReader;
import java.io.IOException;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

/**
 * Created by hans on 23.07.2015.
 */
public class CalvalusDataInputs {

    private static final Logger LOG = LoggerFactory.getLogger(CalvalusDataInputs.class);

    private final Map<String, String> inputMap;

    /**
     * Constructor.
     *
     * @param processletInputs WPS Inputs.
     */
    public CalvalusDataInputs(ProcessletInputs processletInputs) {
        this.inputMap = new HashMap<>();
        processInputData(processletInputs);
    }

    private void processInputData(ProcessletInputs processletInputs) {
        List<ProcessletInput> processletInputList = processletInputs.getParameters();
        for (ProcessletInput processletInput : processletInputList) {
            if (processletInput instanceof LiteralInput) {
                inputMap.put(processletInput.getIdentifier().toString(), ((LiteralInput) processletInput).getValue());
            } else if (processletInput instanceof ComplexInput) {
                ComplexInput complexInput = (ComplexInput) processletInput;
                inputMap.put(complexInput.getIdentifier().toString(), getXmlParameter(complexInput));
            }
        }
    }

    /**
     * Returns a value for the given parameter name.
     *
     * @param parameterName Parameter name.
     *
     * @return The corresponding value.
     */
    public String getValue(String parameterName) {
        return inputMap.get(parameterName);
    }

    /**
     * Returns a key value pair of the input data.
     *
     * @return A Map object that consists of key value pair of the input data.
     */
    public Map<String, String> getInputMap() {
        return inputMap;
    }

    @Override
    public String toString() {
        StringBuilder stringBuilder = new StringBuilder();
        for (String key : inputMap.keySet()) {
            stringBuilder.append(key);
            stringBuilder.append(" : ");
            stringBuilder.append(inputMap.get(key));
            stringBuilder.append("\n");
        }
        return stringBuilder.toString();
    }

    private String getXmlParameter(ComplexInput complexInput) {
        try {
            XMLStreamReader xmlStreamReader = complexInput.getValueAsXMLStream();
            return getXmlStringFromXmlStreamReader(new XmlProcessor(xmlStreamReader));
        } catch (XMLStreamException | IOException exception) {
            LOG.error("Error when extracting " + complexInput.getIdentifier() + " : " + exception.getMessage());
            return "";
        }
    }

    private String getXmlStringFromXmlStreamReader(XmlProcessor xmlProcessor) {
        return xmlProcessor.getXmlString();
    }
}
