package com.bc.calvalus.wps2.responses;

import static com.bc.calvalus.wps2.jaxb.ExecuteResponse.ProcessOutputs;

import com.bc.calvalus.wps2.jaxb.CodeType;
import com.bc.calvalus.wps2.jaxb.ComplexDataType;
import com.bc.calvalus.wps2.jaxb.DataType;
import com.bc.calvalus.wps2.jaxb.ExecuteResponse;
import com.bc.calvalus.wps2.jaxb.LiteralDataType;
import com.bc.calvalus.wps2.jaxb.OutputDataType;

import java.util.List;

/**
 * Created by hans on 14/08/2015.
 */
public class ExecuteSuccessfulResponse {

    public ExecuteResponse getExecuteResponse(List<String> productionResults) {
        ExecuteResponse executeResponse = new ExecuteResponse();
        ProcessOutputs productUrl = new ProcessOutputs();
        OutputDataType url = new OutputDataType();
        DataType urlValue = new DataType();
        LiteralDataType urlData = new LiteralDataType();
        urlData.setValue("http://dummyUrl.com");
        urlData.setDataType("URL");
        urlValue.setLiteralData(urlData);
        LiteralDataType urlData2 = new LiteralDataType();
        urlData2.setValue("http://dummyUrl2.com");
        urlData2.setDataType("URL");
        urlValue.setLiteralData(urlData2);
        ComplexDataType urlXml = new ComplexDataType();
        urlXml.getContent().add("test1\n");
        urlXml.getContent().add("test2");
        urlValue.setComplexData(urlXml);
        url.setData(urlValue);

        CodeType outputId = new CodeType();
        outputId.setValue("productionResults");
        url.setIdentifier(outputId);
        productUrl.getOutput().add(url);
        executeResponse.setProcessOutputs(productUrl);

        return executeResponse;
    }

}
