package com.bc.calvalus.wps2.responses;

import static com.bc.calvalus.wps2.jaxb.ExecuteResponse.ProcessOutputs;

import com.bc.calvalus.wps2.jaxb.CodeType;
import com.bc.calvalus.wps2.jaxb.ExecuteResponse;
import com.bc.calvalus.wps2.jaxb.OutputDataType;
import com.bc.calvalus.wps2.jaxb.OutputReferenceType;
import com.bc.calvalus.wps2.jaxb.StatusType;

import javax.xml.datatype.DatatypeConfigurationException;
import javax.xml.datatype.DatatypeFactory;
import javax.xml.datatype.XMLGregorianCalendar;
import java.util.GregorianCalendar;
import java.util.List;

/**
 * Created by hans on 14/08/2015.
 */
public class ExecuteAcceptedResponse {

    public ExecuteResponse getExecuteResponse(List<String> productionResults) throws DatatypeConfigurationException {
        ExecuteResponse executeResponse = new ExecuteResponse();

        StatusType statusType = new StatusType();
        GregorianCalendar gregorianCalendar = new GregorianCalendar();
        XMLGregorianCalendar currentTime = DatatypeFactory.newInstance().newXMLGregorianCalendar(gregorianCalendar);
        statusType.setCreationTime(currentTime);
        statusType.setProcessAccepted("The request has been accepted.");
        executeResponse.setStatus(statusType);

        ProcessOutputs productUrl = new ProcessOutputs();
        OutputDataType url = new OutputDataType();

        CodeType outputId = new CodeType();
        outputId.setValue("productionResults");
        url.setIdentifier(outputId);
        OutputReferenceType urlLink = new OutputReferenceType();
        urlLink.setHref("http://link-to-product");
        urlLink.setMimeType("binary");
        url.setReference(urlLink);
        productUrl.getOutput().add(url);
        executeResponse.setProcessOutputs(productUrl);

        return executeResponse;
    }

}
