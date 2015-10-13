package com.bc.calvalus.wpsrest.responses;


import com.bc.calvalus.wpsrest.ServletRequestWrapper;
import com.bc.calvalus.wpsrest.jaxb.DataInputsType;
import com.bc.calvalus.wpsrest.jaxb.DocumentOutputDefinitionType;
import com.bc.calvalus.wpsrest.jaxb.ExecuteResponse;
import com.bc.calvalus.wpsrest.jaxb.OutputDefinitionsType;
import com.bc.calvalus.wpsrest.jaxb.StatusType;

import javax.xml.datatype.DatatypeConfigurationException;
import javax.xml.datatype.DatatypeFactory;
import javax.xml.datatype.XMLGregorianCalendar;
import java.util.GregorianCalendar;
import java.util.List;

/**
 * Created by hans on 14/08/2015.
 */
public class ExecuteAcceptedResponse {

    private final ExecuteResponse executeResponse;

    public ExecuteAcceptedResponse() {
        this.executeResponse = new ExecuteResponse();
        this.executeResponse.setService("WPS");
        this.executeResponse.setVersion("1.0.0");
        this.executeResponse.setLang("en");
    }

    public ExecuteResponse getExecuteResponse(String productId, ServletRequestWrapper servletRequestWrapper) throws DatatypeConfigurationException {
        StatusType statusType = new StatusType();
        GregorianCalendar gregorianCalendar = new GregorianCalendar();
        XMLGregorianCalendar currentTime = DatatypeFactory.newInstance().newXMLGregorianCalendar(gregorianCalendar);
        statusType.setCreationTime(currentTime);
        statusType.setProcessAccepted("The request has been accepted. The status of the process can be found in the URL.");
        executeResponse.setStatus(statusType);

        String getStatusUrl = getStatusUrl(productId, servletRequestWrapper);
        executeResponse.setStatusLocation(getStatusUrl);

        return executeResponse;
    }

    public ExecuteResponse getExecuteResponseWithLineage(String productId,
                                                         DataInputsType dataInputs,
                                                         List<DocumentOutputDefinitionType> rawDataOutput,
                                                         ServletRequestWrapper servletRequestWrapper)
                throws DatatypeConfigurationException {
        StatusType statusType = new StatusType();
        GregorianCalendar gregorianCalendar = new GregorianCalendar();
        XMLGregorianCalendar currentTime = DatatypeFactory.newInstance().newXMLGregorianCalendar(gregorianCalendar);
        statusType.setCreationTime(currentTime);
        statusType.setProcessAccepted("The request has been accepted. The status of the process can be found in the URL.");
        executeResponse.setStatus(statusType);
        String getStatusUrl = getStatusUrl(productId, servletRequestWrapper);
        executeResponse.setStatusLocation(getStatusUrl);
        executeResponse.setDataInputs(dataInputs);
        OutputDefinitionsType outputDefinitionsType = new OutputDefinitionsType();
        outputDefinitionsType.getOutput().addAll(rawDataOutput);
        executeResponse.setOutputDefinitions(outputDefinitionsType);

        return executeResponse;
    }

    private String getStatusUrl(String productId, ServletRequestWrapper servletRequestWrapper) {
        return servletRequestWrapper.getRequestUrl() + "?Service=WPS&Request=GetStatus&JobId=" + productId;
    }
}
