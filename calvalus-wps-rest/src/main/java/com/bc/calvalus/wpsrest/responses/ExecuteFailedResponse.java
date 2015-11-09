package com.bc.calvalus.wpsrest.responses;


import com.bc.calvalus.wpsrest.jaxb.ExceptionReport;
import com.bc.calvalus.wpsrest.jaxb.ExceptionType;
import com.bc.calvalus.wpsrest.jaxb.ExecuteResponse;
import com.bc.calvalus.wpsrest.jaxb.ProcessFailedType;
import com.bc.calvalus.wpsrest.jaxb.StatusType;

import javax.xml.datatype.DatatypeConfigurationException;
import javax.xml.datatype.DatatypeFactory;
import javax.xml.datatype.XMLGregorianCalendar;
import java.util.GregorianCalendar;

/**
 * Created by hans on 14/08/2015.
 */
public class ExecuteFailedResponse {

    private final ExecuteResponse executeResponse;

    public ExecuteFailedResponse() {
        this.executeResponse = new ExecuteResponse();
        this.executeResponse.setService("WPS");
        this.executeResponse.setVersion("1.0.0");
        this.executeResponse.setLang("en");
    }


    public ExecuteResponse getExecuteResponse(String exceptionMessage) throws DatatypeConfigurationException {
        StatusType statusType = new StatusType();
        GregorianCalendar gregorianCalendar = new GregorianCalendar();
        XMLGregorianCalendar currentTime = DatatypeFactory.newInstance().newXMLGregorianCalendar(gregorianCalendar);
        statusType.setCreationTime(currentTime);

        ProcessFailedType processFailedType = new ProcessFailedType();
        ExceptionReport exceptionReport = new ExceptionReport();
        exceptionReport.setVersion("1");
        ExceptionType exceptionType = new ExceptionType();
        exceptionType.getExceptionText().add(exceptionMessage);
        exceptionReport.getException().add(exceptionType);
        processFailedType.setExceptionReport(exceptionReport);
        statusType.setProcessFailed(processFailedType);
        executeResponse.setStatus(statusType);

        return executeResponse;
    }
}
