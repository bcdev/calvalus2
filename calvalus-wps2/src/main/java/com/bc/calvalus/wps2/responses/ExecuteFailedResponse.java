package com.bc.calvalus.wps2.responses;

import com.bc.calvalus.wps2.jaxb.ExceptionReport;
import com.bc.calvalus.wps2.jaxb.ExecuteResponse;
import com.bc.calvalus.wps2.jaxb.ProcessFailedType;
import com.bc.calvalus.wps2.jaxb.StatusType;

import javax.xml.datatype.DatatypeConfigurationException;
import javax.xml.datatype.DatatypeFactory;
import javax.xml.datatype.XMLGregorianCalendar;
import java.util.GregorianCalendar;

/**
 * Created by hans on 14/08/2015.
 */
public class ExecuteFailedResponse {

    public ExecuteResponse getExecuteResponse() throws DatatypeConfigurationException {
        ExecuteResponse executeResponse = new ExecuteResponse();

        StatusType statusType = new StatusType();
        GregorianCalendar gregorianCalendar = new GregorianCalendar();
        XMLGregorianCalendar currentTime = DatatypeFactory.newInstance().newXMLGregorianCalendar(gregorianCalendar);
        statusType.setCreationTime(currentTime);

        ProcessFailedType processFailedType = new ProcessFailedType();
        ExceptionReport exceptionReport = new ExceptionReport();
        exceptionReport.setVersion("1");
        processFailedType.setExceptionReport(exceptionReport);
        statusType.setProcessFailed(processFailedType);
        executeResponse.setStatus(statusType);

        return executeResponse;
    }
}
