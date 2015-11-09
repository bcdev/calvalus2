package com.bc.calvalus.wpsrest.responses;

import com.bc.calvalus.wpsrest.jaxb.ExecuteResponse;
import com.bc.calvalus.wpsrest.jaxb.ProcessStartedType;
import com.bc.calvalus.wpsrest.jaxb.StatusType;

import javax.xml.datatype.DatatypeConfigurationException;
import javax.xml.datatype.DatatypeFactory;
import javax.xml.datatype.XMLGregorianCalendar;
import java.util.GregorianCalendar;

/**
 * Created by hans on 25/08/2015.
 */
public class ExecuteStartedResponse {

    private final ExecuteResponse executeResponse;

    public ExecuteStartedResponse() {
        this.executeResponse = new ExecuteResponse();
        this.executeResponse.setService("WPS");
        this.executeResponse.setVersion("1.0.0");
        this.executeResponse.setLang("en");
    }

    public ExecuteResponse getExecuteResponse(String state, float progress) throws DatatypeConfigurationException {
        StatusType statusType = new StatusType();
        GregorianCalendar gregorianCalendar = new GregorianCalendar();
        XMLGregorianCalendar currentTime = DatatypeFactory.newInstance().newXMLGregorianCalendar(gregorianCalendar);
        statusType.setCreationTime(currentTime);

        ProcessStartedType processStartedType = new ProcessStartedType();
        processStartedType.setValue(state);
        processStartedType.setPercentCompleted(Math.round(progress));
        statusType.setProcessStarted(processStartedType);
        executeResponse.setStatus(statusType);

        return executeResponse;
    }
}
