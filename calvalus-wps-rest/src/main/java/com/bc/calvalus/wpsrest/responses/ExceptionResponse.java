package com.bc.calvalus.wpsrest.responses;

import com.bc.calvalus.wpsrest.jaxb.ExceptionReport;
import com.bc.calvalus.wpsrest.jaxb.ExceptionType;

/**
 * Created by hans on 25/08/2015.
 */
public class ExceptionResponse {

    public ExceptionReport getExceptionResponse(Exception exception) {
        ExceptionReport exceptionReport = new ExceptionReport();
        ExceptionType exceptionResponse = new ExceptionType();
        exceptionResponse.getExceptionText().add(exception.getMessage());
        exceptionResponse.setExceptionCode("exceptionCode");
        exceptionResponse.setLocator("locator");

        exceptionReport.getException().add(exceptionResponse);
        exceptionReport.setLang("Lang");
        exceptionReport.setVersion("version");

        return exceptionReport;
    }

}
