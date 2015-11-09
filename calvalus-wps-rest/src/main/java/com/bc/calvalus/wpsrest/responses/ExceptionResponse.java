package com.bc.calvalus.wpsrest.responses;

import com.bc.calvalus.wpsrest.jaxb.ExceptionReport;
import com.bc.calvalus.wpsrest.jaxb.ExceptionType;

import java.util.Date;

/**
 * @author hans
 */
public class ExceptionResponse {

    public ExceptionReport getGeneralExceptionWithCustomMessageResponse(String errorMessage) {
        return getGeneralExceptionReport(errorMessage);
    }

    private ExceptionReport getGeneralExceptionReport(String errorMessage) {
        ExceptionReport exceptionReport = new ExceptionReport();
        ExceptionType exceptionResponse = new ExceptionType();
        exceptionResponse.getExceptionText().add(errorMessage);
        exceptionResponse.setExceptionCode("NoApplicableCode");

        exceptionReport.getException().add(exceptionResponse);
        exceptionReport.setLang("Lang");
        exceptionReport.setVersion("version");
        return exceptionReport;
    }

    public ExceptionReport getGeneralExceptionResponse(Exception exception) {
        return getGeneralExceptionReport(exception.getMessage());
    }

    public ExceptionReport getMissingParameterExceptionResponse(Exception exception, String missingParameter) {
        ExceptionReport exceptionReport = new ExceptionReport();
        ExceptionType exceptionResponse = new ExceptionType();
        exceptionResponse.getExceptionText().add(exception.getMessage());
        exceptionResponse.setExceptionCode("MissingParameterValue");
        exceptionResponse.setLocator(missingParameter);

        exceptionReport.getException().add(exceptionResponse);
        exceptionReport.setLang("Lang");
        exceptionReport.setVersion("version");

        return exceptionReport;
    }

    public ExceptionReport getInvalidParameterExceptionResponse(Exception exception, String invalidParameter) {
        ExceptionReport exceptionReport = new ExceptionReport();
        ExceptionType exceptionResponse = new ExceptionType();
        exceptionResponse.getExceptionText().add(exception.getMessage());
        exceptionResponse.setExceptionCode("InvalidParameterValue");
        exceptionResponse.setLocator(invalidParameter);

        exceptionReport.getException().add(exceptionResponse);
        exceptionReport.setLang("Lang");
        exceptionReport.setVersion("version");

        return exceptionReport;
    }

}
