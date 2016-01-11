package com.bc.calvalus.wpsrest.responses;

import com.bc.calvalus.wpsrest.jaxb.ExceptionReport;
import com.bc.calvalus.wpsrest.jaxb.ExceptionType;

/**
 * @author hans
 */
public class ExceptionResponse {

    public ExceptionReport getGeneralExceptionWithCustomMessageResponse(String errorMessage, Throwable cause) {
        return getGeneralExceptionReport(errorMessage, cause);
    }

    private ExceptionReport getGeneralExceptionReport(String errorMessage, Throwable cause) {
        ExceptionReport exceptionReport = new ExceptionReport();
        ExceptionType exceptionResponse = new ExceptionType();
        if (cause != null) {
            exceptionResponse.getExceptionText().add(errorMessage + " : " + cause.getMessage());
        } else {
            exceptionResponse.getExceptionText().add(errorMessage);
        }
        exceptionResponse.setExceptionCode("NoApplicableCode");

        exceptionReport.getException().add(exceptionResponse);
        exceptionReport.setLang("Lang");
        exceptionReport.setVersion("version");
        return exceptionReport;
    }

    public ExceptionReport getGeneralExceptionResponse(Exception exception) {
        return getGeneralExceptionReport(exception.getMessage(), exception.getCause());
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
