package com.bc.calvalus.wpsrest.wpsoperations.describeprocess;

import com.bc.calvalus.wpsrest.JaxbHelper;
import com.bc.calvalus.wpsrest.jaxb.ExceptionReport;
import com.bc.calvalus.wpsrest.jaxb.ProcessDescriptions;
import com.bc.calvalus.wpsrest.responses.ExceptionResponse;
import com.bc.calvalus.wpsrest.wpsoperations.WpsMetadata;

import javax.xml.bind.JAXBException;
import java.io.StringWriter;
import java.util.logging.Level;
import java.util.logging.Logger;

/**
 * @author hans
 */
public abstract class AbstractDescribeProcessOperation {

    private Logger logger = getLogger();

    private WpsMetadata wpsMetadata;
    private String processorId;

    public AbstractDescribeProcessOperation(WpsMetadata wpsMetadata, String processorId) {
        this.wpsMetadata = wpsMetadata;
        this.processorId = processorId;
    }

    public String describeProcess() {
        StringWriter writer = new StringWriter();
        JaxbHelper jaxbHelper = new JaxbHelper();

        try {
            ProcessDescriptions processDescriptions = getProcessDescriptions(wpsMetadata, processorId);
            jaxbHelper.marshal(processDescriptions, writer);
            return writer.toString();
        } catch (JAXBException exception) {
            logger.log(Level.SEVERE, "An error occurred when trying to construct a DescribeProcess response.", exception);
            ExceptionReport exceptionReport = getExceptionReport(exception);
            try {
                jaxbHelper.marshal(exceptionReport, writer);
                return writer.toString();
            } catch (JAXBException jaxbException) {
                logger.log(Level.SEVERE, "Unable to marshal the WPS exception.", jaxbException);
                return getWpsJaxbExceptionResponse();
            }
        }
    }

    public abstract ProcessDescriptions getProcessDescriptions(WpsMetadata wpsMetadata, String processorId);

    public abstract Logger getLogger();

    public ExceptionReport getExceptionReport(Exception exception) {
        ExceptionResponse exceptionResponse = new ExceptionResponse();
        return exceptionResponse.getGeneralExceptionResponse(exception);
    }

    public String getWpsJaxbExceptionResponse() {
        return "<?xml version=\"1.0\" encoding=\"UTF-8\" standalone=\"yes\"?>\n" +
               "<ExceptionReport version=\"version\" xml:lang=\"Lang\">\n" +
               "    <Exception exceptionCode=\"NoApplicableCode\">\n" +
               "        <ExceptionText>Unable to generate the exception XML : JAXB Exception.</ExceptionText>\n" +
               "    </Exception>\n" +
               "</ExceptionReport>\n";
    }

}
