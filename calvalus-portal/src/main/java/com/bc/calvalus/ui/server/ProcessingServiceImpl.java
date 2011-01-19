package com.bc.calvalus.ui.server;

import com.bc.calvalus.ui.client.ProcessingService;
import com.bc.calvalus.ui.shared.ProcessingRequest;
import com.bc.calvalus.ui.shared.ProcessingRequestException;
import com.google.gwt.user.server.rpc.RemoteServiceServlet;

import java.text.MessageFormat;

/**
 * The server side implementation of the RPC processing service.
 */
@SuppressWarnings("serial")
public class ProcessingServiceImpl extends RemoteServiceServlet implements ProcessingService {

    public String process(ProcessingRequest request) throws ProcessingRequestException {
        if (!ProcessingRequest.isValid(request)) {
            throw new ProcessingRequestException("Invalid processing request.");
        }
        return MessageFormat.format("About to process {0} to {1} using {2}.",
                                    request.getInputProductSet(),
                                    request.getOutputProductSet(),
                                    request.getProcessorName());
    }


}
