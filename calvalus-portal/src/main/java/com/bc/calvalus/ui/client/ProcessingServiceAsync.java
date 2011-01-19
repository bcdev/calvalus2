package com.bc.calvalus.ui.client;

import com.bc.calvalus.ui.shared.ProcessingRequest;
import com.bc.calvalus.ui.shared.ProcessingRequestException;
import com.google.gwt.user.client.rpc.AsyncCallback;
import com.google.gwt.user.client.rpc.RemoteService;
import com.google.gwt.user.client.rpc.RemoteServiceRelativePath;

/**
 * The client side stub for the RPC service.
 */
@RemoteServiceRelativePath("process")
public interface ProcessingServiceAsync {
    void process(ProcessingRequest request, AsyncCallback<String> callback);
}
