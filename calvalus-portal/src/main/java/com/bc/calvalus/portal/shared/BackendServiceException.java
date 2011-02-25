package com.bc.calvalus.portal.shared;

import com.google.gwt.user.client.rpc.IsSerializable;

public class BackendServiceException extends Exception implements IsSerializable {
    BackendServiceException() {
    }

    public BackendServiceException(String message) {
        super(message);
    }

    public BackendServiceException(String message, Throwable cause) {
        super(message, cause);
    }

    public BackendServiceException(Throwable cause) {
        super(cause);
    }
}
