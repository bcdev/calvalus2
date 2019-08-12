package com.bc.calvalus.portal.shared;

import com.google.gwt.user.client.rpc.AsyncCallback;

public interface ContextRetrievalServiceAsync {

    void getInputSelection(String userName, AsyncCallback<DtoInputSelection> async);
}
