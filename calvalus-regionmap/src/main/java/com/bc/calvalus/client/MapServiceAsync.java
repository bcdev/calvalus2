package com.bc.calvalus.client;

import com.bc.calvalus.shared.EncodedRegion;
import com.google.gwt.user.client.rpc.AsyncCallback;

/**
 * The async counterpart of <code>MapService</code>.
 */
public interface MapServiceAsync {
    void getRegions(AsyncCallback<EncodedRegion[]> callback);
}
