package com.bc.calvalus.client;

import com.bc.calvalus.shared.EncodedRegion;
import com.google.gwt.user.client.rpc.RemoteService;
import com.google.gwt.user.client.rpc.RemoteServiceRelativePath;

import java.io.IOException;

/**
 * The client side stub for the RPC service.
 */
@RemoteServiceRelativePath("map")
public interface MapService extends RemoteService {
    EncodedRegion[] getRegions() throws IOException;
}
