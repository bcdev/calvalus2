package com.bc.calvalus.portal.client;

import com.bc.calvalus.portal.shared.BackendServiceAsync;
import com.bc.calvalus.portal.shared.GsProcessorDescriptor;
import com.bc.calvalus.portal.shared.GsProductSet;
import com.bc.calvalus.portal.shared.GsProduction;
import com.google.gwt.view.client.ListDataProvider;

/**
 * The Calvalus Portal application context.
 * Lets you access static data resources, communicate with the server and control the currently displayed view.
 * (Norman says "ooops, that is actually too much responsibility here...")
 *
 * @author Norman
 */
public interface PortalContext {
    // make this return ListDataProvider<GsProductSet>
    GsProductSet[] getProductSets();

    // make this return ListDataProvider<GsProcessorDescriptor>
    GsProcessorDescriptor[] getProcessors();

    ListDataProvider<GsProduction> getProductions();

    BackendServiceAsync getBackendService();

    void showView(String id);
}
