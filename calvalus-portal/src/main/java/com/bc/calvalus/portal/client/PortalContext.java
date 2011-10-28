package com.bc.calvalus.portal.client;

import com.bc.calvalus.portal.client.map.Region;
import com.bc.calvalus.portal.client.map.RegionMapModel;
import com.bc.calvalus.portal.shared.BackendServiceAsync;
import com.bc.calvalus.portal.shared.DtoProcessorDescriptor;
import com.bc.calvalus.portal.shared.DtoProductSet;
import com.bc.calvalus.portal.shared.DtoProduction;
import com.google.gwt.user.client.Timer;
import com.google.gwt.view.client.ListDataProvider;

/**
 * The Calvalus Portal application context.
 * Lets you access static data resources, communicate with the server and control the currently displayed view.
 * (Norman says "ooops, that is actually too much responsibility here...")
 *
 * @author Norman
 */
public interface PortalContext {
    RegionMapModel getRegionMapModel();

    ListDataProvider<Region> getRegions();

    // make this return ListDataProvider<GsProductSet>
    DtoProductSet[] getProductSets();

    // make this return ListDataProvider<GsProcessorDescriptor>
    DtoProcessorDescriptor[] getProcessors();

    ListDataProvider<DtoProduction> getProductions();

    BackendServiceAsync getBackendService();

    void showView(String id);

    Timer getProductionsUpdateTimer();

    boolean isProductionListFiltered();

    void setProductionListFiltered(boolean filter);
}
