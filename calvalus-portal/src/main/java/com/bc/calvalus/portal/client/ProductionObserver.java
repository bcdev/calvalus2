package com.bc.calvalus.portal.client;

import com.bc.calvalus.portal.shared.PortalProductionResponse;
import com.bc.calvalus.portal.shared.WorkStatus;
import com.google.gwt.user.client.Window;

/**
 * An observer for production jobs.
 *
 * @author Norman
 */
public class ProductionObserver implements WorkObserver {
    private final PortalProductionResponse response;

    public ProductionObserver(PortalProductionResponse response) {
        this.response = response;
    }

    @Override
    public void workStarted(WorkStatus status) {
        Window.alert("Starting " + response.getProductionId());
    }

    @Override
    public void workProgressing(WorkStatus status) {
        System.out.println("Progress: " + status.getProgress());
    }

    @Override
    public void workDone(WorkStatus status) {
        Window.alert("Done with " + response.getProductionId());
    }
}
