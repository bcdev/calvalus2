package com.bc.calvalus.portal.client;

import com.bc.calvalus.portal.shared.BackendServiceAsync;
import com.bc.calvalus.portal.shared.PortalProduction;
import com.bc.calvalus.portal.shared.PortalProductionResponse;
import com.bc.calvalus.portal.shared.WorkStatus;
import com.google.gwt.user.client.rpc.AsyncCallback;

/**
 * A reporter for production status.
 *
 * @author Norman
 */
public class ProductionReporter implements WorkReporter {
    private final BackendServiceAsync backendService;
    private final PortalProduction production;
    private WorkStatus currentStatus;

    public ProductionReporter(BackendServiceAsync backendService, PortalProduction production) {
        this.backendService = backendService;
        this.production = production;
        this.currentStatus = production.getWorkStatus();
    }

    @Override
    public WorkStatus getWorkStatus() {
        backendService.getProductionStatus(production.getId(), new AsyncCallback<WorkStatus>() {
            @Override
            public void onSuccess(WorkStatus result) {
                currentStatus = result;
            }

            @Override
            public void onFailure(Throwable caught) {
                currentStatus = new WorkStatus(WorkStatus.State.ERROR, caught.getMessage(), 0.0);
            }
        });
        return currentStatus;
    }
}
