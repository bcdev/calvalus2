package com.bc.calvalus.portal.client;

import com.bc.calvalus.portal.shared.BackendServiceAsync;
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
    private final PortalProductionResponse response;
    private WorkStatus currentStatus;

    public ProductionReporter(BackendServiceAsync backendService, PortalProductionResponse response) {
        this.backendService = backendService;
        this.response = response;
        this.currentStatus = new WorkStatus(WorkStatus.State.WAITING, "Waiting for progress...", 0.0);
    }

    @Override
    public WorkStatus getWorkStatus() {
        backendService.getProductionStatus(response.getProductionId(), new AsyncCallback<WorkStatus>() {
            @Override
            public void onSuccess(WorkStatus result) {
                currentStatus = result;
            }

            @Override
            public void onFailure(Throwable caught) {
                currentStatus = new WorkStatus(WorkStatus.State.ERROR, "Error: " + caught.getMessage(), 0.0);
            }
        });
        return currentStatus;
    }
}
