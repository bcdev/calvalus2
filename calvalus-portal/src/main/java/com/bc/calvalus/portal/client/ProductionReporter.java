package com.bc.calvalus.portal.client;

import com.bc.calvalus.portal.shared.BackendServiceAsync;
import com.bc.calvalus.portal.shared.PortalProduction;
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
    private WorkStatus reportedStatus;

    public ProductionReporter(BackendServiceAsync backendService, PortalProduction production) {
        this.backendService = backendService;
        this.production = production;
        this.reportedStatus = production.getWorkStatus();
    }

    @Override
    public WorkStatus getWorkStatus() {
        backendService.getProductionStatus(production.getId(), new AsyncCallback<WorkStatus>() {
            @Override
            public void onSuccess(WorkStatus result) {
                reportedStatus = result;
                production.setWorkStatus(reportedStatus);
            }

            @Override
            public void onFailure(Throwable caught) {
                reportedStatus = new WorkStatus(WorkStatus.State.ERROR, caught.getMessage(), 0.0);
                // Note: we don't call production.setWorkStatus here, because we have not been able to
                // retrieve the actual status.
            }
        });
        return reportedStatus;
    }

    @Override
    public String toString() {
        return "ProductionReporter{" +
                "production=" + production +
                ", reportedStatus=" + reportedStatus +
                '}';
    }
}
