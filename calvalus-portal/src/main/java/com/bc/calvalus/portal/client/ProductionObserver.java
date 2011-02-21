package com.bc.calvalus.portal.client;

import com.bc.calvalus.portal.shared.PortalProduction;
import com.bc.calvalus.portal.shared.WorkStatus;
import com.google.gwt.core.client.GWT;
import com.google.gwt.view.client.ListDataProvider;

import java.util.List;

/**
 * An observer for production jobs.
 *
 * @author Norman
 */
public class ProductionObserver implements WorkObserver {
    private final ListDataProvider<PortalProduction> productions;
    private final PortalProduction production;
    private int index;

    public ProductionObserver(ListDataProvider<PortalProduction> productions,
                              PortalProduction production) {
        this.productions = productions;
        this.production = production;
    }

    @Override
    public void workStarted(WorkStatus status) {
        List<PortalProduction> list = productions.getList();
        production.setWorkStatus(status);
        index = list.indexOf(production);
        if (index == -1) {
            list.add(production);
            index = list.size() - 1;
        }
        productions.refresh();
    }

    @Override
    public void workProgressing(WorkStatus status) {
        processWorkStatus(status);
    }

    @Override
    public void workStopped(WorkStatus status) {
        processWorkStatus(status);
    }

    private void processWorkStatus(WorkStatus status) {
        List<PortalProduction> list = productions.getList();
        PortalProduction production = list.get(index);
        production.setWorkStatus(status);
        productions.refresh();
        // GWT.log("processWorkStatus: status=" + status);
    }
}
