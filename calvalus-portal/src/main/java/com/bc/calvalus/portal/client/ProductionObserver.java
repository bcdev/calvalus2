package com.bc.calvalus.portal.client;

import com.bc.calvalus.portal.shared.PortalProductionResponse;
import com.bc.calvalus.portal.shared.WorkStatus;
import com.google.gwt.view.client.ListDataProvider;

import java.util.List;

/**
 * An observer for production jobs.
 *
 * @author Norman
 */
public class ProductionObserver implements WorkObserver {
    private final ListDataProvider<CalvalusPortal.JobInfo>  dataProvider;
    private final PortalProductionResponse response;
    private int index;

    public ProductionObserver(ListDataProvider<CalvalusPortal.JobInfo> dataProvider, PortalProductionResponse response) {
        this.dataProvider = dataProvider;
        this.response = response;
    }

    @Override
    public void workStarted(WorkStatus status) {
        List<CalvalusPortal.JobInfo> list = dataProvider.getList();
        CalvalusPortal.JobInfo jobInfo = new CalvalusPortal.JobInfo(response.getProductionId(),
                                                                    response.getProductionName(),
                                                                    status.getState() + "");
        list.add(jobInfo);
        index = list.size() - 1;
        dataProvider.flush();
    }

    @Override
    public void workProgressing(WorkStatus status) {
        List<CalvalusPortal.JobInfo> list = dataProvider.getList();
        CalvalusPortal.JobInfo jobInfo = new CalvalusPortal.JobInfo(response.getProductionId(),
                                                                    response.getProductionName(),
                                                                    status.getState() + " (" + (int) (.5 + status.getProgress() * 100) + "% done)");
        list.set(index, jobInfo);
        dataProvider.flush();
    }

    @Override
    public void workDone(WorkStatus status) {
        List<CalvalusPortal.JobInfo> list = dataProvider.getList();
        CalvalusPortal.JobInfo jobInfo = new CalvalusPortal.JobInfo(response.getProductionId(),
                                                                    response.getProductionName(),
                                                                    status.getState() + "");
        list.set(index, jobInfo);
        dataProvider.flush();

    }
}
