package com.bc.calvalus.portal.client;

import com.bc.calvalus.portal.shared.BackendService;
import com.bc.calvalus.portal.shared.BackendServiceAsync;
import com.bc.calvalus.portal.shared.PortalProcessor;
import com.bc.calvalus.portal.shared.PortalProductSet;
import com.bc.calvalus.portal.shared.PortalProduction;
import com.google.gwt.core.client.EntryPoint;
import com.google.gwt.core.client.GWT;
import com.google.gwt.user.client.DOM;
import com.google.gwt.user.client.Window;
import com.google.gwt.user.client.rpc.AsyncCallback;
import com.google.gwt.user.client.ui.DecoratedTabPanel;
import com.google.gwt.user.client.ui.Label;
import com.google.gwt.user.client.ui.RootPanel;
import com.google.gwt.user.client.ui.VerticalPanel;
import com.google.gwt.user.client.ui.Widget;
import com.google.gwt.view.client.ListDataProvider;

import java.util.Arrays;

/**
 * Entry point classes define <code>onModuleLoad()</code>.
 *
 * @author Norman
 */
public class CalvalusPortal implements EntryPoint {

    private final BackendServiceAsync backendService;
    private boolean initialised;
    private DecoratedTabPanel tabPanel;

    // Data provided by various external services
    private PortalProductSet[] productSets;
    private PortalProcessor[] processors;
    private ListDataProvider<PortalProduction> productions;
    private PortalView[] views;

    public CalvalusPortal() {
        backendService = GWT.create(BackendService.class);
    }

    /**
     * This is the entry point method.
     */
    @Override
    public void onModuleLoad() {
        backendService.getProductSets(null, new GetProductSetsCallback());
        backendService.getProcessors(null, new GetProcessorsCallback());
        backendService.getProductions(null, new GetProductionsCallback());
    }

    public PortalProductSet[] getProductSets() {
        return productSets;
    }

    public PortalProcessor[] getProcessors() {
        return processors;
    }

    public ListDataProvider<PortalProduction> getProductions() {
        return productions;
    }

    public BackendServiceAsync getBackendService() {
        return backendService;
    }

    public void showView(int id) {
        tabPanel.selectTab(id);
    }

    public PortalView getView(int id) {
        return views[id];
    }

    private void maybeInitFrontend() {
        if (!initialised && isAllInputDataAvailable()) {
            initialised = true;
            initFrontend();
        }
    }

    private void initFrontend() {
        views = new PortalView[]{
                new ManageProductSetsView(this),
                new OrderL2ProductionView(this),
                new ManageProductionsView(this),
        };

        tabPanel = new DecoratedTabPanel();
        tabPanel.setWidth("640px");
        tabPanel.setAnimationEnabled(true);
        tabPanel.ensureDebugId("cwTabPanel");

        for (PortalView view : views) {
            tabPanel.add(view, view.getTitle());
        }

        showView(OrderL2ProductionView.ID);

        DOM.removeChild(RootPanel.getBodyElement(), DOM.getElementById("splashScreen"));
        RootPanel.get("mainPanel").add(tabPanel);

        for (PortalView view : views) {
            view.handlePortalStartedUp();
        }
    }

    static VerticalPanel createLabeledWidget(String labelText, Widget widget) {
        VerticalPanel panel = new VerticalPanel();
        panel.setSpacing(2);
        panel.add(new Label(labelText));
        panel.add(widget);
        return panel;
    }

    private boolean isAllInputDataAvailable() {
        return productSets != null
                && processors != null
                && productions != null;
    }

    private class GetProductSetsCallback implements AsyncCallback<PortalProductSet[]> {
        @Override
        public void onSuccess(PortalProductSet[] productSets) {
            CalvalusPortal.this.productSets = productSets;
            maybeInitFrontend();
        }

        @Override
        public void onFailure(Throwable caught) {
            Window.alert("Error!\n" + caught.getMessage());
            CalvalusPortal.this.productSets = new PortalProductSet[0];
        }
    }

    private class GetProcessorsCallback implements AsyncCallback<PortalProcessor[]> {
        @Override
        public void onSuccess(PortalProcessor[] processors) {
            CalvalusPortal.this.processors = processors;
            maybeInitFrontend();
        }

        @Override
        public void onFailure(Throwable caught) {
            Window.alert("Error!\n" + caught.getMessage());
            CalvalusPortal.this.processors = new PortalProcessor[0];
        }
    }

    private class GetProductionsCallback implements AsyncCallback<PortalProduction[]> {
        @Override
        public void onSuccess(PortalProduction[] productions) {
            CalvalusPortal.this.productions = new ListDataProvider<PortalProduction>();
            CalvalusPortal.this.productions.getList().addAll(Arrays.asList(productions));
            maybeInitFrontend();
        }

        @Override
        public void onFailure(Throwable caught) {
            Window.alert("Error!\n" + caught.getMessage());
            CalvalusPortal.this.productions = new ListDataProvider<PortalProduction>();
        }
    }
}



