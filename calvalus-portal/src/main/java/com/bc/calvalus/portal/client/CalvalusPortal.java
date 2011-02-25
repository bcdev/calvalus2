package com.bc.calvalus.portal.client;

import com.bc.calvalus.portal.shared.BackendService;
import com.bc.calvalus.portal.shared.BackendServiceAsync;
import com.bc.calvalus.portal.shared.PortalProcessor;
import com.bc.calvalus.portal.shared.PortalProductSet;
import com.bc.calvalus.portal.shared.PortalProduction;
import com.bc.calvalus.portal.shared.PortalProductionRequest;
import com.bc.calvalus.portal.shared.PortalProductionResponse;
import com.google.gwt.core.client.EntryPoint;
import com.google.gwt.core.client.GWT;
import com.google.gwt.user.cellview.client.CellTree;
import com.google.gwt.user.cellview.client.HasKeyboardSelectionPolicy;
import com.google.gwt.user.client.DOM;
import com.google.gwt.user.client.Timer;
import com.google.gwt.user.client.Window;
import com.google.gwt.user.client.rpc.AsyncCallback;
import com.google.gwt.user.client.ui.DecoratedTabPanel;
import com.google.gwt.user.client.ui.HorizontalPanel;
import com.google.gwt.user.client.ui.Label;
import com.google.gwt.user.client.ui.RootPanel;
import com.google.gwt.user.client.ui.VerticalPanel;
import com.google.gwt.user.client.ui.Widget;
import com.google.gwt.view.client.ListDataProvider;
import com.google.gwt.view.client.SelectionChangeEvent;
import com.google.gwt.view.client.SingleSelectionModel;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.Map;

/**
 * Entry point classes define <code>onModuleLoad()</code>.
 *
 * @author Norman
 */
public class CalvalusPortal implements EntryPoint {

    private static final int UPDATE_PERIOD_MILLIS = 1000;

    private final BackendServiceAsync backendService;
    private boolean initialised;
    private DecoratedTabPanel tabPanel;

    // Data provided by various external services
    private PortalProductSet[] productSets;
    private PortalProcessor[] processors;
    private ListDataProvider<PortalProduction> productions;
    private Map<String, PortalProduction> productionsMap;
    private PortalView[] views;
    private HorizontalPanel mainPanel;

    public CalvalusPortal() {
        backendService = GWT.create(BackendService.class);
    }

    /**
     * This is the entry point method.
     */
    @Override
    public void onModuleLoad() {
        backendService.getProductSets(null, new InitProductSetsCallback());
        backendService.getProcessors(null, new InitProcessorsCallback());
        backendService.getProductions(null, new InitProductionsCallback());
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
                new OrderL3ProductionView(this),
                new ManageProductionsView(this),
                new FrameView(this, 4, "File System", "http://cvmaster00:50070/dfshealth.jsp"),
                new FrameView(this, 5, "Job Tracker", "http://cvmaster00:50030/jobtracker.jsp"),
        };

        tabPanel = new DecoratedTabPanel();
        tabPanel.setWidth("640px");
        tabPanel.setWidth("480px");
        tabPanel.setAnimationEnabled(true);
        tabPanel.ensureDebugId("cwTabPanel");

        for (PortalView view : views) {
            tabPanel.add(view, view.getTitle());
        }


        mainPanel = new HorizontalPanel();
        //mainPanel.add(createMainMenu()); // test, test, test
        mainPanel.add(tabPanel);

        removeSplashScreen();
        showView(OrderL2ProductionView.ID);
        showMainPanel();

        for (PortalView view : views) {
            view.handlePortalStartedUp();
        }

        // Start a timer that periodically retrieves production statuses from server
        Timer timer = new Timer() {
            @Override
            public void run() {
                backendService.getProductions(null, new UpdateProductionsCallback());
            }
        };
        timer.scheduleRepeating(UPDATE_PERIOD_MILLIS);
    }

    // test, test, test
    private CellTree createMainMenu() {
        final SingleSelectionModel<PortalView> selectionModel = new SingleSelectionModel<PortalView>();
        final MainMenuModel treeModel = new MainMenuModel(this, selectionModel);
        // Create the cell tree.
        CellTree mainMenu = new CellTree(treeModel, null);
        mainMenu.setAnimationEnabled(true);
        mainMenu.setKeyboardSelectionPolicy(HasKeyboardSelectionPolicy.KeyboardSelectionPolicy.DISABLED);
        selectionModel.addSelectionChangeHandler(
                new SelectionChangeEvent.Handler() {
                    public void onSelectionChange(SelectionChangeEvent event) {
                        PortalView selected = selectionModel.getSelectedObject();
                        if (selected != null) {
                            Window.alert("About to show " + selected.getTitle());
                        }
                    }
                });
        return mainMenu;
    }

    private void showMainPanel() {
        RootPanel.get("mainPanel").add(mainPanel);
    }

    private void removeSplashScreen() {
        DOM.removeChild(RootPanel.getBodyElement(), DOM.getElementById("splashScreen"));
    }

    static VerticalPanel createLabeledWidgetV(String labelText, Widget widget) {
        VerticalPanel panel = new VerticalPanel();
        panel.setSpacing(2);
        panel.add(new Label(labelText));
        panel.add(widget);
        return panel;
    }

    static HorizontalPanel createLabeledWidgetH(String labelText, Widget widget) {
        HorizontalPanel panel = new HorizontalPanel();
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

    private synchronized void updateProductions(PortalProduction[] unknownProductions) {
        if (productions == null) {
            productions = new ListDataProvider<PortalProduction>();
            productionsMap = new HashMap<String, PortalProduction>();
        }
        boolean listChange = false;
        boolean propertyChange = false;
        ArrayList<PortalProduction> deletedProductions = new ArrayList<PortalProduction>(productions.getList());
        for (int i = 0; i < unknownProductions.length; i++) {
            PortalProduction unknownProduction = unknownProductions[i];
            PortalProduction knownProduction = productionsMap.get(unknownProduction.getId());
            if (knownProduction != null) {
                if (!unknownProduction.getStatus().equals(knownProduction.getStatus())) {
                    knownProduction.setStatus(unknownProduction.getStatus());
                    propertyChange = true;
                }
                deletedProductions.remove(knownProduction);
            } else {
                productions.getList().add(i, unknownProduction);
                productionsMap.put(unknownProduction.getId(), unknownProduction);
                listChange = true;
            }
        }
        for (PortalProduction deletedProduction : deletedProductions) {
            productions.getList().remove(deletedProduction);
            productionsMap.remove(deletedProduction.getId());
            listChange = true;
        }
        // GWT.log("Updated productions: got " + productions.getList().size() + ",  listChange = " + listChange + ", propertyChange = " + propertyChange);
        if (listChange) {
            productions.flush();
        }
        if (propertyChange) {
            productions.refresh();
        }
    }

    public void orderProduction(PortalProductionRequest request) {
        getBackendService().orderProduction(request, new AsyncCallback<PortalProductionResponse>() {
            public void onSuccess(final PortalProductionResponse response) {
                ManageProductionsView view = (ManageProductionsView) getView(ManageProductionsView.ID);
                view.show();
            }

            public void onFailure(Throwable caught) {
                Window.alert("Failed to order production:\n" + caught.getMessage());
            }
        });
    }


    private class InitProductSetsCallback implements AsyncCallback<PortalProductSet[]> {
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

    private class InitProcessorsCallback implements AsyncCallback<PortalProcessor[]> {
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

    private class InitProductionsCallback implements AsyncCallback<PortalProduction[]> {
        @Override
        public void onSuccess(PortalProduction[] productions) {
            updateProductions(productions);
            maybeInitFrontend();
        }

        @Override
        public void onFailure(Throwable caught) {
            Window.alert("Error!\n" + caught.getMessage());
            CalvalusPortal.this.productions = new ListDataProvider<PortalProduction>();
        }
    }

    private class UpdateProductionsCallback implements AsyncCallback<PortalProduction[]> {
        @Override
        public void onSuccess(PortalProduction[] unknownProductions) {
            updateProductions(unknownProductions);
        }

        @Override
        public void onFailure(Throwable caught) {
            GWT.log("Failed to get productions from server", caught);
        }
    }
}



