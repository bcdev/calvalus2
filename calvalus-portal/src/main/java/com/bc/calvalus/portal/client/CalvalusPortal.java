package com.bc.calvalus.portal.client;

import com.bc.calvalus.portal.shared.BackendService;
import com.bc.calvalus.portal.shared.BackendServiceAsync;
import com.bc.calvalus.portal.shared.GsProcessorDescriptor;
import com.bc.calvalus.portal.shared.GsProductSet;
import com.bc.calvalus.portal.shared.GsProduction;
import com.bc.calvalus.portal.shared.GsProductionRequest;
import com.bc.calvalus.portal.shared.GsProductionResponse;
import com.google.gwt.core.client.EntryPoint;
import com.google.gwt.core.client.GWT;
import com.google.gwt.maps.client.Maps;
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
    private GsProductSet[] productSets;
    private GsProcessorDescriptor[] processors;
    private ListDataProvider<GsProduction> productions;
    private Map<String, GsProduction> productionsMap;
    private PortalView[] views;
    private Map<String, Integer> viewTabIndices;
    private HorizontalPanel mainPanel;

    public CalvalusPortal() {
        backendService = GWT.create(BackendService.class);
    }

    /**
     * This is the entry point method.
     */
    @Override
    public void onModuleLoad() {

       /*
        * Asynchronously loads the Maps API.
        *
        * The first parameter should be a valid Maps API Key to deploy this
        * application on a public server, but a blank key will work for an
        * application served from localhost.
       */
       Maps.loadMapsApi("", "2", false, new Runnable() {
           public void run() {
               backendService.getProductSets(null, new InitProductSetsCallback());
               backendService.getProcessors(null, new InitProcessorsCallback());
               backendService.getProductions(null, new InitProductionsCallback());
           }
       });
    }

    public GsProductSet[] getProductSets() {
        return productSets;
    }

    public GsProcessorDescriptor[] getProcessors() {
        return processors;
    }

    public ListDataProvider<GsProduction> getProductions() {
        return productions;
    }

    public BackendServiceAsync getBackendService() {
        return backendService;
    }

    public void showView(String id) {
        Integer integer = viewTabIndices.get(id);
        tabPanel.selectTab(integer);
    }

    private void maybeInitFrontend() {
        if (!initialised && isAllInputDataAvailable()) {
            initialised = true;
            initFrontend();
        }
    }

    private void initFrontend() {

        views = new PortalView[]{
// todo nf/nf 20110414 add ManageProductSetsView
//                new ManageProductSetsView(this),
                new OrderL2ProductionView(this),
                new OrderL3ProductionView(this),
                new OrderTAProductionView(this),
                new ManageProductionsView(this),
                new FrameView(this, "FS", "File System", "http://cvmaster00:50070/dfshealth.jsp"),
                new FrameView(this, "JT", "Job Tracker", "http://cvmaster00:50030/jobtracker.jsp"),
        };

        viewTabIndices = new HashMap<String, Integer>();
        for (int i = 0; i < views.length; i++) {
            viewTabIndices.put(views[i].getViewId(), i);
        }

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

    private synchronized void updateProductions(GsProduction[] unknownProductions) {
        if (productions == null) {
            productions = new ListDataProvider<GsProduction>();
            productionsMap = new HashMap<String, GsProduction>();
        }
        boolean listChange = false;
        boolean propertyChange = false;
        ArrayList<GsProduction> deletedProductions = new ArrayList<GsProduction>(productions.getList());
        for (int i = 0; i < unknownProductions.length; i++) {
            GsProduction unknownProduction = unknownProductions[i];
            GsProduction knownProduction = productionsMap.get(unknownProduction.getId());
            if (knownProduction != null) {
                if (!unknownProduction.getProcessingStatus().equals(knownProduction.getProcessingStatus())) {
                    knownProduction.setProcessingStatus(unknownProduction.getProcessingStatus());
                    propertyChange = true;
                }
                if (!unknownProduction.getStagingStatus().equals(knownProduction.getStagingStatus())) {
                    knownProduction.setStagingStatus(unknownProduction.getStagingStatus());
                    propertyChange = true;
                }
                deletedProductions.remove(knownProduction);
            } else {
                productions.getList().add(i, unknownProduction);
                productionsMap.put(unknownProduction.getId(), unknownProduction);
                listChange = true;
            }
        }
        for (GsProduction deletedProduction : deletedProductions) {
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

    public void orderProduction(GsProductionRequest request) {
        getBackendService().orderProduction(request, new AsyncCallback<GsProductionResponse>() {
            public void onSuccess(final GsProductionResponse response) {
                showView(ManageProductionsView.ID);
            }

            public void onFailure(Throwable caught) {
                caught.printStackTrace(System.err);
                Window.alert("Failed to order production:\n" + caught.getMessage());
            }
        });
    }


    private class InitProductSetsCallback implements AsyncCallback<GsProductSet[]> {
        @Override
        public void onSuccess(GsProductSet[] productSets) {
            CalvalusPortal.this.productSets = productSets;
            maybeInitFrontend();
        }

        @Override
        public void onFailure(Throwable caught) {
            caught.printStackTrace(System.err);
            Window.alert("Error!\n" + caught.getMessage());
            CalvalusPortal.this.productSets = new GsProductSet[0];
        }
    }

    private class InitProcessorsCallback implements AsyncCallback<GsProcessorDescriptor[]> {
        @Override
        public void onSuccess(GsProcessorDescriptor[] processors) {
            CalvalusPortal.this.processors = processors;
            maybeInitFrontend();
        }

        @Override
        public void onFailure(Throwable caught) {
            caught.printStackTrace(System.err);
            Window.alert("Error!\n" + caught.getMessage());
            CalvalusPortal.this.processors = new GsProcessorDescriptor[0];
        }
    }

    private class InitProductionsCallback implements AsyncCallback<GsProduction[]> {
        @Override
        public void onSuccess(GsProduction[] productions) {
            updateProductions(productions);
            maybeInitFrontend();
        }

        @Override
        public void onFailure(Throwable caught) {
            caught.printStackTrace(System.err);
            Window.alert("Error!\n" + caught.getMessage());
            CalvalusPortal.this.productions = new ListDataProvider<GsProduction>();
        }
    }

    private class UpdateProductionsCallback implements AsyncCallback<GsProduction[]> {
        @Override
        public void onSuccess(GsProduction[] unknownProductions) {
            updateProductions(unknownProductions);
        }

        @Override
        public void onFailure(Throwable caught) {
            caught.printStackTrace(System.err);
            GWT.log("Failed to get productions from server", caught);
        }
    }
}



