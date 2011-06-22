package com.bc.calvalus.portal.client;

import com.bc.calvalus.portal.shared.BackendService;
import com.bc.calvalus.portal.shared.BackendServiceAsync;
import com.bc.calvalus.portal.shared.GsProcessorDescriptor;
import com.bc.calvalus.portal.shared.GsProductSet;
import com.bc.calvalus.portal.shared.GsProduction;
import com.bc.calvalus.portal.shared.GsRegion;
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
import com.google.gwt.user.client.ui.RootPanel;
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
public class CalvalusPortal implements EntryPoint, PortalContext {

    private static final int UPDATE_PERIOD_MILLIS = 1000;
    public static final String NO_FILTER = "";

    private final BackendServiceAsync backendService;
    private boolean initialised;

    // Data provided by various external services
    private GsRegion[] regions;
    private GsProductSet[] productSets;
    private GsProcessorDescriptor[] processors;
    private ListDataProvider<GsProduction> productions;
    private Map<String, GsProduction> productionsMap;
    private PortalView[] views;
    private Map<String, Integer> viewTabIndices;
    private DecoratedTabPanel mainPanel;

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
                backendService.getRegions(NO_FILTER, new InitRegionsCallback());
                backendService.getProductSets(NO_FILTER, new InitProductSetsCallback());
                backendService.getProcessors(NO_FILTER, new InitProcessorsCallback());
                backendService.getProductions(NO_FILTER, new InitProductionsCallback());
            }
        });
    }

    @Override
    public GsRegion[] getRegions() {
        return regions;
    }

    @Override
    public GsProductSet[] getProductSets() {
        return productSets;
    }

    @Override
    public GsProcessorDescriptor[] getProcessors() {
        return processors;
    }

    @Override
    public ListDataProvider<GsProduction> getProductions() {
        return productions;
    }

    @Override
    public BackendServiceAsync getBackendService() {
        return backendService;
    }

    @Override
    public void showView(String id) {
        Integer integer = viewTabIndices.get(id);
        mainPanel.selectTab(integer);
    }

    private void maybeInitFrontend() {
        if (!initialised && isAllInputDataAvailable()) {
            initialised = true;
            initFrontend();
        }
    }

    private void initFrontend() {

        views = new PortalView[]{
                new OrderL2ProductionView(this),
                new OrderL3ProductionView(this),
                new OrderTAProductionView(this),
                new ManageProductionsView(this),
                new ManageRegionsView(this),
                new FrameView(this, "FS", "File System", "http://cvmaster00:50070/dfshealth.jsp"),
                new FrameView(this, "JT", "Job Tracker", "http://cvmaster00:50030/jobtracker.jsp"),
        };

        viewTabIndices = new HashMap<String, Integer>();
        for (int i = 0; i < views.length; i++) {
            viewTabIndices.put(views[i].getViewId(), i);
        }

        mainPanel = new DecoratedTabPanel();
        mainPanel.ensureDebugId("mainPanel");
        mainPanel.setWidth("100%");
        mainPanel.setHeight("100%");
        for (PortalView view : views) {
            mainPanel.add(view, view.getTitle());
        }

        removeSplashScreen();
        showView(OrderL2ProductionView.ID);
        showMainPanel(mainPanel);

        for (PortalView view : views) {
            view.handlePortalStartedUp();
        }

        // Start a timer that periodically retrieves production statuses from server
        Timer timer = new Timer() {
            @Override
            public void run() {
                backendService.getProductions(NO_FILTER, new UpdateProductionsCallback());
            }
        };
        timer.scheduleRepeating(UPDATE_PERIOD_MILLIS);
    }

    private void showMainPanel(Widget mainPanel) {
        //noinspection GwtToHtmlReferences
        RootPanel.get("mainPanel").add(mainPanel);
    }

    private void removeSplashScreen() {
        DOM.removeChild(RootPanel.getBodyElement(), DOM.getElementById("splashScreen"));
    }

    private boolean isAllInputDataAvailable() {
        return regions != null
                && productSets != null
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

    /*
     * Do not remove code.
     * Alternative menu component - example taken from GWTShowcase. May be used as a left-menu.
     */
    @SuppressWarnings({"UnusedDeclaration"})
    private CellTree createMainMenu() {
        final SingleSelectionModel<PortalView> selectionModel = new SingleSelectionModel<PortalView>();
        final MainMenuModel treeModel = new MainMenuModel(this, views, selectionModel);
        // Create the cell tree.
        CellTree mainMenu = new CellTree(treeModel, null);
        mainMenu.setAnimationEnabled(true);
        mainMenu.setKeyboardSelectionPolicy(HasKeyboardSelectionPolicy.KeyboardSelectionPolicy.DISABLED);
        selectionModel.addSelectionChangeHandler(
                new SelectionChangeEvent.Handler() {
                    public void onSelectionChange(SelectionChangeEvent event) {
                        PortalView selected = selectionModel.getSelectedObject();
                        if (selected != null) {
                            showView(selected.getViewId());
                        }
                    }
                });
        return mainMenu;
    }

    private class InitRegionsCallback implements AsyncCallback<GsRegion[]> {
        @Override
        public void onSuccess(GsRegion[] regions) {
            CalvalusPortal.this.regions = regions;
            maybeInitFrontend();
        }

        @Override
        public void onFailure(Throwable caught) {
            caught.printStackTrace(System.err);
            Window.alert("Error!\n" + caught.getMessage());
            CalvalusPortal.this.regions = new GsRegion[0];
        }
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
