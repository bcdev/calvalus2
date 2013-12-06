package com.bc.calvalus.portal.client;

import com.bc.calvalus.commons.shared.BundleFilter;
import com.bc.calvalus.portal.client.map.Region;
import com.bc.calvalus.portal.client.map.RegionConverter;
import com.bc.calvalus.portal.client.map.RegionMapModel;
import com.bc.calvalus.portal.client.map.RegionMapModelImpl;
import com.bc.calvalus.portal.shared.BackendService;
import com.bc.calvalus.portal.shared.BackendServiceAsync;
import com.bc.calvalus.portal.shared.DtoProcessorDescriptor;
import com.bc.calvalus.portal.shared.DtoProductSet;
import com.bc.calvalus.portal.shared.DtoProduction;
import com.bc.calvalus.portal.shared.DtoRegion;
import com.google.gwt.core.client.EntryPoint;
import com.google.gwt.core.client.GWT;
import com.google.gwt.maps.client.LoadApi;
import com.google.gwt.user.client.Command;
import com.google.gwt.user.client.DOM;
import com.google.gwt.user.client.Timer;
import com.google.gwt.user.client.rpc.AsyncCallback;
import com.google.gwt.user.client.ui.DeckLayoutPanel;
import com.google.gwt.user.client.ui.MenuBar;
import com.google.gwt.user.client.ui.MenuItem;
import com.google.gwt.user.client.ui.RootPanel;
import com.google.gwt.user.client.ui.VerticalPanel;
import com.google.gwt.user.client.ui.Widget;
import com.google.gwt.view.client.ListDataProvider;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

/**
 * Entry point classes define <code>onModuleLoad()</code>.
 *
 * @author Norman
 */
public class CalvalusPortal2 implements EntryPoint, PortalContext {

    public static final String NO_FILTER = "";

    private final BackendServiceAsync backendService;
    private boolean initialised;

    // Data provided by various external services
    private ListDataProvider<Region> regions;
    private DtoProductSet[] productSets;
    private DtoProcessorDescriptor[] systemProcessors;
    private DtoProcessorDescriptor[] userProcessors;
    private DtoProcessorDescriptor[] allUserProcessors;
    private ListDataProvider<DtoProduction> productions;
    private Map<String, DtoProduction> productionsMap;
    private Map<String, PortalView> idToViewMap;
    private DeckLayoutPanel viewPanel;
    // A timer that periodically retrieves production statuses from server
    private Timer productionsUpdateTimer;
    private RegionMapModel regionMapModel;
    private ManageProductionsView manageProductionsView;
    private boolean productionListFiltered;

    public CalvalusPortal2() {
        backendService = GWT.create(BackendService.class);
        productionListFiltered = true;
        idToViewMap = new HashMap<String, PortalView>();
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
         *
         * IMPORTANT NOTE: The Maps API key has been generated for the site http://www.brockmann-consult.de/calvalus
         * (see http://code.google.com/intl/de-DE/apis/maps/signup.html)
         */
        Runnable runnable = new Runnable() {
            public void run() {
                backendService.loadRegions(NO_FILTER, new InitRegionsCallback());
                backendService.getProductSets(NO_FILTER, new InitProductSetsCallback());

                final BundleFilter systemFilter = new BundleFilter();
                systemFilter.withProvider(BundleFilter.PROVIDER_SYSTEM);
                backendService.getProcessors(systemFilter.toString(), new InitProcessorsCallback(BundleFilter.PROVIDER_SYSTEM));

                final BundleFilter userFilter = new BundleFilter();
                userFilter.withProvider(BundleFilter.PROVIDER_USER);
                backendService.getProcessors(userFilter.toString(), new InitProcessorsCallback(BundleFilter.PROVIDER_USER));

                final BundleFilter allUserFilter = new BundleFilter();
                allUserFilter.withProvider(BundleFilter.PROVIDER_ALL_USERS);
                backendService.getProcessors(allUserFilter.toString(), new InitProcessorsCallback(BundleFilter.PROVIDER_ALL_USERS));

                backendService.getProductions(getProductionFilterString(), new InitProductionsCallback());
            }
        };
        // load all the libs for use in the maps
        ArrayList<LoadApi.LoadLibrary> loadLibraries = new ArrayList<LoadApi.LoadLibrary>();
        loadLibraries.add(LoadApi.LoadLibrary.DRAWING);
        loadLibraries.add(LoadApi.LoadLibrary.GEOMETRY);

        LoadApi.go(runnable, loadLibraries, false);
    }

    @Override
    public RegionMapModel getRegionMapModel() {
        return regionMapModel;
    }

    @Override
    public ListDataProvider<Region> getRegions() {
        return regions;
    }

    @Override
    public DtoProductSet[] getProductSets() {
        return productSets;
    }

    @Override
    public DtoProcessorDescriptor[] getProcessors(String filter) {
        if (filter.equals(BundleFilter.PROVIDER_SYSTEM)) {
            return systemProcessors;
        } else if (filter.equals(BundleFilter.PROVIDER_USER)) {
            return userProcessors;
        } else if (filter.equals(BundleFilter.PROVIDER_ALL_USERS)) {
            return allUserProcessors;
        }
        return new DtoProcessorDescriptor[0];
    }

    @Override
    public ListDataProvider<DtoProduction> getProductions() {
        return productions;
    }

    @Override
    public BackendServiceAsync getBackendService() {
        return backendService;
    }

    @Override
    public void showView(String id) {
        PortalView view = idToViewMap.get(id);
        viewPanel.showWidget(view.asWidget());
    }

    @Override
    public Timer getProductionsUpdateTimer() {
        return productionsUpdateTimer;
    }

    @Override
    public boolean isProductionListFiltered() {
        return productionListFiltered;
    }

    @Override
    public void setProductionListFiltered(boolean productionListFiltered) {
        if (productionListFiltered != this.productionListFiltered) {
            this.productionListFiltered = productionListFiltered;
            updateProductionList();
        }
    }

    private void maybeInitFrontend() {
        if (!initialised && isAllInputDataAvailable()) {
            initialised = true;
            initFrontend();
        }
    }

    private void initFrontend() {
        regionMapModel = new RegionMapModelImpl(getRegions());

        manageProductionsView = new ManageProductionsView(this);

        viewPanel = new DeckLayoutPanel();
        viewPanel.setSize("85em", "150em");  // todo
        viewPanel.setVisible(true);
        MenuBar mainMenuBar = new MenuBar();
        mainMenuBar.setAutoOpen(true);
        mainMenuBar.setAnimationEnabled(true);
        mainMenuBar.addItem("Order", createOrderMenu());
        mainMenuBar.addItem(createPortalViewMenuItem(manageProductionsView));

//        PortalView[] views = new PortalView[]{
//                new FrameView(this, "NewsView", "News", "calvalus-news.html"),
//                new OrderL2ProductionView(this),
//                new OrderMAProductionView(this),
//                new OrderL3ProductionView(this),
//                new OrderTAProductionView(this),
//                new OrderFreshmonProductionView(this),
//                new OrderBootstrappingView(this),
//                new ManageRegionsView(this),
//                new ManageBundleView(this),
//                manageProductionsView,
//        };


        removeSplashScreen();

        showView(OrderL2ProductionView.ID);
        VerticalPanel mainPanel = new VerticalPanel();
        mainPanel.add(mainMenuBar);
        mainPanel.add(viewPanel);
        mainPanel.ensureDebugId("mainPanel");
        showMainPanel(mainPanel);

        productionsUpdateTimer = new Timer() {
            @Override
            public void run() {
                updateProductionList();
            }
        };
    }

    private MenuBar createOrderMenu() {
        MenuBar orderMenu = new MenuBar(true);
        orderMenu.addItem(createPortalViewMenuItem(new OrderL2ProductionView(this)));
        orderMenu.addItem(createPortalViewMenuItem(new OrderMAProductionView(this)));
        orderMenu.addItem(createPortalViewMenuItem(new OrderL3ProductionView(this)));
        orderMenu.addItem(createPortalViewMenuItem(new OrderTAProductionView(this)));
        orderMenu.addSeparator();
        orderMenu.addItem(createPortalViewMenuItem(new OrderFreshmonProductionView(this)));
        orderMenu.addItem(createPortalViewMenuItem(new OrderBootstrappingView(this)));
        return orderMenu;
    }

    private MenuItem createPortalViewMenuItem(final PortalView portalView) {
        idToViewMap.put(portalView.getViewId(), portalView);
        viewPanel.add(portalView.asWidget());
        return new MenuItem(portalView.getTitle(), new Command() {
            @Override
            public void execute() {
                viewPanel.showWidget(portalView.asWidget());
            }
        });
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
               && systemProcessors != null && userProcessors != null && allUserProcessors != null
               && productions != null;
    }

    private synchronized void updateProductions(DtoProduction[] unknownProductions) {
        if (productions == null) {
            productions = new ListDataProvider<DtoProduction>();
            productionsMap = new HashMap<String, DtoProduction>();
        }
        boolean listChange = false;
        boolean propertyChange = false;
        ArrayList<DtoProduction> deletedProductions = new ArrayList<DtoProduction>(productions.getList());
        for (int i = 0; i < unknownProductions.length; i++) {
            DtoProduction unknownProduction = unknownProductions[i];
            DtoProduction knownProduction = productionsMap.get(unknownProduction.getId());
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
        for (DtoProduction deletedProduction : deletedProductions) {
            productions.getList().remove(deletedProduction);
            productionsMap.remove(deletedProduction.getId());
            listChange = true;
        }
        if (listChange) {
            if (manageProductionsView != null) {
                manageProductionsView.fireSortListEvent();
            }
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
//    @SuppressWarnings({"UnusedDeclaration"})
//    private CellTree createMainMenu() {
//        final SingleSelectionModel<PortalView> selectionModel = new SingleSelectionModel<PortalView>();
//        final MainMenuModel treeModel = new MainMenuModel(this, views, selectionModel);
//        // Create the cell tree.
//        CellTree mainMenu = new CellTree(treeModel, null);
//        mainMenu.setAnimationEnabled(true);
//        mainMenu.setKeyboardSelectionPolicy(HasKeyboardSelectionPolicy.KeyboardSelectionPolicy.DISABLED);
//        selectionModel.addSelectionChangeHandler(
//                new SelectionChangeEvent.Handler() {
//                    public void onSelectionChange(SelectionChangeEvent event) {
//                        PortalView selected = selectionModel.getSelectedObject();
//                        if (selected != null) {
//                            showView(selected.getViewId());
//                        }
//                    }
//                });
//        return mainMenu;
//    }

    private void updateProductionList() {
        backendService.getProductions(getProductionFilterString(), new UpdateProductionsCallback());
    }

    private String getProductionFilterString() {
        return BackendService.PARAM_NAME_CURRENT_USER_ONLY + "=" + isProductionListFiltered();
    }

    private class InitRegionsCallback implements AsyncCallback<DtoRegion[]> {

        @Override
        public void onSuccess(DtoRegion[] regions) {
            List<Region> regionList = RegionConverter.decodeRegions(regions);
            CalvalusPortal2.this.regions = new ListDataProvider<Region>(regionList);
            maybeInitFrontend();
        }

        @Override
        public void onFailure(Throwable caught) {
            caught.printStackTrace(System.err);
            Dialog.error("Server-side Error", caught.getMessage());
            CalvalusPortal2.this.regions = new ListDataProvider<Region>();
        }
    }

    private class InitProductSetsCallback implements AsyncCallback<DtoProductSet[]> {

        @Override
        public void onSuccess(DtoProductSet[] productSets) {
            CalvalusPortal2.this.productSets = productSets;
            maybeInitFrontend();
        }

        @Override
        public void onFailure(Throwable caught) {
            caught.printStackTrace(System.err);
            Dialog.error("Server-side Error", caught.getMessage());
            CalvalusPortal2.this.productSets = new DtoProductSet[0];
        }
    }

    private class InitProcessorsCallback implements AsyncCallback<DtoProcessorDescriptor[]> {

        private final String filter;

        public InitProcessorsCallback(String filter) {
            this.filter = filter;
        }

        @Override
        public void onSuccess(DtoProcessorDescriptor[] processors) {
            assign(processors);
            maybeInitFrontend();
        }

        @Override
        public void onFailure(Throwable caught) {
            caught.printStackTrace(System.err);
            Dialog.error("Server-side Error", caught.getMessage());
            assign(new DtoProcessorDescriptor[0]);
        }

        private void assign(DtoProcessorDescriptor[] processors) {
            if (filter.equals(BundleFilter.PROVIDER_SYSTEM)) {
                CalvalusPortal2.this.systemProcessors = processors;
            } else if (filter.equals(BundleFilter.PROVIDER_USER)) {
                CalvalusPortal2.this.userProcessors = processors;
            } else if (filter.equals(BundleFilter.PROVIDER_ALL_USERS)) {
                CalvalusPortal2.this.allUserProcessors = processors;
            }
        }
    }

    private class InitProductionsCallback implements AsyncCallback<DtoProduction[]> {

        @Override
        public void onSuccess(DtoProduction[] productions) {
            updateProductions(productions);
            maybeInitFrontend();
        }

        @Override
        public void onFailure(Throwable caught) {
            caught.printStackTrace(System.err);
            Dialog.error("Server-side Error", caught.getMessage());
            CalvalusPortal2.this.productions = new ListDataProvider<DtoProduction>();
        }
    }

    private class UpdateProductionsCallback implements AsyncCallback<DtoProduction[]> {

        @Override
        public void onSuccess(DtoProduction[] unknownProductions) {
            updateProductions(unknownProductions);
        }

        @Override
        public void onFailure(Throwable caught) {
            caught.printStackTrace(System.err);
            GWT.log("Failed to get productions from server", caught);
        }
    }
}
