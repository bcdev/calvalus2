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
import com.google.gwt.event.logical.shared.BeforeSelectionEvent;
import com.google.gwt.event.logical.shared.BeforeSelectionHandler;
import com.google.gwt.event.logical.shared.SelectionEvent;
import com.google.gwt.event.logical.shared.SelectionHandler;
import com.google.gwt.maps.client.LoadApi;
import com.google.gwt.user.cellview.client.CellTree;
import com.google.gwt.user.cellview.client.HasKeyboardSelectionPolicy;
import com.google.gwt.user.client.DOM;
import com.google.gwt.user.client.Timer;
import com.google.gwt.user.client.rpc.AsyncCallback;
import com.google.gwt.user.client.ui.DecoratedTabPanel;
import com.google.gwt.user.client.ui.RootPanel;
import com.google.gwt.user.client.ui.Widget;
import com.google.gwt.view.client.ListDataProvider;
import com.google.gwt.view.client.SelectionChangeEvent;
import com.google.gwt.view.client.SingleSelectionModel;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

/**
 * Entry point classes define <code>onModuleLoad()</code>.
 *
 * @author Norman
 */
public class CalvalusPortal implements EntryPoint, PortalContext {

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
    private PortalView[] views;
    private Map<String, Integer> viewTabIndices;
    private DecoratedTabPanel mainPanel;
    // A timer that periodically retrieves production statuses from server
    private Timer productionsUpdateTimer;
    private RegionMapModel regionMapModel;
    private ManageProductionsView manageProductionsView;
    private boolean productionListFiltered;
    private Boolean isCalvalusUser = null;
    private Boolean isCcUser = null;
    private Boolean isCalEsa = null;
    private Boolean isCalOpus = null;

    public boolean withPortalFeature(String featureName) {
        if ("analysistab".equals(featureName)) {
            return isCalvalusUser;
        } else if ("othersets".equals(featureName)) {
            return isCalvalusUser || isCalEsa || isCalOpus;
        } else if ("catalogue".equals(featureName)) {
            return isCalvalusUser;
        } else if ("unlimitedJobSize".equals(featureName)) {
            return isCalvalusUser || isCalEsa || isCalOpus;
        } else if ("coretab".equals(featureName)) {
            return isCalEsa || isCalOpus;
        } else {
            return false;
        }
    }

    public CalvalusPortal() {
        backendService = GWT.create(BackendService.class);
        productionListFiltered = true;
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

                GWT.log("checking for user roles asynchronously");
                backendService.isUserInRole("eop_file.modify_calopus_b", new UserRolesCallback("eop_file.modify_calopus_b"));
                backendService.isUserInRole("calesa", new UserRolesCallback("calesa"));
                backendService.isUserInRole("calvalus", new UserRolesCallback("calvalus"));
                backendService.isUserInRole("coastcolour", new UserRolesCallback("coastcolour"));
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
        Integer newViewIndex = viewTabIndices.get(id);
        mainPanel.selectTab(newViewIndex);
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
        if (withPortalFeature("analysistab")) {
            views = new PortalView[]{
                    new FrameView(this, "NewsView", "News", "calvalus-news.html"),
                    new OrderL2ProductionView(this),
                    new OrderMAProductionView(this),
                    new OrderL3ProductionView(this),
                    new OrderTAProductionView(this),
                    new OrderFreshmonProductionView(this),
                    new BootstrappingView(this),
                    new OrderVCProductionView(this),
                    new OrderMACProductionView(this),
                    new OrderL2toL3ProductionView(this),
                    new ManageRegionsView(this),
                    new ManageBundleView(this),
                    manageProductionsView,
            };
        } else if (withPortalFeature("coretab")) {
            views = new PortalView[]{
                    //new FrameView(this, "NewsView", "News", "calvalus-news.html"),
                    new OrderL2ProductionView(this),
                    new OrderMAProductionView(this),
                    new OrderL3ProductionView(this),
                    new OrderTAProductionView(this),
                    new ManageRegionsView(this),
                    new ManageBundleView(this),
                    manageProductionsView,
            };
        } else {
            views = new PortalView[]{
                    new FrameView(this, "NewsView", "News", "calvalus-news.html"),
                    new OrderL2ProductionView(this),
                    new OrderL3ProductionView(this),
                    new ManageRegionsView(this),
                    manageProductionsView,
            };
        }

        viewTabIndices = new HashMap<String, Integer>();
        for (int i = 0; i < views.length; i++) {
            viewTabIndices.put(views[i].getViewId(), i);
        }

        mainPanel = new DecoratedTabPanel();
        mainPanel.setAnimationEnabled(true);
        mainPanel.ensureDebugId("mainPanel");
        for (PortalView view : views) {
            mainPanel.add(view, view.getTitle());
        }

        mainPanel.addSelectionHandler(new SelectionHandler<Integer>() {
            @Override
            public void onSelection(SelectionEvent<Integer> integerSelectionEvent) {
                Integer tabIndex = integerSelectionEvent.getSelectedItem();
                if (tabIndex != null && tabIndex >= 0 && tabIndex < views.length) {
                    PortalView view = views[tabIndex];
                    GWT.log("Now showing: " + view.getTitle());
                    view.onShowing();
                }
            }
        });
        mainPanel.addBeforeSelectionHandler(new BeforeSelectionHandler<Integer>() {
            @Override
            public void onBeforeSelection(BeforeSelectionEvent<Integer> integerBeforeSelectionEvent) {
                int tabIndex = mainPanel.getTabBar().getSelectedTab();
                if (tabIndex >= 0 && tabIndex < views.length) {
                    PortalView view = views[tabIndex];
                    GWT.log("Now hidden: " + view.getTitle());
                    view.onHidden();
                }
            }
        });

        removeSplashScreen();

        showView(OrderL2ProductionView.ID);
        showMainPanel(mainPanel);

        productionsUpdateTimer = new Timer() {
            @Override
            public void run() {
                updateProductionList();
            }
        };
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
               && productions != null
               && isCcUser != null
               && isCalvalusUser != null;
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
            CalvalusPortal.this.regions = new ListDataProvider<Region>(regionList);
            maybeInitFrontend();
        }

        @Override
        public void onFailure(Throwable caught) {
            caught.printStackTrace(System.err);
            Dialog.error("Server-side Error", caught.getMessage());
            CalvalusPortal.this.regions = new ListDataProvider<Region>();
        }
    }

    private class InitProductSetsCallback implements AsyncCallback<DtoProductSet[]> {

        @Override
        public void onSuccess(DtoProductSet[] productSets) {
            CalvalusPortal.this.productSets = productSets;
            maybeInitFrontend();
        }

        @Override
        public void onFailure(Throwable caught) {
            caught.printStackTrace(System.err);
            Dialog.error("Server-side Error", caught.getMessage());
            CalvalusPortal.this.productSets = new DtoProductSet[0];
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
                CalvalusPortal.this.systemProcessors = processors;
            } else if (filter.equals(BundleFilter.PROVIDER_USER)) {
                CalvalusPortal.this.userProcessors = processors;
            } else if (filter.equals(BundleFilter.PROVIDER_ALL_USERS)) {
                CalvalusPortal.this.allUserProcessors = processors;
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
            CalvalusPortal.this.productions = new ListDataProvider<DtoProduction>();
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

    private class UserRolesCallback implements AsyncCallback<Boolean> {
        String role;
        public UserRolesCallback(String role) {
            this.role = role;
        }

        @Override
        public void onSuccess(Boolean value) {
            if ("calvalus".equals(role)) {
                isCalvalusUser = value;
                GWT.log("User role " + role + " is " + value);
            } else if ("coastcolour".equals(role)) {
                isCcUser = value;
                GWT.log("User role " + role + " is " + value);
            } else if ("calesa".equals(role)) {
                isCalEsa = value;
                GWT.log("User role " + role + " is " + value);
            } else if ("eop_file.modify_calopus_b".equals(role)) {
                isCalOpus = value;
                GWT.log("User role " + role + " is " + value);
            } else {
                GWT.log("Unknown user role " + role + " is " + value);
            }
            maybeInitFrontend();
        }

        @Override
        public void onFailure(Throwable caught) {
            caught.printStackTrace(System.err);
            GWT.log("Failed to check for user role " + role + " at server", caught);
        }
    }
}
