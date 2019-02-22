package com.bc.calvalus.portal.client;

import com.bc.calvalus.commons.shared.BundleFilter;
import com.bc.calvalus.portal.client.map.Region;
import com.bc.calvalus.portal.client.map.RegionConverter;
import com.bc.calvalus.portal.client.map.RegionMapModel;
import com.bc.calvalus.portal.client.map.RegionMapModelImpl;
import com.bc.calvalus.portal.shared.BackendService;
import com.bc.calvalus.portal.shared.BackendServiceAsync;
import com.bc.calvalus.portal.shared.ContextRetrievalService;
import com.bc.calvalus.portal.shared.ContextRetrievalServiceAsync;
import com.bc.calvalus.portal.shared.DtoAggregatorDescriptor;
import com.bc.calvalus.portal.shared.DtoCalvalusConfig;
import com.bc.calvalus.portal.shared.DtoColorPalette;
import com.bc.calvalus.portal.shared.DtoProcessorDescriptor;
import com.bc.calvalus.portal.shared.DtoProductSet;
import com.bc.calvalus.portal.shared.DtoProduction;
import com.bc.calvalus.portal.shared.DtoRegion;
import com.google.gwt.core.client.EntryPoint;
import com.google.gwt.core.client.GWT;
import com.google.gwt.dom.client.Element;
import com.google.gwt.maps.client.LoadApi;
import com.google.gwt.user.client.DOM;
import com.google.gwt.user.client.Timer;
import com.google.gwt.user.client.Window;
import com.google.gwt.user.client.rpc.AsyncCallback;
import com.google.gwt.user.client.ui.DeckLayoutPanel;
import com.google.gwt.user.client.ui.FlowPanel;
import com.google.gwt.user.client.ui.RootPanel;
import com.google.gwt.user.client.ui.Widget;
import com.google.gwt.view.client.ListDataProvider;
import com.google.gwt.view.client.SelectionChangeEvent;
import com.google.gwt.view.client.SingleSelectionModel;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.logging.Logger;

/**
 * Entry point classes define <code>onModuleLoad()</code>.
 *
 * @author Norman
 */
public class CalvalusPortal implements EntryPoint, PortalContext {

    public static final Logger LOG = Logger.getLogger("CalvalusPortal");

    public static final String NO_FILTER = "";
    private static final String[] VIEW_NAMES = {
                "newsView",
                "l2View",
                "maView",
                "raView",
                "l3View",
                "taView",
                "freshmonView",
                "bootstrappingView",
                "vicariousCalibrationView",
                "matchupComparisonView",
                "l2ToL3ComparisonView",
                "qlView",
                "regionsView",
                "requestsView",
                "bundlesView",
                "masksView",
                "productionsView"
    };

    private final BackendServiceAsync backendService;
    private final ContextRetrievalServiceAsync retrievalService;
    private boolean initialised;

    // Data provided by various external services
    private ListDataProvider<Region> regions;
    private DtoProductSet[] productSets;
    private DtoColorPalette[] colorPalettes;
    private DtoProcessorDescriptor[] systemProcessors;
    private DtoProcessorDescriptor[] userProcessors;
    private DtoProcessorDescriptor[] allUserProcessors;
    private DtoAggregatorDescriptor[] systemAggregators;
    private DtoAggregatorDescriptor[] userAggregators;
    private DtoAggregatorDescriptor[] allUserAggregators;
    private ListDataProvider<DtoProduction> productions;
    private Map<String, DtoProduction> productionsMap;
    // A timer that periodically retrieves production statuses from server
    private Timer productionsUpdateTimer;
    private RegionMapModel regionMapModel;
    private ManageProductionsView manageProductionsView;
    private boolean productionListFiltered;
    private Map<String, Boolean> calvalusConfig = null;
    private String[] queues = null;
    private MainMenu mainMenu;
    private PortalView currentView = null;
    private String userName;
    private Map<String, OrderProductionView> productionTypeViews;

    public boolean withPortalFeature(String featureName) {
        Boolean v = calvalusConfig.get(featureName);
        return v != null && v;
    }

    public String[] getRequestQueues() {
        return queues;
    }

    public CalvalusPortal() {
        backendService = GWT.create(BackendService.class);
        retrievalService = GWT.create(ContextRetrievalService.class);
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
                backendService.loadColorPalettes(NO_FILTER, new InitColorPaletteSetsCallback());
                backendService.getProductSets(NO_FILTER, new InitProductSetsCallback());

                final BundleFilter systemFilter = new BundleFilter();
                systemFilter.withProvider(BundleFilter.PROVIDER_SYSTEM);
                backendService.getProcessors(systemFilter.toString(),
                                             new InitProcessorsCallback(BundleFilter.PROVIDER_SYSTEM));
                backendService.getAggregators(systemFilter.toString(),
                                              new InitAggregatorsCallback(BundleFilter.PROVIDER_SYSTEM));

                final BundleFilter userFilter = new BundleFilter();
                userFilter.withProvider(BundleFilter.PROVIDER_USER);
                backendService.getProcessors(userFilter.toString(),
                                             new InitProcessorsCallback(BundleFilter.PROVIDER_USER));
                backendService.getAggregators(userFilter.toString(),
                                              new InitAggregatorsCallback(BundleFilter.PROVIDER_USER));

                final BundleFilter allUserFilter = new BundleFilter();
                allUserFilter.withProvider(BundleFilter.PROVIDER_ALL_USERS);
                backendService.getProcessors(allUserFilter.toString(),
                                             new InitProcessorsCallback(BundleFilter.PROVIDER_ALL_USERS));
                // aggregators from other users are currently not shown
                // backendService.getAggregators(allUserFilter.toString(), new InitAggregatorsCallback(BundleFilter.PROVIDER_ALL_USERS));
                backendService.getProductions(getProductionFilterString(), new InitProductionsCallback());

                GWT.log("checking for user roles asynchronously");
                backendService.getCalvalusConfig(new CalvalusConfigCallback());
            }
        };
        // load all the libs for use in the maps
        ArrayList<LoadApi.LoadLibrary> loadLibraries = new ArrayList<LoadApi.LoadLibrary>();
        loadLibraries.add(LoadApi.LoadLibrary.DRAWING);
        loadLibraries.add(LoadApi.LoadLibrary.GEOMETRY);


        //LoadApi.go(runnable, loadLibraries, false);
        // Google API key of martin.boettcher@brockmann-consult.de for test purposes
        LoadApi.go(runnable, loadLibraries, false, "key=AIzaSyDC6oUduMAdfWa48HkKSQyExtEGEWL2A2I");
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
    public DtoColorPalette[] getColorPalettes() {
        return colorPalettes;
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
    public DtoAggregatorDescriptor[] getAggregators(String filter) {
        if (filter.equals(BundleFilter.PROVIDER_SYSTEM)) {
            return systemAggregators;
        } else if (filter.equals(BundleFilter.PROVIDER_USER)) {
            return userAggregators;
        } else if (filter.equals(BundleFilter.PROVIDER_ALL_USERS)) {
            return allUserAggregators;
        }
        return new DtoAggregatorDescriptor[0];
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
    public ContextRetrievalServiceAsync getContextRetrievalService() {
        return retrievalService;
    }

    @Override
    public void showView(String id) {
        mainMenu.selectView(id);
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

    @Override
    public OrderProductionView getViewForRestore(String productionType) {
        return productionTypeViews.get(productionType);
    }

    @Override
    public String getUserName() {
        return userName;
    }

    private void maybeInitFrontend() {
        if (!initialised && isAllInputDataAvailable()) {
            initialised = true;
            initFrontend();
        }
    }

    private PortalView createViewOf(String name) {
        switch (name) {
        case "newsView":
            return new FrameView(this, "NewsView", "News", "calvalus-news.html");
        case "l2View":
            return new OrderL2ProductionView(this);
        case "maView":
            return new OrderMAProductionView(this);
        case "raView":
            return new OrderRAProductionView(this);
        case "l3View":
            return new OrderL3ProductionView(this);
        case "taView":
            return new OrderTAProductionView(this);
        case "freshmonView":
            return new OrderFreshmonProductionView(this);
        case "bootstrappingView":
            return new OrderBootstrappingView(this);
        case "vicariousCalibrationView":
            return new OrderVCProductionView(this);
        case "matchupComparisonView":
            return new OrderMACProductionView(this);
        case "l2ToL3ComparisonView":
            return new OrderL2toL3ProductionView(this);
        case "qlView":
            return new OrderQLProductionView(this);
        case "regionsView":
            return new ManageRegionsView(this);
        case "bundlesView":
            return new ManageBundleView(this);
        case "requestsView":
            return new ManageRequestView(this);
        case "masksView":
            return new ManageMasksView(this);
        case "productionsView":
            manageProductionsView = new ManageProductionsView(this);
            return manageProductionsView;
        case "emptyView":
            return new FrameView(this, "EmptyView", "Empty", "empty.html");
        default:
            throw new RuntimeException("unknown view " + name);
        }
    }

    private void initFrontend() {
        regionMapModel = new RegionMapModelImpl(getRegions());
        productionTypeViews = new HashMap<>();
        List<PortalView> views = new ArrayList<>();
        for (String viewName : VIEW_NAMES) {
            if (withPortalFeature(viewName)) {
                PortalView portalView = createViewOf(viewName);
                views.add(portalView);
                if (portalView instanceof OrderProductionView) {
                    OrderProductionView orderProductionView = (OrderProductionView) portalView;
                    if (orderProductionView.isRestoringRequestPossible()) {
                        String productionType = orderProductionView.getProductionType();
                        productionTypeViews.put(productionType, orderProductionView);
                    }
                }
            }
        }
        if (views.isEmpty()) {
            views.add(createViewOf("emptyView"));
        }
        mainMenu = new MainMenu(views);

        DeckLayoutPanel viewPanel = new DeckLayoutPanel();
        viewPanel.getElement().setClassName("view-container");
        viewPanel.setVisible(true);
        for (PortalView view : views) {
            viewPanel.add(view.asWidget());
        }

        Element childElement = viewPanel.getElement().getFirstChildElement();
        while (childElement != null) {
            childElement.getStyle().setProperty("overflow", "auto");
            childElement = childElement.getNextSiblingElement();
        }

        FlowPanel mainPanel = new FlowPanel();
        mainPanel.add(mainMenu.getWidget());
        mainPanel.add(viewPanel);
        mainPanel.ensureDebugId("mainPanel");

        Element mainMenuElement = mainMenu.getWidget().getElement();
        mainMenuElement.setClassName("main-menu");
        mainMenuElement.getParentElement().setClassName("main-panel-sub");

        SingleSelectionModel<PortalView> selectionModel = mainMenu.getSelectionModel();
        selectionModel.addSelectionChangeHandler(
                    new SelectionChangeEvent.Handler() {
                        public void onSelectionChange(SelectionChangeEvent event) {
                            final PortalView selected = selectionModel.getSelectedObject();
                            if (selected != null) {
                                if (currentView != null) {
                                    GWT.log("Now hiding: " + currentView.getTitle());
                                    currentView.onHidden();
                                }
                                currentView = selected;
                                GWT.log("Now showing: " + selected.getTitle());
                                viewPanel.showWidget(selected.asWidget());
                                Window.scrollTo(0, 0);
                                selected.onShowing();
                            }
                        }
                    });

        removeSplashScreen();

        mainMenu.showAndSelectFirstView();

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
        RootPanel.getBodyElement().removeChild(DOM.getElementById("splashScreen"));
    }

    private boolean isAllInputDataAvailable() {
        return regions != null
               && productSets != null
               && systemProcessors != null && userProcessors != null && allUserProcessors != null
               && systemAggregators != null && userAggregators != null
               && productions != null
               && calvalusConfig != null;
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

    private class InitColorPaletteSetsCallback implements AsyncCallback<DtoColorPalette[]> {

        @Override
        public void onSuccess(DtoColorPalette[] dtoColorPalettes) {
            CalvalusPortal.this.colorPalettes = dtoColorPalettes;
            maybeInitFrontend();
        }

        @Override
        public void onFailure(Throwable caught) {
            caught.printStackTrace(System.err);
            Dialog.error("Server-side Error", caught.getMessage());
            CalvalusPortal.this.colorPalettes = new DtoColorPalette[0];
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
            Arrays.sort(processors, (o1, o2) -> o1.getDisplayText().compareToIgnoreCase(o2.getDisplayText()));
            if (filter.equals(BundleFilter.PROVIDER_SYSTEM)) {
                CalvalusPortal.this.systemProcessors = processors;
            } else if (filter.equals(BundleFilter.PROVIDER_USER)) {
                CalvalusPortal.this.userProcessors = processors;
            } else if (filter.equals(BundleFilter.PROVIDER_ALL_USERS)) {
                CalvalusPortal.this.allUserProcessors = processors;
            }
        }
    }

    private class InitAggregatorsCallback implements AsyncCallback<DtoAggregatorDescriptor[]> {

        private final String filter;

        public InitAggregatorsCallback(String filter) {
            this.filter = filter;
        }

        @Override
        public void onSuccess(DtoAggregatorDescriptor[] aggregators) {
            assign(aggregators);
            maybeInitFrontend();
        }

        @Override
        public void onFailure(Throwable caught) {
            caught.printStackTrace(System.err);
            Dialog.error("Server-side Error", caught.getMessage());
            assign(new DtoAggregatorDescriptor[0]);
        }

        private void assign(DtoAggregatorDescriptor[] aggregators) {
            if (filter.equals(BundleFilter.PROVIDER_SYSTEM)) {
                CalvalusPortal.this.systemAggregators = aggregators;
            } else if (filter.equals(BundleFilter.PROVIDER_USER)) {
                CalvalusPortal.this.userAggregators = aggregators;
            } else if (filter.equals(BundleFilter.PROVIDER_ALL_USERS)) {
                CalvalusPortal.this.allUserAggregators = aggregators;
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

    private class CalvalusConfigCallback implements AsyncCallback<DtoCalvalusConfig> {

        public CalvalusConfigCallback() {
        }

        @Override
        public void onSuccess(DtoCalvalusConfig config) {
            calvalusConfig = new HashMap<>();
            List<String> queueList = new ArrayList<>();
            userName = config.getUser();
            if (config.getConfig().containsKey("calvalus.hadoop.mapreduce.job.queuename")) {
                queueList.add(config.getConfig().get("calvalus.hadoop.mapreduce.job.queuename"));
            }
            for (String key : config.getConfig().keySet()) {
                if (key.startsWith("calvalus.portal.")) {
                    calvalusConfig.put(key.substring("calvalus.portal.".length()),
                                       roleSupports(key, config.getRoles(), config.getConfig()));
                } else if (key.startsWith("calvalus.queue.")
                           && contains(config.getRoles(), key.substring("calvalus.queue.".length()))) {
                    for (String queue : config.getConfig().get(key).split(" ")) {
                        if (!queueList.contains(queue)) {
                            queueList.add(queue);
                        }
                    }
                }
            }
            if (queueList.size() > 0) {
                queues = queueList.toArray(new String[queueList.size()]);
            }
            maybeInitFrontend();
        }

        @Override
        public void onFailure(Throwable caught) {
            caught.printStackTrace(System.err);
            GWT.log("Failed to read Calvalus config from server", caught);
        }
    }

    private void addQueues(String[] split) {
    }

    private static boolean contains(String[] roles, String role) {
        for (String r : roles) {
            if (r.equals(role)) {
                return true;
            }
        }
        return false;
    }

    private static boolean roleSupports(String viewName, String[] roles, Map<String, String> config) {
        final String viewRoles = config.get(viewName);
        // for debugging
        final StringBuilder s = new StringBuilder();
        for (String r : roles) {
            s.append(r);
            s.append(' ');
        }
        s.setLength(s.length() - 1);
        // end for debugging
        if (viewRoles != null) {
            for (String configuredRole : viewRoles.split(" ")) {
                for (String userRole : roles) {
                    if (userRole.equals(configuredRole)) {
                        LOG.fine("prop " + viewName + " (" + viewRoles + ") supported by role " + userRole + " (" + s.toString() + ")");
                        return true;
                    }
                }
            }
        }
        LOG.fine("prop " + viewName + " (" + viewRoles + ") not supported by any role (" + s.toString() + ")");
        return false;
    }
}
