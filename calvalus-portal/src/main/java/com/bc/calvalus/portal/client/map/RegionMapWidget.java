package com.bc.calvalus.portal.client.map;

import com.bc.calvalus.portal.client.map.actions.DeleteRegionsAction;
import com.bc.calvalus.portal.client.map.actions.EditVerticesAction;
import com.bc.calvalus.portal.client.map.actions.InsertBoxInteraction;
import com.bc.calvalus.portal.client.map.actions.InsertPolygonInteraction;
import com.bc.calvalus.portal.client.map.actions.LocateRegionsAction;
import com.bc.calvalus.portal.client.map.actions.RenameRegionAction;
import com.bc.calvalus.portal.client.map.actions.SelectInteraction;
import com.bc.calvalus.portal.client.map.actions.ShowRegionInfoAction;
import com.google.gwt.cell.client.AbstractCell;
import com.google.gwt.cell.client.Cell;
import com.google.gwt.core.client.GWT;
import com.google.gwt.dom.client.Style;
import com.google.gwt.event.shared.HandlerRegistration;
import com.google.gwt.maps.client.MapOptions;
import com.google.gwt.maps.client.MapWidget;
import com.google.gwt.maps.client.base.LatLng;
import com.google.gwt.maps.client.drawinglib.DrawingManager;
import com.google.gwt.maps.client.drawinglib.DrawingManagerOptions;
import com.google.gwt.maps.client.events.click.ClickMapEvent;
import com.google.gwt.maps.client.events.click.ClickMapHandler;
import com.google.gwt.maps.client.events.insertat.InsertAtMapEvent;
import com.google.gwt.maps.client.events.insertat.InsertAtMapHandler;
import com.google.gwt.maps.client.events.removeat.RemoveAtMapEvent;
import com.google.gwt.maps.client.events.removeat.RemoveAtMapHandler;
import com.google.gwt.maps.client.events.setat.SetAtMapEvent;
import com.google.gwt.maps.client.events.setat.SetAtMapHandler;
import com.google.gwt.maps.client.mvc.MVCArray;
import com.google.gwt.maps.client.overlays.Polygon;
import com.google.gwt.maps.client.overlays.PolygonOptions;
import com.google.gwt.resources.client.ClientBundle;
import com.google.gwt.resources.client.ImageResource;
import com.google.gwt.safehtml.shared.SafeHtmlBuilder;
import com.google.gwt.user.cellview.client.CellTree;
import com.google.gwt.user.client.ui.DockLayoutPanel;
import com.google.gwt.user.client.ui.Image;
import com.google.gwt.user.client.ui.ResizeComposite;
import com.google.gwt.user.client.ui.ScrollPanel;
import com.google.gwt.user.client.ui.SplitLayoutPanel;
import com.google.gwt.view.client.ListDataProvider;
import com.google.gwt.view.client.ProvidesKey;
import com.google.gwt.view.client.SelectionChangeEvent;
import com.google.gwt.view.client.SelectionModel;
import com.google.gwt.view.client.TreeViewModel;

import java.util.HashMap;
import java.util.List;
import java.util.Map;

/**
 * An implementation of a Google map that has regions.
 *
 * @author Norman Fomferra
 */
public class RegionMapWidget extends ResizeComposite implements RegionMap, ClickMapHandler {

    private final RegionMapModel regionMapModel;
    private final RegionMapSelectionModel regionMapSelectionModel;
    private MapWidget mapWidget;
    private boolean adjustingRegionSelection;
    private MapInteraction currentInteraction;

    private boolean editable;
    private final MapAction[] actions;
    private Map<Region, Polygon> polygonMap;
    private Map<Polygon, Region> regionMap;
    private Map<Region, HandlerRegistration> handlerRegistrationMap;

    private PolygonOptions normalPolyStyle;
    private PolygonOptions selectedPolyStyle;
    private RegionMapToolbar regionMapToolbar;
    private CellTree regionCellTree;
    private HandlerRegistration insertHandlerRegistration;
    private HandlerRegistration removeHandlerRegistration;
    private HandlerRegistration setHandlerRegistration;

    public RegionMapWidget(RegionMapModel regionMapModel, boolean editable, MapAction... actions) {
        this(regionMapModel, new RegionMapSelectionModelImpl(), editable, actions);
    }

    public RegionMapWidget(RegionMapModel regionMapModel,
                           RegionMapSelectionModel regionMapSelectionModel,
                           boolean editable,
                           MapAction... actions) {
        this.regionMapModel = regionMapModel;
        this.regionMapSelectionModel = regionMapSelectionModel;
        this.handlerRegistrationMap = new HashMap<Region, HandlerRegistration>();
        this.editable = editable;
        this.actions = actions;

        this.normalPolyStyle = PolygonOptions.newInstance();
        this.normalPolyStyle.setStrokeColor("#0000FF");
        this.normalPolyStyle.setStrokeWeight(3);
        this.normalPolyStyle.setStrokeOpacity(0.8);
        this.normalPolyStyle.setFillColor("#0000FF");
        this.normalPolyStyle.setFillOpacity(0.2);

        this.selectedPolyStyle = PolygonOptions.newInstance();
        this.selectedPolyStyle.setStrokeColor("#FFFF00");
        this.selectedPolyStyle.setStrokeWeight(3);
        this.selectedPolyStyle.setStrokeOpacity(0.8);
        this.selectedPolyStyle.setFillColor("#0000FF");
        this.selectedPolyStyle.setFillOpacity(0.2);

        polygonMap = new HashMap<Region, Polygon>();
        regionMap = new HashMap<Polygon, Region>();
        initUi();
    }

    @Override
    public RegionMapModel getRegionModel() {
        return regionMapModel;
    }

    @Override
    public RegionMapSelectionModel getRegionMapSelectionModel() {
        return regionMapSelectionModel;
    }

    @Override
    public MapWidget getMapWidget() {
        return mapWidget;
    }

    @Override
    public Polygon getPolygon(Region region) {
        return polygonMap.get(region);
    }

    @Override
    public Region getRegion(String qualifiedName) {
        List<Region> regionList = getRegionModel().getRegionProvider().getList();
        for (Region region : regionList) {
            if (region.getQualifiedName().equalsIgnoreCase(qualifiedName)) {
                return region;
            }
        }
        return null;
    }

    @Override
    public Region getRegion(Polygon polygon) {
        return regionMap.get(polygon);
    }

    @Override
    public void addRegion(Region region) {
        if (!getRegionModel().getRegionProvider().getList().contains(region)) {
            getRegionModel().getRegionProvider().getList().add(0, region);
            getRegionMapSelectionModel().clearSelection();
            getRegionMapSelectionModel().setSelected(region, true);
            getRegionModel().fireRegionAdded(this, region);
        }
    }

    @Override
    public void removeRegion(Region region) {
        if (getRegionModel().getRegionProvider().getList().remove(region)) {
            getRegionMapSelectionModel().setSelected(region, false);
            getRegionModel().fireRegionRemoved(this, region);
        }
    }

    @Override
    public MapAction[] getActions() {
        return actions;
    }

    @Override
    public MapInteraction getCurrentInteraction() {
        return currentInteraction;
    }

    @Override
    public void setCurrentInteraction(MapInteraction interaction) {
        if (currentInteraction != interaction) {
            if (currentInteraction != null) {
                currentInteraction.detachFrom(this);
                if (regionMapToolbar != null) {
                    regionMapToolbar.deselect(currentInteraction);
                }
            }
            currentInteraction = interaction;
            if (currentInteraction != null) {
                currentInteraction.attachTo(this);
                if (regionMapToolbar != null) {
                    regionMapToolbar.select(currentInteraction);
                }
            }
        }
    }

    @Override
    public void onEvent(ClickMapEvent event) {
        getRegionMapSelectionModel().clearSelection();
    }

    interface SpatialFilterTreeResource extends CellTree.Resources {

        @Source("com/bc/calvalus/portal/client/spatialFilterCellTreeStyle.css")
        SpatialFilterTreeStyle cellTreeStyle();
    }

    interface SpatialFilterTreeStyle extends CellTree.Style {}

    private void initUi() {
        MapOptions mapOptions = MapOptions.newInstance();
        mapOptions.setCenter(LatLng.newInstance(0.0, 0.0));
        mapOptions.setZoom(2);
        mapOptions.setDisableDoubleClickZoom(false);
        mapOptions.setScrollWheel(true);
        mapOptions.setMapTypeControl(true);
        mapOptions.setZoomControl(false);
        mapOptions.setPanControl(false);
        mapOptions.setStreetViewControl(false);
        mapWidget = new MapWidget(mapOptions);
        mapWidget.addClickHandler(this);

        CellTree.Resources cellTreeRes = GWT.create(SpatialFilterTreeResource.class);
//        final SelectionModel<Region> regionSelectionModel = new MultiSelectionModel<Region>(Region.KEY_PROVIDER);
        RegionTreeSelectionModel treeNodeSingleSelectionModel = new RegionTreeSelectionModel();
        RegionTreeViewModel treeViewModel = new RegionTreeViewModel(regionMapModel, treeNodeSingleSelectionModel);
        regionCellTree = new CellTree(treeViewModel, null, cellTreeRes);
        regionCellTree.setAnimationEnabled(true);
        regionCellTree.setDefaultNodeSize(100);

        DockLayoutPanel regionPanel = new DockLayoutPanel(Style.Unit.EM);
        regionPanel.ensureDebugId("regionPanel");
        if (actions.length > 0) {
            regionMapToolbar = new RegionMapToolbar(this);
            regionPanel.addSouth(regionMapToolbar, /*3.5*/6.0);
        }
        regionPanel.add(new ScrollPanel(regionCellTree));

        SplitLayoutPanel regionSplitLayoutPanel = new SplitLayoutPanel();
        regionSplitLayoutPanel.ensureDebugId("regionSplitLayoutPanel");
        regionSplitLayoutPanel.addWest(regionPanel, 180);
        regionSplitLayoutPanel.add(mapWidget);

        if (getCurrentInteraction() == null) {
            setCurrentInteraction(createSelectInteraction());
        }

        updatePolygonStyles();
        initWidget(regionSplitLayoutPanel);
        bind(treeNodeSingleSelectionModel, treeViewModel);
    }

    private void bind(final RegionTreeSelectionModel regionTreeSelectionModel,
                      final RegionTreeViewModel treeViewModel) {
        getRegionModel().addChangeListener(new RegionMapModel.ChangeListener() {

            @Override
            public void onRegionAdded(RegionMapModel.ChangeEvent event) {
                ensurePolygonPresent(event.getRegion());
            }

            @Override
            public void onRegionRemoved(RegionMapModel.ChangeEvent event) {
                ensurePolygonAbsent(event.getRegion());
            }

            @Override
            public void onRegionChanged(RegionMapModel.ChangeEvent event) {
                if (event.getRegionMap() != RegionMapWidget.this) {
                    ensurePolygonAbsent(event.getRegion());
                    ensurePolygonPresent(event.getRegion());
                }
            }
        });
        regionTreeSelectionModel.addSelectionChangeHandler(new SelectionChangeEvent.Handler() {
            @Override
            public void onSelectionChange(SelectionChangeEvent event) {
                if (!adjustingRegionSelection) {
                    try {
                        adjustingRegionSelection = true;
                        RegionMapSelectionModel regionMapSelectionModel = getRegionMapSelectionModel();
                        updateRegionSelectionInMap(regionTreeSelectionModel, regionMapSelectionModel);
                        updatePolygonStyles();
                        if (!editable && regionMapSelectionModel.getSelectedRegion() != null) {
                            new LocateRegionsAction().run(RegionMapWidget.this);
                        }
                    } finally {
                        adjustingRegionSelection = false;
                    }
                }
            }
        });

        getRegionMapSelectionModel().addSelectionChangeHandler(new SelectionChangeEvent.Handler() {
            @Override
            public void onSelectionChange(SelectionChangeEvent event) {
                if (!adjustingRegionSelection) {
                    try {
                        adjustingRegionSelection = true;
                        updateRegionSelectionInTree(getRegionMapSelectionModel(), regionTreeSelectionModel,
                                                    treeViewModel);
                        updatePolygonStyles();
                        // todo - scroll to selected region in regionTreeList (nf,mz.mp)
                    } finally {
                        adjustingRegionSelection = false;
                    }
                }
            }
        });
    }

    private Polygon ensurePolygonPresent(Region region) {
        Polygon polygon = polygonMap.get(region);
        if (polygon == null) {
            polygon = region.createPolygon();
            polygon.setVisible(true);
            regionMap.put(polygon, region);
            polygonMap.put(region, polygon);
            polygon.setMap(mapWidget);
            handlerRegistrationMap.put(region, polygon.addClickHandler(new RegionClickMapHandler(region)));
            updatePolygonStyle(region, polygon, regionMapSelectionModel.isSelected(region));
        }
        return polygon;
    }

    private Polygon ensurePolygonAbsent(Region region) {
        Polygon polygon = polygonMap.get(region);
        if (polygon != null) {
            polygon.setMap(null);
            HandlerRegistration handlerRegistration = handlerRegistrationMap.get(region);
            if (handlerRegistration != null) {
                handlerRegistrationMap.remove(region);
                handlerRegistration.removeHandler();
            }
            regionMap.remove(polygon);
            polygonMap.remove(region);
        }
        return polygon;
    }

    private void updatePolygonStyles() {
        List<Region> regionList = regionMapModel.getRegionProvider().getList();
        for (Region region : regionList) {
            if (!regionMapSelectionModel.isSelected(region)) {
                updatePolygonStyle(region, ensurePolygonPresent(region), false);
            }
        }
        Region selectedRegion = regionMapSelectionModel.getSelectedRegion();
        if (selectedRegion != null) {
            updatePolygonStyle(selectedRegion, ensurePolygonPresent(selectedRegion), true);
        }
    }

    private void updatePolygonStyle(Region region, Polygon polygon, boolean selected) {
        polygon.setOptions(selected ? selectedPolyStyle : normalPolyStyle);
        if (editable && region.isUserRegion()) {
            polygon.setEditable(selected);
            if (selected) {
                MVCArray<LatLng> polygonPath = polygon.getPath();
                insertHandlerRegistration = polygonPath.addInsertAtHandler(new MyInsertAtMapHandler(region));
                removeHandlerRegistration = polygonPath.addRemoveAtHandler(new MyRemoveAtMapHandler(region));
                setHandlerRegistration = polygonPath.addSetAtHandler(new MySetAtMapHandler(region));
            } else {
                if (insertHandlerRegistration != null) {
                    insertHandlerRegistration.removeHandler();
                }
                if (removeHandlerRegistration != null) {
                    removeHandlerRegistration.removeHandler();
                }
                if (setHandlerRegistration != null) {
                    setHandlerRegistration.removeHandler();
                }
            }
        }
    }

    private void updateRegionSelectionInTree(RegionMapSelectionModel source,
                                             RegionTreeSelectionModel target,
                                             RegionTreeViewModel treeViewModel) {
        Region selectedRegion = source.getSelectedRegion();
        target.clearSelection();
        TreeNode nodeToSelect = findNodeForRegion(selectedRegion, treeViewModel.getRootNode(), false);
        if (nodeToSelect != null) {
            target.setSelected(nodeToSelect, true);
        }
    }

    private static TreeNode findNodeForRegion(Region region, GroupNode rootNode, boolean returnParent) {
        for (TreeNode childNode : rootNode.getChildNodes().getList()) {
            if (childNode instanceof LeafNode) {
                LeafNode leafNode = (LeafNode) childNode;
                if (leafNode.getRegion() == region) {
                    if (returnParent) {
                        return rootNode;
                    } else {
                        return leafNode;
                    }
                }
            } else if (childNode instanceof GroupNode) {
                TreeNode nodeForRegion = findNodeForRegion(region, (GroupNode) childNode, returnParent);
                if (nodeForRegion != null) {
                    return nodeForRegion;
                }
            }
        }
        return null;
    }

    private void updateRegionSelectionInMap(RegionTreeSelectionModel source,
                                            RegionMapSelectionModel target) {
        target.clearSelection();

        LeafNode selectedLeafNode = source.getSelectedLeafNode();
        if (selectedLeafNode != null) {
            target.setSelected(selectedLeafNode.getRegion(), true);
        }
    }

    interface Icons extends ClientBundle {
        @ClientBundle.Source("actions/RegionPolygon24.gif")
        ImageResource getPolygonIcon();
        @ClientBundle.Source("actions/RegionBBox24.gif")
        ImageResource getBBoxIcon();
        @ClientBundle.Source("actions/RegionSelect24.gif")
        ImageResource getSelectIcon();
    }


    public static MapAction[] createDefaultEditingActions() {
        final SelectInteraction selectInteraction = createSelectInteraction();
        DrawingManagerOptions options = DrawingManagerOptions.newInstance();
        options.setDrawingControl(false);
        DrawingManager drawingManager = DrawingManager.newInstance(options);
        drawingManager.setDrawingMode(null);
        return new MapAction[] {
                selectInteraction,
                new LocateRegionsAction(),
                new ShowRegionInfoAction(),
                new DeleteRegionsAction(),
                MapAction.SEPARATOR,
                new InsertPolygonInteraction(drawingManager, new AbstractMapAction("P", new Image(((Icons) GWT.create(Icons.class)).getPolygonIcon()), "New polygon region") {
                    @Override
                    public void run(RegionMap regionMap) {
                        regionMap.setCurrentInteraction(selectInteraction);
                    }
                }),
                new InsertBoxInteraction(drawingManager, new AbstractMapAction("B", new Image(((Icons) GWT.create(Icons.class)).getBBoxIcon()), "New box region") {
                    @Override
                    public void run(RegionMap regionMap) {
                        regionMap.setCurrentInteraction(selectInteraction);
                    }
                }),
                new EditVerticesAction(),
                new RenameRegionAction()
        };
    }

    private static SelectInteraction createSelectInteraction() {
        return new SelectInteraction(new AbstractMapAction("S", new Image(((Icons) GWT.create(Icons.class)).getSelectIcon()), "Select region") {
            @Override
            public void run(RegionMap regionMap) {
            }
        });
    }

    // todo - how can we enable / disable stuff? (nf)
    public boolean isEnabled() {
        return true;
    }

    public void setEnabled(boolean value) {
        mapWidget.setVisible(value);
        regionCellTree.setVisible(value);
    }

    private class RegionClickMapHandler implements ClickMapHandler {

        private final Region region;

        private RegionClickMapHandler(Region region) {
            this.region = region;
        }

        @Override
        public void onEvent(ClickMapEvent event) {
            getRegionMapSelectionModel().clearSelection();
            getRegionMapSelectionModel().setSelected(region, true);
        }
    }

    private abstract class RegionChangeHandler {

        private final Region region;

        private RegionChangeHandler(Region region) {
            this.region = region;
        }

        protected void handleRegionChangeEvent() {
             Polygon polygon = polygonMap.get(region);
             region.setVertices(Region.getVertices(polygon));
             getRegionModel().fireRegionChanged(RegionMapWidget.this, region);
         }
    }

    private class MyInsertAtMapHandler extends  RegionChangeHandler implements InsertAtMapHandler {

        private MyInsertAtMapHandler(Region region) {
            super(region);
        }

        @Override
        public void onEvent(InsertAtMapEvent event) {
            handleRegionChangeEvent();
        }
    }

    private class MySetAtMapHandler extends  RegionChangeHandler implements SetAtMapHandler {

        private MySetAtMapHandler(Region region) {
            super(region);
        }

        @Override
        public void onEvent(SetAtMapEvent event) {
            handleRegionChangeEvent();
        }
    }

    private class MyRemoveAtMapHandler extends  RegionChangeHandler implements RemoveAtMapHandler {

        private MyRemoveAtMapHandler(Region region) {
            super(region);
        }

        @Override
        public void onEvent(RemoveAtMapEvent event) {
            handleRegionChangeEvent();
        }
    }

    private static class RegionTreeSelectionModel extends SelectionModel.AbstractSelectionModel<TreeNode> {

        private LeafNode selectedNode;

        public RegionTreeSelectionModel() {
            super(RegionMapWidget.KEY_PROVIDER);
        }

        @Override
        public boolean isSelected(TreeNode treeNode) {
            return selectedNode == treeNode;
        }

        @Override
        public void setSelected(TreeNode treeNode, boolean selected) {
            if (treeNode instanceof LeafNode) {
                if (selected) {
                    if (selectedNode != treeNode) {
                        selectedNode = (LeafNode) treeNode;
                        fireSelectionChangeEvent();
                    }
                } else {
                    if (selectedNode == treeNode) {
                        selectedNode = null;
                        fireSelectionChangeEvent();
                    }
                }
            }
        }

        public void clearSelection() {
            if (selectedNode != null) {
                selectedNode = null;
                fireSelectionChangeEvent();
            }
        }

        public LeafNode getSelectedLeafNode() {
            return selectedNode;
        }
    }

    private static class RegionTreeViewModel implements TreeViewModel {

        private final Cell<TreeNode> cell;
        private final SelectionModel<TreeNode> treeSelectionModel;
        private final GroupNode rootNode = new GroupNode("");

        public RegionTreeViewModel(RegionMapModel regionMapModel, RegionTreeSelectionModel regionTreeSelectionModel) {
            this.treeSelectionModel = regionTreeSelectionModel;
            for (Region region : regionMapModel.getRegionProvider().getList()) {
                addRegion(rootNode, region);
            }

            regionMapModel.addChangeListener(new RegionMapModel.ChangeListener() {
                @Override
                public void onRegionAdded(RegionMapModel.ChangeEvent event) {
                    addRegion(rootNode, event.getRegion());
                }

                @Override
                public void onRegionRemoved(RegionMapModel.ChangeEvent event) {
                    Region region = event.getRegion();
                    TreeNode parentNodeForRegion = findNodeForRegion(region, rootNode, true);
                    if (parentNodeForRegion instanceof GroupNode) {
                        GroupNode parentNode = (GroupNode) parentNodeForRegion;
                        List<TreeNode> nodeList = parentNode.getChildNodes().getList();
                        for (TreeNode childNode : nodeList) {
                            if (childNode instanceof LeafNode) {
                                LeafNode node = (LeafNode) childNode;
                                if (node.region == region) {
                                    nodeList.remove(childNode);
                                    parentNode.getChildNodes().refresh();
                                    break;
                                }
                            }
                        }
                    }
                }

                @Override
                public void onRegionChanged(RegionMapModel.ChangeEvent event) {
                    Region region = event.getRegion();
                    TreeNode parentNodeForRegion = findNodeForRegion(region, rootNode, true);
                    if (parentNodeForRegion instanceof GroupNode) {
                        GroupNode parentNode = (GroupNode) parentNodeForRegion;
                        parentNode.getChildNodes().refresh();
                    }
                }
            });
            cell = new AbstractCell<TreeNode>() {
                @Override
                public void render(Context context, TreeNode value, SafeHtmlBuilder sb) {
                    sb.appendHtmlConstant(value.getName());
                }
            };
        }

        public GroupNode getRootNode() {
            return rootNode;
        }

        private static void addRegion(GroupNode rootNode, Region region) {
            GroupNode currentNode = rootNode;
            for (String pathElem : region.getPath()) {
                GroupNode childNode = null;
                for (TreeNode aNode : currentNode.getChildNodes().getList()) {
                    if (aNode.getName().equals(pathElem) && aNode instanceof GroupNode) {
                        childNode = (GroupNode) aNode;
                        break;
                    }
                }
                if (childNode == null) {
                    childNode = new GroupNode(pathElem);
                    currentNode.getChildNodes().getList().add(childNode);
                    currentNode.getChildNodes().refresh();
                }
                currentNode = childNode;
            }
            currentNode.getChildNodes().getList().add(new LeafNode(region));
            currentNode.getChildNodes().refresh();
        }

        @Override
        public <T> NodeInfo<?> getNodeInfo(T value) {
            ListDataProvider<TreeNode> dataProvider = null;
            if (value == null) {
                dataProvider = rootNode.getChildNodes();
            } else if (value instanceof GroupNode) {
                dataProvider = ((GroupNode) value).getChildNodes();
            }
            if (dataProvider != null) {
                return new DefaultNodeInfo<TreeNode>(dataProvider, cell, treeSelectionModel, null);
            }
            return null;
        }

        @Override
        public boolean isLeaf(Object value) {
            return value instanceof LeafNode;
        }

    }

    public static final ProvidesKey<TreeNode> KEY_PROVIDER = new ProvidesKey<TreeNode>() {
        @Override
        public Object getKey(TreeNode treeNode) {
            return treeNode;
        }
    };

    private static interface TreeNode {
        String getName();
    }

    private static class GroupNode implements TreeNode {

        private final String name;
        private final ListDataProvider<TreeNode> childNodes;

        private GroupNode(String name) {
            this.name = name;
            childNodes = new ListDataProvider<TreeNode>();
        }

        @Override
        public String getName() {
            return name;
        }

        public ListDataProvider<TreeNode> getChildNodes() {
            return childNodes;
        }
    }

    private static class LeafNode implements TreeNode {

        private final Region region;

        private LeafNode(Region region) {
            this.region = region;
        }

        @Override
        public String getName() {
            return region.getName();
        }

        Region getRegion() {
            return region;
        }
    }
}
