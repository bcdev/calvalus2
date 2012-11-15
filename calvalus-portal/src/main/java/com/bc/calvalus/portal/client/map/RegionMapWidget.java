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
import com.google.gwt.maps.client.MapWidget;
import com.google.gwt.maps.client.control.MapTypeControl;
import com.google.gwt.maps.client.event.PolygonLineUpdatedHandler;
import com.google.gwt.maps.client.overlay.PolyStyleOptions;
import com.google.gwt.maps.client.overlay.Polygon;
import com.google.gwt.safehtml.shared.SafeHtmlBuilder;
import com.google.gwt.user.cellview.client.CellTree;
import com.google.gwt.user.client.ui.DockLayoutPanel;
import com.google.gwt.user.client.ui.ResizeComposite;
import com.google.gwt.user.client.ui.ScrollPanel;
import com.google.gwt.user.client.ui.SplitLayoutPanel;
import com.google.gwt.view.client.ListDataProvider;
import com.google.gwt.view.client.ProvidesKey;
import com.google.gwt.view.client.SelectionChangeEvent;
import com.google.gwt.view.client.SelectionModel;
import com.google.gwt.view.client.TreeViewModel;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

/**
 * An implementation of a Google map that has regions.
 *
 * @author Norman Fomferra
 */
public class RegionMapWidget extends ResizeComposite implements RegionMap {

    private final RegionMapModel regionMapModel;
    private final RegionMapSelectionModel regionMapSelectionModel;
    private MapWidget mapWidget;
    private boolean adjustingRegionSelection;
    private MapInteraction currentInteraction;

    private boolean editable;
    private final MapAction[] actions;
    private Map<Region, Polygon> polygonMap;
    private Map<Polygon, Region> regionMap;

    private PolyStyleOptions normalPolyStrokeStyle;
    private PolyStyleOptions normalPolyFillStyle;
    private PolyStyleOptions selectedPolyStrokeStyle;
    private PolyStyleOptions selectedPolyFillStyle;
    private RegionMapToolbar regionMapToolbar;
    private PolygonLineUpdatedHandler polygonLineUpdatedHandler;
    private CellTree regionCellTree;

    public RegionMapWidget(RegionMapModel regionMapModel, boolean editable, MapAction... actions) {
        this(regionMapModel, new RegionMapSelectionModelImpl(), editable, actions);
    }

    public RegionMapWidget(RegionMapModel regionMapModel,
                           RegionMapSelectionModel regionMapSelectionModel,
                           boolean editable,
                           MapAction... actions) {
        this.regionMapModel = regionMapModel;
        this.regionMapSelectionModel = regionMapSelectionModel;
        this.editable = editable;
        this.actions = actions;
        this.normalPolyStrokeStyle = PolyStyleOptions.newInstance("#0000FF", 3, 0.8);
        this.normalPolyFillStyle = PolyStyleOptions.newInstance("#0000FF", 3, 0.2);
        this.selectedPolyStrokeStyle = PolyStyleOptions.newInstance("#FFFF00", 3, 0.8);
        this.selectedPolyFillStyle = normalPolyFillStyle;
        polygonLineUpdatedHandler = new MyPolygonLineUpdatedHandler();
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

    interface CalvalusTreeResource extends CellTree.Resources {

        @Source("com/bc/calvalus/portal/client/cellTreeStyle.css")
        CellTree.Style cellTreeStyle();
    }

    private void initUi() {

        mapWidget = new MapWidget();
        mapWidget.setDoubleClickZoom(true);
        mapWidget.setScrollWheelZoomEnabled(true);
        mapWidget.addControl(new MapTypeControl());
        // Other possible MapWidget Controls are:
        // mapWidget.addControl(new SmallMapControl());
        // mapWidget.addControl(new OverviewMapControl());

        CellTree.Resources res = GWT.create(CalvalusTreeResource.class);
//        final SelectionModel<Region> regionSelectionModel = new MultiSelectionModel<Region>(Region.KEY_PROVIDER);
        RegionTreeSelectionModel treeNodeSingleSelectionModel = new RegionTreeSelectionModel();
        RegionTreeViewModel treeViewModel = new RegionTreeViewModel(regionMapModel, treeNodeSingleSelectionModel);
        regionCellTree = new CellTree(treeViewModel, null, res);
        regionCellTree.setAnimationEnabled(true);
        regionCellTree.setDefaultNodeSize(100);

        DockLayoutPanel regionPanel = new DockLayoutPanel(Style.Unit.EM);
        regionPanel.ensureDebugId("regionPanel");
        if (actions.length > 0) {
            regionMapToolbar = new RegionMapToolbar(this);
            regionPanel.addSouth(regionMapToolbar, 3.5);
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
            mapWidget.addOverlay(polygon);
            updatePolygonStyle(region, polygon);
        }
        return polygon;
    }

    private Polygon ensurePolygonAbsent(Region region) {
        Polygon polygon = polygonMap.get(region);
        if (polygon != null) {
            mapWidget.removeOverlay(polygon);
            regionMap.remove(polygon);
            polygonMap.remove(region);
        }
        return polygon;
    }

    private void updatePolygonStyles() {
        List<Region> regionList = regionMapModel.getRegionProvider().getList();
        for (Region region : regionList) {
            updatePolygonStyle(region, ensurePolygonPresent(region));
        }
    }

    private void updatePolygonStyle(Region region, Polygon polygon) {
        boolean selected = regionMapSelectionModel.isSelected(region);
        polygon.setStrokeStyle(selected ? selectedPolyStrokeStyle : normalPolyStrokeStyle);
        polygon.setFillStyle(selected ? selectedPolyFillStyle : normalPolyFillStyle);
        if (editable && region.isUserRegion()) {
            polygon.setEditingEnabled(selected);
            if (selected) {
                polygon.addPolygonLineUpdatedHandler(polygonLineUpdatedHandler);
            } else {
                polygon.removePolygonLineUpdatedHandler(polygonLineUpdatedHandler);
            }
        }
    }

    private void updateRegionSelectionInTree(RegionMapSelectionModel source,
                                             RegionTreeSelectionModel target,
                                             RegionTreeViewModel treeViewModel) {
        Region selectedRegion = source.getSelectedRegion();
        target.clearSelection();
        TreeNode nodeToSelect = findNodeForRegion(selectedRegion, treeViewModel.getRootNode());
        if (nodeToSelect != null) {
            target.setSelected(nodeToSelect, true);
        }
    }

    private TreeNode findNodeForRegion(Region region, GroupNode rootNode) {
        for (TreeNode childNode : rootNode.getChildNodes()) {
            if (childNode instanceof LeafNode) {
                LeafNode leafNode = (LeafNode) childNode;
                if (leafNode.getRegion() == region) {
                    return leafNode;
                }
            } else if (childNode instanceof GroupNode) {
                TreeNode nodeForRegion = findNodeForRegion(region, (GroupNode) childNode);
                if(nodeForRegion != null) {
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

    public static MapAction[] createDefaultEditingActions() {
        // todo: use the action constructor that takes an icon image (nf)
        final SelectInteraction selectInteraction = createSelectInteraction();
        return new MapAction[]{
                selectInteraction,
                new InsertPolygonInteraction(new AbstractMapAction("P", "New polygon region") {
                    @Override
                    public void run(RegionMap regionMap) {
                        regionMap.setCurrentInteraction(selectInteraction);
                    }
                }),
                new InsertBoxInteraction(new AbstractMapAction("B", "New box region") {
                    @Override
                    public void run(RegionMap regionMap) {
                        regionMap.setCurrentInteraction(selectInteraction);
                    }
                }),
                MapAction.SEPARATOR,
                new EditVerticesAction(),
                new RenameRegionAction(),
                new DeleteRegionsAction(),
                new LocateRegionsAction(),
                new ShowRegionInfoAction()
        };
    }

    private static SelectInteraction createSelectInteraction() {
        return new SelectInteraction(new AbstractMapAction("S", "Select region") {
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

    private class MyPolygonLineUpdatedHandler implements PolygonLineUpdatedHandler {

        @Override
        public void onUpdate(PolygonLineUpdatedEvent event) {
            Polygon polygon = event.getSender();
            Region region = regionMap.get(polygon);
            if (region != null) {
                region.setVertices(Region.getVertices(polygon));
                getRegionModel().fireRegionChanged(RegionMapWidget.this, region);
            }
        }
    }

    private static class RegionTreeViewModel implements TreeViewModel {

        private final Cell<TreeNode> cell;
        private final RegionMapModel regionMapModel;
        private final SelectionModel<TreeNode> treeSelectionModel;
        private GroupNode rootNode;

        public RegionTreeViewModel(final RegionMapModel regionMapModel,
                                   RegionTreeSelectionModel treeNodeSingleSelectionModel) {
            this.regionMapModel = regionMapModel;
            this.treeSelectionModel = treeNodeSingleSelectionModel;
            buildTree();
            regionMapModel.addChangeListener(new RegionMapModel.ChangeListener() {
                @Override
                public void onRegionAdded(RegionMapModel.ChangeEvent event) {
                    buildTree();
                }

                @Override
                public void onRegionRemoved(RegionMapModel.ChangeEvent event) {
                    buildTree();
                }

                @Override
                public void onRegionChanged(RegionMapModel.ChangeEvent event) {
                    buildTree();
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

        private void buildTree() {
            rootNode = new GroupNode("");
            List<Region> regions = regionMapModel.getRegionProvider().getList();
            for (Region region : regions) {
                GroupNode currentNode = rootNode;
                for (String pathElem : region.getPath()) {
                    GroupNode childNode = null;
                    for (TreeNode aNode : currentNode.getChildNodes()) {
                        if (aNode.getName().equals(pathElem) && aNode instanceof GroupNode) {
                            childNode = (GroupNode) aNode;
                            break;
                        }
                    }
                    if (childNode == null) {
                        childNode = new GroupNode(pathElem);
                        currentNode.getChildNodes().add(childNode);
                    }
                    currentNode = childNode;
                }
                currentNode.getChildNodes().add(new LeafNode(region));
            }
        }

        @Override
        public <T> NodeInfo<?> getNodeInfo(T value) {
            ListDataProvider<TreeNode> dataProvider = null;
            if (value == null) {
                dataProvider = new ListDataProvider<TreeNode>(rootNode.getChildNodes());
            } else if (value instanceof GroupNode) {
                dataProvider = new ListDataProvider<TreeNode>(((GroupNode) value).getChildNodes());
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
        private final List<TreeNode> childNodes;

        private GroupNode(String name) {
            this.name = name;
            childNodes = new ArrayList<TreeNode>();
        }

        @Override
        public String getName() {
            return name;
        }

        public List<TreeNode> getChildNodes() {
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
