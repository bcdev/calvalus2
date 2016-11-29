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
import com.google.gwt.cell.client.CheckboxCell;
import com.google.gwt.cell.client.CompositeCell;
import com.google.gwt.cell.client.FieldUpdater;
import com.google.gwt.cell.client.HasCell;
import com.google.gwt.core.client.GWT;
import com.google.gwt.dom.client.Element;
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
import com.google.gwt.user.cellview.client.TreeNode;
import com.google.gwt.user.client.ui.DockLayoutPanel;
import com.google.gwt.user.client.ui.Image;
import com.google.gwt.user.client.ui.ResizeComposite;
import com.google.gwt.user.client.ui.ScrollPanel;
import com.google.gwt.user.client.ui.SplitLayoutPanel;
import com.google.gwt.view.client.ListDataProvider;
import com.google.gwt.view.client.MultiSelectionModel;
import com.google.gwt.view.client.ProvidesKey;
import com.google.gwt.view.client.SelectionChangeEvent;
import com.google.gwt.view.client.SelectionModel;
import com.google.gwt.view.client.TreeViewModel;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Set;

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
            polygon.setVisible(region.isShowPolyon());
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
            if (!regionMapSelectionModel.isSelected(region) && region.isShowPolyon()) {
                updatePolygonStyle(region, ensurePolygonPresent(region), false);
            }
        }
        Region selectedRegion = regionMapSelectionModel.getSelectedRegion();
        if (selectedRegion != null  && selectedRegion.isShowPolyon()) {
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
        RegionTreeNode nodeToSelect = findNodeForRegion(selectedRegion, treeViewModel.getRootNode(), false);
        if (nodeToSelect != null) {
            target.setSelected(nodeToSelect, true);
        }
    }

    private static RegionTreeNode findNodeForRegion(Region region, RegionGroupNode rootNode, boolean returnParent) {
        for (RegionTreeNode childNode : rootNode.getChildNodes().getList()) {
            if (childNode instanceof RegionLeafNode) {
                RegionLeafNode leafNode = (RegionLeafNode) childNode;
                if (leafNode.getRegion() == region) {
                    if (returnParent) {
                        return rootNode;
                    } else {
                        return leafNode;
                    }
                }
            } else if (childNode instanceof RegionGroupNode) {
                RegionTreeNode nodeForRegion = findNodeForRegion(region, (RegionGroupNode) childNode, returnParent);
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

        RegionLeafNode selectedLeafNode = source.getSelectedLeafNode();
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

    private static class RegionTreeSelectionModel extends SelectionModel.AbstractSelectionModel<RegionTreeNode> {

        private RegionLeafNode selectedNode;

        public RegionTreeSelectionModel() {
            super(RegionMapWidget.KEY_PROVIDER);
        }

        @Override
        public boolean isSelected(RegionTreeNode regionTreeNode) {
            return selectedNode == regionTreeNode;
        }

        @Override
        public void setSelected(RegionTreeNode regionTreeNode, boolean selected) {
            if (regionTreeNode instanceof RegionLeafNode) {
                if (selected) {
                    if (selectedNode != regionTreeNode) {
                        selectedNode = (RegionLeafNode) regionTreeNode;
                        fireSelectionChangeEvent();
                    }
                } else {
                    if (selectedNode == regionTreeNode) {
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

        public RegionLeafNode getSelectedLeafNode() {
            return selectedNode;
        }
    }

    private class RegionTreeViewModel implements TreeViewModel {

        private final Cell<RegionTreeNode> nameCell;
        private final Cell<RegionTreeNode> topLevelCell;
        //        private final Cell<TreeNode> cell;
        private final SelectionModel<RegionTreeNode> treeSelectionModel;
        private final MultiSelectionModel<RegionTreeNode> topLevelSelectionModel;
        private final RegionGroupNode rootNode = new RegionGroupNode("");

        public RegionTreeViewModel(RegionMapModel regionMapModel, RegionTreeSelectionModel regionTreeSelectionModel) {
            this.treeSelectionModel = regionTreeSelectionModel;
            this.topLevelSelectionModel = new MultiSelectionModel<RegionTreeNode>();
            this.topLevelSelectionModel.addSelectionChangeHandler(new SelectionChangeEvent.Handler() {
                @Override
                public void onSelectionChange(SelectionChangeEvent event) {
                    Set<RegionTreeNode> selectedSet = topLevelSelectionModel.getSelectedSet();
                    List<RegionTreeNode> list = rootNode.getChildNodes().getList();
                    for (int i = 0; i < list.size(); i++) {
                        RegionTreeNode topLevelNode = list.get(i);
                        boolean showPolyon = selectedSet.contains(topLevelNode);
                        setSubtreeShow(topLevelNode, showPolyon);
                        if (!showPolyon) {
                            TreeNode rootTreeNode = regionCellTree.getRootTreeNode();
                            rootTreeNode.setChildOpen(i, false, true);
                        }
                    }
                }

                private void setSubtreeShow(RegionTreeNode regionTreeNode, boolean showPolygon) {
                    if (regionTreeNode instanceof RegionGroupNode) {
                        RegionGroupNode groupNode = (RegionGroupNode) regionTreeNode;
                        for (RegionTreeNode childNode : groupNode.getChildNodes().getList()) {
                            setSubtreeShow(childNode, showPolygon);
                        }
                    } else if (regionTreeNode instanceof RegionLeafNode) {
                        RegionLeafNode leafNode = (RegionLeafNode) regionTreeNode;
                        Region region = leafNode.getRegion();
                        ensurePolygonPresent(region);
                        region.setShowPolyon(showPolygon);
                        polygonMap.get(region).setVisible(showPolygon);
                    }
                }
            });
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
                    RegionTreeNode parentNodeForRegion = findNodeForRegion(region, rootNode, true);
                    if (parentNodeForRegion instanceof RegionGroupNode) {
                        RegionGroupNode parentNode = (RegionGroupNode) parentNodeForRegion;
                        List<RegionTreeNode> nodeList = parentNode.getChildNodes().getList();
                        for (RegionTreeNode childNode : nodeList) {
                            if (childNode instanceof RegionLeafNode) {
                                RegionLeafNode node = (RegionLeafNode) childNode;
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
                    RegionTreeNode parentNodeForRegion = findNodeForRegion(region, rootNode, true);
                    if (parentNodeForRegion instanceof RegionGroupNode) {
                        RegionGroupNode parentNode = (RegionGroupNode) parentNodeForRegion;
                        parentNode.getChildNodes().refresh();
                    }
                }
            });
            nameCell = new AbstractCell<RegionTreeNode>() {
                @Override
                public void render(Context context, RegionTreeNode value, SafeHtmlBuilder sb) {
                    sb.appendHtmlConstant(value.getName());
                }
            };

            // Construct a composite cell fthat includes a checkbox.
            List<HasCell<RegionTreeNode, ?>> hasCells = new ArrayList<>();
            hasCells.add(new HasCell<RegionTreeNode, Boolean>() {

                private CheckboxCell cell = new CheckboxCell(true, true);

                public Cell<Boolean> getCell() {
                    return cell;
                }

                public FieldUpdater<RegionTreeNode, Boolean> getFieldUpdater() {
                    return new FieldUpdater<RegionTreeNode, Boolean>() {
                        @Override
                        public void update(int index, RegionTreeNode regionTreeNode, Boolean value) {
                            topLevelSelectionModel.setSelected(regionTreeNode, value);
                        }
                    };
                }

                public Boolean getValue(RegionTreeNode regionTreeNode) {
                    return topLevelSelectionModel.isSelected(regionTreeNode);
                }
            });
            hasCells.add(new HasCell<RegionTreeNode, RegionTreeNode>() {

                public Cell<RegionTreeNode> getCell() {
                    return nameCell;
                }

                public FieldUpdater<RegionTreeNode, RegionTreeNode> getFieldUpdater() {
                    return null;
                }

                public RegionTreeNode getValue(RegionTreeNode regionTreeNode) {
                    return regionTreeNode;
                }
            });
            topLevelCell = new CompositeCell<RegionTreeNode>(hasCells) {
                @Override
                public void render(Context context, RegionTreeNode value, SafeHtmlBuilder sb) {
                    sb.appendHtmlConstant("<table><tbody><tr>");
                    super.render(context, value, sb);
                    sb.appendHtmlConstant("</tr></tbody></table>");
                }

                @Override
                protected Element getContainerElement(Element parent) {
                    // Return the first TR element in the table.
                    return parent.getFirstChildElement().getFirstChildElement().getFirstChildElement();
                }

                @Override
                protected <X> void render(Context context, RegionTreeNode value,
                                          SafeHtmlBuilder sb, HasCell<RegionTreeNode, X> hasCell) {
                    Cell<X> cell = hasCell.getCell();
                    sb.appendHtmlConstant("<td>");
                    cell.render(context, hasCell.getValue(value), sb);
                    sb.appendHtmlConstant("</td>");
                }
            };
        }

        public RegionGroupNode getRootNode() {
            return rootNode;
        }

        private void addRegion(RegionGroupNode rootNode, Region region) {
            RegionGroupNode currentNode = rootNode;
            for (String pathElem : region.getPath()) {
                RegionGroupNode childNode = null;
                for (RegionTreeNode aNode : currentNode.getChildNodes().getList()) {
                    if (aNode.getName().equals(pathElem) && aNode instanceof RegionGroupNode) {
                        childNode = (RegionGroupNode) aNode;
                        break;
                    }
                }
                if (childNode == null) {
                    childNode = new RegionGroupNode(pathElem);
                    currentNode.getChildNodes().getList().add(childNode);
                    currentNode.getChildNodes().refresh();
                }
                currentNode = childNode;
            }
            currentNode.getChildNodes().getList().add(new RegionLeafNode(region));
            currentNode.getChildNodes().refresh();
        }

        @Override
        public <T> NodeInfo<?> getNodeInfo(T value) {
            ListDataProvider<RegionTreeNode> dataProvider = null;
            if (value == null) {
                dataProvider = rootNode.getChildNodes();
                return new DefaultNodeInfo<RegionTreeNode>(dataProvider, topLevelCell, topLevelSelectionModel, null);
            } else if (value instanceof RegionGroupNode) {
                dataProvider = ((RegionGroupNode) value).getChildNodes();
                return new DefaultNodeInfo<RegionTreeNode>(dataProvider, nameCell, treeSelectionModel, null);
            }
            return null;
        }

        @Override
        public boolean isLeaf(Object value) {
            return value instanceof RegionLeafNode;
        }

    }

    public static final ProvidesKey<RegionTreeNode> KEY_PROVIDER = new ProvidesKey<RegionTreeNode>() {
        @Override
        public Object getKey(RegionTreeNode regionTreeNode) {
            return regionTreeNode;
        }
    };

    private interface RegionTreeNode {
        String getName();
    }

    private class RegionGroupNode implements RegionTreeNode {

        private final String name;
        private final ListDataProvider<RegionTreeNode> childNodes;

        private RegionGroupNode(String name) {
            this.name = name;
            childNodes = new ListDataProvider<RegionTreeNode>();
        }

        @Override
        public String getName() {
            return name;
        }

        public ListDataProvider<RegionTreeNode> getChildNodes() {
            return childNodes;
        }
    }

    private class RegionLeafNode implements RegionTreeNode {

        private final Region region;

        private RegionLeafNode(Region region) {
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
