package com.bc.calvalus.portal.client.map;

import com.bc.calvalus.portal.client.map.actions.*;
import com.google.gwt.cell.client.AbstractCell;
import com.google.gwt.cell.client.Cell;
import com.google.gwt.core.client.GWT;
import com.google.gwt.dom.client.Style;
import com.google.gwt.maps.client.MapWidget;
import com.google.gwt.maps.client.control.MapTypeControl;
import com.google.gwt.maps.client.overlay.PolyStyleOptions;
import com.google.gwt.maps.client.overlay.Polygon;
import com.google.gwt.safehtml.shared.SafeHtmlBuilder;
import com.google.gwt.user.cellview.client.CellList;
import com.google.gwt.user.cellview.client.HasKeyboardPagingPolicy;
import com.google.gwt.user.cellview.client.HasKeyboardSelectionPolicy;
import com.google.gwt.user.client.ui.DockLayoutPanel;
import com.google.gwt.user.client.ui.ResizeComposite;
import com.google.gwt.user.client.ui.ScrollPanel;
import com.google.gwt.user.client.ui.SplitLayoutPanel;
import com.google.gwt.view.client.*;

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
    private Map<Region, Polygon> polygonMap;
    private Map<Polygon, Region> regionMap;

    private PolyStyleOptions normalPolyStrokeStyle;
    private PolyStyleOptions normalPolyFillStyle;
    private PolyStyleOptions selectedPolyStrokeStyle;
    private PolyStyleOptions selectedPolyFillStyle;

    public static RegionMapWidget create(ListDataProvider<Region> regionList, boolean editable) {
        final RegionMapModelImpl model;
        if (editable) {
            model = new RegionMapModelImpl(regionList, createDefaultEditingActions());
        } else {
            model = new RegionMapModelImpl(regionList);
        }
        return new RegionMapWidget(model, editable);
    }

    public RegionMapWidget(RegionMapModel regionMapModel, boolean editable) {
        this(regionMapModel, new RegionMapSelectionModelImpl(), editable);
    }

    public RegionMapWidget(RegionMapModel regionMapModel, RegionMapSelectionModel regionMapSelectionModel, boolean editable) {
        this.regionMapModel = regionMapModel;
        this.regionMapSelectionModel = regionMapSelectionModel;
        this.editable = editable;
        this.normalPolyStrokeStyle = PolyStyleOptions.newInstance("#0000FF", 3, 0.8);
        this.normalPolyFillStyle = PolyStyleOptions.newInstance("#0000FF", 3, 0.2);
        this.selectedPolyStrokeStyle = PolyStyleOptions.newInstance("#FFFF00", 3, 0.8);
        this.selectedPolyFillStyle = normalPolyFillStyle;

        polygonMap = new HashMap<Region, Polygon>();
        regionMap = new HashMap<Polygon, Region>();

        initUi();
    }

    @Override
    public RegionMapModel getRegionModel() {
        return regionMapModel;
    }

    @Override
    public RegionMapSelectionModel getRegionSelectionModel() {
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

        Polygon polygon = region.createPolygon();
        mapWidget.addOverlay(polygon);
        regionMap.put(polygon, region);
        polygonMap.put(region, polygon);

        getRegionModel().getRegionProvider().getList().add(0, region);
        getRegionSelectionModel().clearSelection();
        getRegionSelectionModel().setSelected(region, true);
    }

    @Override
    public void removeRegion(Region region) {

        getRegionSelectionModel().setSelected(region, false);
        getRegionModel().getRegionProvider().getList().remove(region);

        Polygon polygon = polygonMap.get(region);
        mapWidget.removeOverlay(polygon);
        regionMap.remove(polygon);
        polygonMap.remove(region);
    }

    @Override
    public MapInteraction getCurrentInteraction() {
        return currentInteraction;
    }

    @Override
    public void setCurrentInteraction(MapInteraction mapInteraction) {
        if (currentInteraction != null) {
            currentInteraction.detachFrom(this);
        }
        currentInteraction = mapInteraction;
        if (currentInteraction != null) {
            currentInteraction.attachTo(this);
        }
    }

    private void initUi() {

        mapWidget = new MapWidget();
        mapWidget.setDoubleClickZoom(true);
        mapWidget.setScrollWheelZoomEnabled(true);
        mapWidget.addControl(new MapTypeControl());
        // mapWidget.addControl(new SmallMapControl());
        // mapWidget.addControl(new OverviewMapControl());

        Cell<Region> regionCell = new AbstractCell<Region>() {
            @Override
            public void render(Context context, Region value, SafeHtmlBuilder sb) {
                sb.appendHtmlConstant(value.getName());
            }
        };

        ProvidesKey<Region> regionKey = new ProvidesKey<Region>() {
            @Override
            public Object getKey(Region item) {
                return item.getQualifiedName();
            }
        };

        final CellList<Region> regionCellList = new CellList<Region>(regionCell, regionKey);
        regionCellList.setVisibleRange(0, 256);
        regionCellList.setKeyboardPagingPolicy(HasKeyboardPagingPolicy.KeyboardPagingPolicy.INCREASE_RANGE);
        regionCellList.setKeyboardSelectionPolicy(HasKeyboardSelectionPolicy.KeyboardSelectionPolicy.ENABLED);
        regionMapModel.getRegionProvider().addDataDisplay(regionCellList);

        // Add a selection model so we can select cells.
        final MultiSelectionModel<Region> regionListSelectionModel = new MultiSelectionModel<Region>(regionKey);
        regionCellList.setSelectionModel(regionListSelectionModel);
        regionCellList.getSelectionModel().addSelectionChangeHandler(new SelectionChangeEvent.Handler() {
            @Override
            public void onSelectionChange(SelectionChangeEvent event) {
                if (!adjustingRegionSelection) {
                    try {
                        adjustingRegionSelection = true;
                        updateRegionSelection(regionListSelectionModel, regionMapSelectionModel);
                        if (!editable) {
                            new LocateRegionsAction().run(RegionMapWidget.this);
                        }
                    } finally {
                        adjustingRegionSelection = false;
                    }
                }
            }
        });

        regionMapSelectionModel.addSelectionChangeHandler(new SelectionChangeEvent.Handler() {
            @Override
            public void onSelectionChange(SelectionChangeEvent event) {
                if (!adjustingRegionSelection) {
                    try {
                        adjustingRegionSelection = true;
                        updateRegionSelection(regionMapSelectionModel, regionListSelectionModel);
                        updatePolygons();
                    } finally {
                        adjustingRegionSelection = false;
                    }
                }
            }
        });

        ScrollPanel regionScrollPanel = new ScrollPanel(regionCellList);

        DockLayoutPanel regionPanel = new DockLayoutPanel(Style.Unit.EM);
        regionPanel.ensureDebugId("regionPanel");

        if (getRegionModel().getActions().length > 0) {
            RegionMapToolbar regionMapToolbar = new RegionMapToolbar(this);
            regionPanel.addSouth(regionMapToolbar, 3.5);
        }
        regionPanel.add(regionScrollPanel);

        SplitLayoutPanel regionSplitLayoutPanel = new SplitLayoutPanel();
        regionSplitLayoutPanel.ensureDebugId("regionSplitLayoutPanel");
        regionSplitLayoutPanel.addWest(regionPanel, 180);
        regionSplitLayoutPanel.add(mapWidget);

        if (getCurrentInteraction() == null) {
            setCurrentInteraction(createSelectInteraction());
        }

        updatePolygons();
        initWidget(regionSplitLayoutPanel);
    }

    private void updatePolygons() {
        List<Region> regionList = regionMapModel.getRegionProvider().getList();
        for (Region region : regionList) {
            boolean selected = regionMapSelectionModel.isSelected(region);
            Polygon polygon = polygonMap.get(region);
            if (polygon == null) {
                polygon = region.createPolygon();
                polygon.setVisible(true);
                mapWidget.addOverlay(polygon);
                regionMap.put(polygon, region);
                polygonMap.put(region, polygon);
            }
            polygon.setStrokeStyle(selected ? selectedPolyStrokeStyle : normalPolyStrokeStyle);
            polygon.setFillStyle(selected ? selectedPolyFillStyle : normalPolyFillStyle);
            if (editable && region.isUserRegion()) {
                polygon.setEditingEnabled(selected);
            }
        }
    }

    public void applyVertexChanges() {
        GWT.log("Applying vertex changes in user regions...");
        List<Region> regionList = regionMapModel.getRegionProvider().getList();
        for (Region region : regionList) {
            if (region.isUserRegion()) {
                Polygon polygon = polygonMap.get(region);
                if (polygon != null) {
                    region.setVertices(Region.getVertices(polygon));
                }
            }
        }
    }

    private void updateRegionSelection(SelectionModel<Region> source, SelectionModel<Region> target) {
        for (Region region : regionMapModel.getRegionProvider().getList()) {
            target.setSelected(region, source.isSelected(region));
        }
    }

    private static MapAction[] createDefaultEditingActions() {
        // todo: use the action constructor that takes an icon image (nf)
        return new MapAction[]{
                createSelectInteraction(),
                new InsertPolygonInteraction(new AbstractMapAction("P", "New polygon region") {
                    @Override
                    public void run(RegionMap regionMap) {
                    }
                }),
                new InsertBoxInteraction(new AbstractMapAction("B", "New box region") {
                    @Override
                    public void run(RegionMap regionMap) {
                    }
                }),
                MapAction.SEPARATOR,
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

}
