package com.bc.calvalus.portal.client.map;

import com.bc.calvalus.portal.client.map.actions.DeleteRegionsAction;
import com.bc.calvalus.portal.client.map.actions.InsertBoxInteraction;
import com.bc.calvalus.portal.client.map.actions.InsertPolygonInteraction;
import com.bc.calvalus.portal.client.map.actions.LocateRegionsAction;
import com.bc.calvalus.portal.client.map.actions.RenameRegionAction;
import com.bc.calvalus.portal.client.map.actions.SelectInteraction;
import com.bc.calvalus.portal.client.map.actions.ShowRegionInfoAction;
import com.google.gwt.cell.client.AbstractCell;
import com.google.gwt.cell.client.Cell;
import com.google.gwt.dom.client.Style;
import com.google.gwt.maps.client.MapWidget;
import com.google.gwt.maps.client.control.MapTypeControl;
import com.google.gwt.maps.client.overlay.PolyStyleOptions;
import com.google.gwt.maps.client.overlay.Polygon;
import com.google.gwt.safehtml.shared.SafeHtmlBuilder;
import com.google.gwt.user.cellview.client.CellList;
import com.google.gwt.user.cellview.client.HasKeyboardPagingPolicy;
import com.google.gwt.user.cellview.client.HasKeyboardSelectionPolicy;
import com.google.gwt.user.client.ui.Composite;
import com.google.gwt.user.client.ui.DockLayoutPanel;
import com.google.gwt.user.client.ui.ScrollPanel;
import com.google.gwt.user.client.ui.SplitLayoutPanel;
import com.google.gwt.view.client.ListDataProvider;
import com.google.gwt.view.client.MultiSelectionModel;
import com.google.gwt.view.client.ProvidesKey;
import com.google.gwt.view.client.SelectionChangeEvent;
import com.google.gwt.view.client.SelectionModel;

import java.util.List;

/**
 * An implementation of a Google map that has regions.
 *
 * @author Norman Fomferra
 */
public class RegionMapWidget extends Composite implements RegionMap {

    private final RegionMapModel regionMapModel;
    private final RegionMapSelectionModel regionMapSelectionModel;
    private MapWidget mapWidget;
    private boolean adjustingRegionSelection;

    private PolyStyleOptions normalPolyStrokeStyle;
    private PolyStyleOptions normalPolyFillStyle;
    private PolyStyleOptions selectedPolyStrokeStyle;
    private PolyStyleOptions selectedPolyFillStyle;

    public static RegionMapWidget create(ListDataProvider<Region> regionList, boolean editable) {
        final RegionMapModelImpl model;
        if (editable) {
            model = new RegionMapModelImpl(regionList, createDefaultEditingActions());
        } else {
            model = new RegionMapModelImpl(regionList, createDefaultNonEditingActions());
        }
        return new RegionMapWidget(model);
    }

    public RegionMapWidget(RegionMapModel regionMapModel) {
        this(regionMapModel, new RegionMapSelectionModelImpl());
    }

    public RegionMapWidget(RegionMapModel regionMapModel, RegionMapSelectionModel regionMapSelectionModel) {
        this.regionMapModel = regionMapModel;
        this.regionMapSelectionModel = regionMapSelectionModel;
        this.normalPolyStrokeStyle = PolyStyleOptions.newInstance("#0000FF", 3, 0.8);
        this.normalPolyFillStyle = PolyStyleOptions.newInstance("#0000FF", 3, 0.2);
        this.selectedPolyStrokeStyle = PolyStyleOptions.newInstance("#FFFF00", 3, 0.8);
        this.selectedPolyFillStyle = normalPolyFillStyle;
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
                return item.getName();
            }
        };

        final CellList<Region> regionCellList = new CellList<Region>(regionCell, regionKey);
        regionCellList.setVisibleRange(0, 256);
        regionCellList.setKeyboardPagingPolicy(HasKeyboardPagingPolicy.KeyboardPagingPolicy.INCREASE_RANGE);
        regionCellList.setKeyboardSelectionPolicy(HasKeyboardSelectionPolicy.KeyboardSelectionPolicy.ENABLED);
        regionMapModel.getRegionList().addDataDisplay(regionCellList);

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
                        updatePolygonStyles();
                    } finally {
                        adjustingRegionSelection = false;
                    }
                }
            }
        });

        ScrollPanel regionScrollPanel = new ScrollPanel(regionCellList);

        RegionMapToolbar regionMapToolbar = new RegionMapToolbar(this);

        DockLayoutPanel regionPanel = new DockLayoutPanel(Style.Unit.EM);
        regionPanel.ensureDebugId("regionPanel");
        regionPanel.addSouth(regionMapToolbar, 3.5);
        regionPanel.add(regionScrollPanel);

        SplitLayoutPanel regionSplitLayoutPanel = new SplitLayoutPanel();
        regionSplitLayoutPanel.ensureDebugId("regionSplitLayoutPanel");
        regionSplitLayoutPanel.addWest(regionPanel, 180);
        regionSplitLayoutPanel.add(mapWidget);
        regionSplitLayoutPanel.setSize("900px", "360px");

        List<Region> regionList = regionMapModel.getRegionList().getList();
        for (Region region : regionList) {
            Polygon polygon = region.getPolygon();
            polygon.setVisible(true);
            polygon.setStrokeStyle(normalPolyStrokeStyle);
            polygon.setFillStyle(normalPolyFillStyle);
            mapWidget.addOverlay(polygon);
        }

        initWidget(regionSplitLayoutPanel);
//        setSize("720px", "360px");
    }

    private void updatePolygonStyles() {
        for (Region region : regionMapModel.getRegionList().getList()) {
            boolean selected = regionMapSelectionModel.isSelected(region);
            region.getPolygon().setStrokeStyle(selected ? selectedPolyStrokeStyle : normalPolyStrokeStyle);
            region.getPolygon().setFillStyle(selected ? selectedPolyFillStyle : normalPolyFillStyle);
            if (region.isUserRegion()) {
                region.getPolygon().setEditingEnabled(selected);
            }
        }
    }

    private void updateRegionSelection(SelectionModel<Region> source, SelectionModel<Region> target) {
        for (Region region : regionMapModel.getRegionList().getList()) {
            target.setSelected(region, source.isSelected(region));
        }
    }

    private static MapAction[] createDefaultEditingActions() {
        // todo: use the action constructor that takes an icon image (nf)
        return new MapAction[]{
                new SelectInteraction(new AbstractMapAction("S", "Select region") {
                    @Override
                    public void run(RegionMap regionMap) {
                        // Window.alert("Selected.");
                    }
                }),
                new InsertPolygonInteraction(new AbstractMapAction("P", "New polygon region") {
                    @Override
                    public void run(RegionMap regionMap) {
                        // Window.alert("New polygon region created.");
                    }
                }),
                new InsertBoxInteraction(new AbstractMapAction("B", "New box region") {
                    @Override
                    public void run(RegionMap regionMap) {
                        // Window.alert("New box region created.");
                    }
                }),
                MapAction.SEPARATOR,
                new RenameRegionAction(),
                new DeleteRegionsAction(),
                new LocateRegionsAction(),
                new ShowRegionInfoAction()
        };
    }

    private static MapAction[] createDefaultNonEditingActions() {
        // todo: use the action constructor that takes an icon image (nf)
        return new MapAction[]{
                new SelectInteraction(new AbstractMapAction("S", "Select region") {
                    @Override
                    public void run(RegionMap regionMap) {
                        // Window.alert("Selected.");
                    }
                }),
                MapAction.SEPARATOR,
                new LocateRegionsAction(),
                new ShowRegionInfoAction()
        };
    }


}
