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
import com.google.gwt.user.client.ui.DockLayoutPanel;
import com.google.gwt.user.client.ui.HTML;
import com.google.gwt.user.client.ui.IsWidget;
import com.google.gwt.user.client.ui.ScrollPanel;
import com.google.gwt.user.client.ui.SplitLayoutPanel;
import com.google.gwt.user.client.ui.Widget;
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
public class RegionMapWidget implements IsWidget, RegionMap {

    private final RegionMapModel regionMapModel;
    private final RegionMapSelectionModel regionMapSelectionModel;
    private final boolean editable;
    private Widget widget;
    private MapWidget mapWidget;
    private boolean adjustingRegionSelection;
    private boolean updatingRegionListSelection;
    private String width;
    private String height;

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
        width = "100%";
        height = "600px";
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
        if (mapWidget == null) {
            initUi();
        }
        return mapWidget;
    }

    @Override
    public Widget asWidget() {
        if (widget == null) {
            initUi();
        }
        return widget;
    }

    public void setSize(String width, String height) {
        this.width = width;
        this.height = height;
    }

    private void initUi() {

        mapWidget = new MapWidget();
        mapWidget.setSize("100%", "100%");
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

//        FlexTable editPanel = new FlexTable();
//        editPanel.ensureDebugId("editPanel");
//        editPanel.setCellSpacing(2);
//        editPanel.setCellPadding(2);
//        int row = 0;
//        editPanel.setWidget(row, 0, new RadioButton("x", "Select region"));
//        editPanel.getFlexCellFormatter().setColSpan(row, 0, 2);
//        row++;
//        editPanel.setWidget(row, 0, new RadioButton("x", "Draw polgon"));
//        editPanel.getFlexCellFormatter().setColSpan(row, 0, 2);
//        row++;
//        editPanel.setWidget(row, 0, new RadioButton("x", "Draw box"));
//        editPanel.getFlexCellFormatter().setColSpan(row, 0, 2);
//        row++;
//        editPanel.setWidget(row, 0, new RadioButton("x", "Enter box"));
//        editPanel.getFlexCellFormatter().setColSpan(row, 0, 2);
//        row++;
//        editPanel.setWidget(row, 0, new Label("North:"));
//        editPanel.setWidget(row, 1, new DoubleBox());
//        row++;
//        editPanel.setWidget(row, 0, new Label("South:"));
//        editPanel.setWidget(row, 1, new DoubleBox());
//        row++;
//        editPanel.setWidget(row, 0, new Label("West:"));
//        editPanel.setWidget(row, 1, new DoubleBox());
//        row++;
//        editPanel.setWidget(row, 0, new Label("East:"));
//        editPanel.setWidget(row, 1, new DoubleBox());
//        row++;

        RegionMapToolbar regionMapToolbar = new RegionMapToolbar(this);

        DockLayoutPanel regionPanel = new DockLayoutPanel(Style.Unit.EM);
        regionPanel.ensureDebugId("regionPanel");
        regionPanel.addNorth(new HTML("<b>Defined regions</b>"), 1.5);
        regionPanel.addSouth(regionMapToolbar, 3.5);
        regionPanel.add(regionScrollPanel);

        SplitLayoutPanel regionSplitLayoutPanel = new SplitLayoutPanel();
        regionSplitLayoutPanel.ensureDebugId("regionSplitLayoutPanel");
        regionSplitLayoutPanel.setSize(width, height);
        regionSplitLayoutPanel.addWest(regionPanel, 180);
        regionSplitLayoutPanel.add(mapWidget);

        List<Region> regionList = regionMapModel.getRegionList().getList();
        for (Region region : regionList) {
            Polygon polygon = region.getPolygon();
            polygon.setVisible(true);
            polygon.setStrokeStyle(normalPolyStrokeStyle);
            polygon.setFillStyle(normalPolyFillStyle);
            mapWidget.addOverlay(polygon);
        }

        this.widget = regionSplitLayoutPanel;
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
