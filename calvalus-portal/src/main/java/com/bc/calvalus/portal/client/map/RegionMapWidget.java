package com.bc.calvalus.portal.client.map;

import com.bc.calvalus.portal.client.map.actions.DeleteRegionsAction;
import com.bc.calvalus.portal.client.map.actions.InsertBoxInteraction;
import com.bc.calvalus.portal.client.map.actions.InsertPolygonInteraction;
import com.bc.calvalus.portal.client.map.actions.LocateRegionsAction;
import com.bc.calvalus.portal.client.map.actions.RenameRegionAction;
import com.bc.calvalus.portal.client.map.actions.SelectInteraction;
import com.bc.calvalus.portal.client.map.actions.ShowRegionInfoAction;
import com.bc.calvalus.portal.shared.GsRegion;
import com.google.gwt.cell.client.AbstractCell;
import com.google.gwt.cell.client.Cell;
import com.google.gwt.dom.client.Style;
import com.google.gwt.maps.client.MapWidget;
import com.google.gwt.maps.client.control.MapTypeControl;
import com.google.gwt.maps.client.overlay.Overlay;
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
import com.google.gwt.view.client.MultiSelectionModel;
import com.google.gwt.view.client.ProvidesKey;
import com.google.gwt.view.client.SelectionChangeEvent;
import com.google.gwt.view.client.SelectionModel;

import java.util.ArrayList;
import java.util.List;

/**
 * An implementation of a Google map that has regions.
 *
 * @author Norman Fomferra
 */
public class RegionMapWidget implements IsWidget, RegionMap {

    private final RegionMapModel regionMapModel;
    private final RegionMapSelectionModel regionMapSelectionModel;
    private Widget widget;
    private MapWidget mapWidget;
    private boolean adjustingRegionSelection;
    private boolean updatingRegionListSelection;

    private PolyStyleOptions normalPolyStrokeStyle;
    private PolyStyleOptions normalPolyFillStyle;
    private PolyStyleOptions selectedPolyStrokeStyle;
    private PolyStyleOptions selectedPolyFillStyle;

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
    }

    public static RegionMapWidget createRegionMapWidget(GsRegion[] encodedRegions) {
        RegionMapModelImpl model = new RegionMapModelImpl(decodeRegions(encodedRegions),
                createDefaultActions());
        return new RegionMapWidget(model);
    }

    public static MapAction[] createDefaultActions() {
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
                new LocateRegionsAction(),
                new RenameRegionAction(),
                new DeleteRegionsAction(),
                new ShowRegionInfoAction()
        };
    }

    public static List<Region> decodeRegions(GsRegion[] encodedRegions) {
        ArrayList<Region> regionList = new ArrayList<Region>();
        for (GsRegion encodedRegion : encodedRegions) {
            String geometryWkt = encodedRegion.getGeometryWkt();
            Overlay overlay = WKTParser.parse(geometryWkt);
            if (overlay instanceof Polygon) {
                Polygon polygon = (Polygon) overlay;
                polygon.setVisible(true);
                regionList.add(new Region(encodedRegion.getName(), polygon));
            }
        }
        return regionList;
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
        regionPanel.addNorth(new HTML("<b>Defined regions:</b>"), 1.5);
        regionPanel.addSouth(regionMapToolbar, 3.5);
        regionPanel.add(regionScrollPanel);

        SplitLayoutPanel regionSplitLayoutPanel = new SplitLayoutPanel();
        regionSplitLayoutPanel.ensureDebugId("regionSplitLayoutPanel");
        regionSplitLayoutPanel.setSize("100%", "600px");
        regionSplitLayoutPanel.addWest(regionPanel, 180);
        regionSplitLayoutPanel.add(mapWidget);

        List<Region> regionList = regionMapModel.getRegionProvider().getList();
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
        for (Region region : regionMapModel.getRegionProvider().getList()) {
            boolean selected = regionMapSelectionModel.isSelected(region);
            region.getPolygon().setStrokeStyle(selected ? selectedPolyStrokeStyle : normalPolyStrokeStyle);
            region.getPolygon().setFillStyle(selected ? selectedPolyFillStyle : normalPolyFillStyle);
            if (region.isUserRegion()) {
                region.getPolygon().setEditingEnabled(selected);
            }
        }
    }

    private void updateRegionSelection(SelectionModel<Region> source, SelectionModel<Region> target) {
        for (Region region : regionMapModel.getRegionProvider().getList()) {
            target.setSelected(region, source.isSelected(region));
        }
    }


}
