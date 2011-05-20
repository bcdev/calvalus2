package com.bc.calvalus.client.map;

import com.google.gwt.cell.client.AbstractCell;
import com.google.gwt.cell.client.Cell;
import com.google.gwt.dom.client.Style;
import com.google.gwt.maps.client.MapWidget;
import com.google.gwt.maps.client.control.MapTypeControl;
import com.google.gwt.maps.client.geom.LatLng;
import com.google.gwt.maps.client.overlay.Polygon;
import com.google.gwt.safehtml.shared.SafeHtmlBuilder;
import com.google.gwt.user.cellview.client.CellList;
import com.google.gwt.user.cellview.client.HasKeyboardPagingPolicy;
import com.google.gwt.user.cellview.client.HasKeyboardSelectionPolicy;
import com.google.gwt.user.client.ui.*;
import com.google.gwt.view.client.ProvidesKey;
import com.google.gwt.view.client.SelectionChangeEvent;
import com.google.gwt.view.client.SingleSelectionModel;

import java.util.List;

/**
 * An implementation of a Google map that has regions.
 *
 * @author Norman Fomferra
 */
public class RegionMapWidget implements IsWidget, RegionMap {
    private final RegionMapModel model;
    private Widget widget;
    private MapWidget mapWidget;

    public RegionMapWidget(RegionMapModel model) {
        this.model = model;
    }

    @Override
    public RegionMapModel getModel() {
        return model;
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

        CellList<Region> regionCellList = new CellList<Region>(regionCell, regionKey);
        regionCellList.setVisibleRange(0, 256);
        regionCellList.setKeyboardPagingPolicy(HasKeyboardPagingPolicy.KeyboardPagingPolicy.INCREASE_RANGE);
        regionCellList.setKeyboardSelectionPolicy(HasKeyboardSelectionPolicy.KeyboardSelectionPolicy.ENABLED);
        model.getRegionProvider().addDataDisplay(regionCellList);

        // Add a selection model so we can select cells.
        final SingleSelectionModel<Region> selectionModel = new SingleSelectionModel<Region>(regionKey);
        regionCellList.setSelectionModel(selectionModel);
        selectionModel.addSelectionChangeHandler(new SelectionChangeEvent.Handler() {
            public void onSelectionChange(SelectionChangeEvent event) {
                Region region = selectionModel.getSelectedObject();
                if (region != null) {
                    model.getRegionSelection().setSelectedRegions(region);
                    Polygon selectedPolygon = region.getPolygon();
                    int zoomLevel = mapWidget.getBoundsZoomLevel(selectedPolygon.getBounds());
                    mapWidget.panTo(selectedPolygon.getBounds().getCenter());
                    mapWidget.setZoomLevel(zoomLevel);
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
        regionSplitLayoutPanel.setSize("100%", "100%");
        regionSplitLayoutPanel.addWest(regionPanel, 150);
        regionSplitLayoutPanel.add(mapWidget);

        List<Region> regionList = model.getRegionProvider().getList();
        for (Region region : regionList) {
            Polygon polygon = region.getPolygon();
            polygon.setVisible(true);
            mapWidget.addOverlay(polygon);
        }

        this.widget = regionSplitLayoutPanel;
    }

}
