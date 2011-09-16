package com.bc.calvalus.portal.client.map.actions;

import com.bc.calvalus.portal.client.Dialog;
import com.bc.calvalus.portal.client.map.AbstractMapAction;
import com.bc.calvalus.portal.client.map.Region;
import com.bc.calvalus.portal.client.map.RegionMap;
import com.google.gwt.maps.client.InfoWindowContent;
import com.google.gwt.maps.client.overlay.Polygon;
import com.google.gwt.user.client.ui.FlexTable;
import com.google.gwt.user.client.ui.HTML;
import com.google.gwt.user.client.ui.VerticalPanel;

/**
 * An action that displays a Google Maps info window for a selected region.
 *
 * @author Norman Fomferra
 */
public class ShowRegionInfoAction extends AbstractMapAction {
    public ShowRegionInfoAction() {
        super("I", "Display some info about the selected region");
    }

    @Override
    public void run(RegionMap regionMap) {
        Region selectedRegion = regionMap.getRegionMapSelectionModel().getSelectedRegion();
        if (selectedRegion == null) {
            Dialog.error("Warning", "No region selected.<br/>Please select a region first.");
            return;
        }

        Polygon polygon = selectedRegion.createPolygon();

        FlexTable flexTable = new FlexTable();
        int row = 0;
        flexTable.setHTML(row, 0, "<b>Vertices:</b>");
        flexTable.setHTML(row, 1, polygon.getVertexCount() + "");
        row++;
        flexTable.setHTML(row, 0, "<b>Area:</b>");
        flexTable.setHTML(row, 1, polygon.getArea() + "");
        flexTable.setHTML(row, 2, "m^2");
        row++;
        flexTable.setHTML(row, 0, "<b>South-West:</b>");
        flexTable.setHTML(row, 1, polygon.getBounds().getSouthWest() + "");
        flexTable.setHTML(row, 2, "degree");
        row++;
        flexTable.setHTML(row, 0, "<b>North-East:</b>");
        flexTable.setHTML(row, 1, polygon.getBounds().getNorthEast() + "");
        flexTable.setHTML(row, 2, "degree");

        VerticalPanel verticalPanel = new VerticalPanel();
        verticalPanel.add(new HTML("<h3>"+selectedRegion.getQualifiedName()+"</h3>"));
        verticalPanel.add(flexTable);

        regionMap.getMapWidget().getInfoWindow().open(polygon.getBounds().getCenter(),
                new InfoWindowContent(verticalPanel));
    }
}
