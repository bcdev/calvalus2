package com.bc.calvalus.portal.client.map.actions;

import com.bc.calvalus.portal.client.map.AbstractMapAction;
import com.bc.calvalus.portal.client.map.Region;
import com.bc.calvalus.portal.client.map.RegionMap;
import com.google.gwt.maps.client.InfoWindowContent;
import com.google.gwt.maps.client.overlay.Polygon;
import com.google.gwt.user.client.Window;
import com.google.gwt.user.client.ui.FlexTable;

/**
 * todo - add api doc
 *
 * @author Norman Fomferra
 */
public class ShowRegionInfoAction extends AbstractMapAction {
    public ShowRegionInfoAction() {
        super("I", "Display info about the selected region");
    }

    @Override
    public void run(RegionMap regionMap) {
        Region selectedRegion = regionMap.getRegionSelectionModel().getSelectedRegion();
        if (selectedRegion == null) {
            Window.alert("No region selected.");
            return;
        }

        Polygon polygon = selectedRegion.getPolygon();

        FlexTable flexTable = new FlexTable();
        int row = 0;
        flexTable.setHTML(row, 0, "<b>Region name:</b>");
        flexTable.setHTML(row, 1, selectedRegion.getName());
        row++;
        flexTable.setHTML(row, 0, "<b>Polygon vertex count:</b>");
        flexTable.setHTML(row, 1, polygon.getVertexCount() + "");
        row++;
        flexTable.setHTML(row, 0, "<b>Polygon area:</b>");
        flexTable.setHTML(row, 1, polygon.getArea() + "");
        flexTable.setHTML(row, 2, "m^2");
        row++;
        flexTable.setHTML(row, 0, "<b>Polygon bounds north-east:</b>");
        flexTable.setHTML(row, 1, polygon.getBounds().getNorthEast() + "");
        flexTable.setHTML(row, 2, "degree");
        row++;
        flexTable.setHTML(row, 0, "<b>Polygon bounds south-west:</b>");
        flexTable.setHTML(row, 1, polygon.getBounds().getSouthWest() + "");
        flexTable.setHTML(row, 2, "degree");
        row++;
        flexTable.setHTML(row, 0, "<b>Polygon bounds center:</b>");
        flexTable.setHTML(row, 1, polygon.getBounds().getCenter() + "");
        flexTable.setHTML(row, 2, "degree");

        regionMap.getMapWidget().getInfoWindow().open(polygon.getBounds().getCenter(),
                new InfoWindowContent(flexTable));
    }
}
