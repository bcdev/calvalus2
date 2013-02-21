package com.bc.calvalus.portal.client.map.actions;

import com.bc.calvalus.portal.client.Dialog;
import com.bc.calvalus.portal.client.map.AbstractMapAction;
import com.bc.calvalus.portal.client.map.Region;
import com.bc.calvalus.portal.client.map.RegionMap;
import com.google.gwt.maps.client.base.LatLngBounds;
import com.google.gwt.maps.client.geometrylib.SphericalUtils;
import com.google.gwt.maps.client.overlays.InfoWindow;
import com.google.gwt.maps.client.overlays.InfoWindowOptions;
import com.google.gwt.maps.client.overlays.Polygon;
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
        LatLngBounds bounds = Region.getBounds(polygon);

        FlexTable flexTable = new FlexTable();
        int row = 0;
        flexTable.setHTML(row, 0, "<b>Vertices:</b>");
        flexTable.setHTML(row, 1, polygon.getPath().getLength() + "");
        row++;
        flexTable.setHTML(row, 0, "<b>Area:</b>");
        flexTable.setHTML(row, 1, SphericalUtils.computeArea(polygon.getPath()) + "");
        flexTable.setHTML(row, 2, "m^2");
        row++;
        flexTable.setHTML(row, 0, "<b>South-West:</b>");
        flexTable.setHTML(row, 1, bounds.getSouthWest() + "");
        flexTable.setHTML(row, 2, "degree");
        row++;
        flexTable.setHTML(row, 0, "<b>North-East:</b>");
        flexTable.setHTML(row, 1, bounds.getNorthEast() + "");
        flexTable.setHTML(row, 2, "degree");

        VerticalPanel verticalPanel = new VerticalPanel();
        verticalPanel.add(new HTML("<h3>"+selectedRegion.getQualifiedName()+"</h3>"));
        verticalPanel.add(flexTable);

        InfoWindowOptions infoWindowOptions = InfoWindowOptions.newInstance();
        infoWindowOptions.setPosition(bounds.getCenter());
        infoWindowOptions.setContent(verticalPanel);
        InfoWindow infoWindow = InfoWindow.newInstance(infoWindowOptions);
        infoWindow.open(regionMap.getMapWidget());
    }
}
