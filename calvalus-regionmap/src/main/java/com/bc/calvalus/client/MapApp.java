package com.bc.calvalus.client;

import com.bc.calvalus.client.map.*;
import com.bc.calvalus.client.map.interactions.InsertBoxInteraction;
import com.bc.calvalus.client.map.interactions.InsertPolygonInteraction;
import com.bc.calvalus.client.map.interactions.SelectInteraction;
import com.bc.calvalus.shared.EncodedRegion;
import com.google.gwt.core.client.EntryPoint;
import com.google.gwt.core.client.GWT;
import com.google.gwt.maps.client.InfoWindowContent;
import com.google.gwt.maps.client.Maps;
import com.google.gwt.maps.client.geom.LatLng;
import com.google.gwt.maps.client.overlay.Overlay;
import com.google.gwt.maps.client.overlay.Polygon;
import com.google.gwt.user.client.Window;
import com.google.gwt.user.client.rpc.AsyncCallback;
import com.google.gwt.user.client.ui.FlexTable;
import com.google.gwt.user.client.ui.RootLayoutPanel;

import java.util.ArrayList;
import java.util.List;

/**
 * Entry point which defines <code>onModuleLoad()</code>.
 */
public class MapApp implements EntryPoint {

    private final MapServiceAsync mapService = GWT.create(MapService.class);

    @Override
    public void onModuleLoad() {
        /*
         * Asynchronously loads the Maps API.
         *
         * The first parameter should be a valid Maps API Key to deploy this
         * application on a public server, but a blank key will work for an
         * application served from localhost.
        */
        Maps.loadMapsApi("", "2", false, new Runnable() {
            public void run() {
                mapService.getRegions(new GetRegionsCallback());
            }
        });
    }

    private List<Region> decodeRegions(EncodedRegion[] encodedRegions) {
        ArrayList<Region> regionList = new ArrayList<Region>();
        for (EncodedRegion encodedRegion : encodedRegions) {
            String wkt = encodedRegion.getWkt();
            Overlay overlay = WKTParser.parse(wkt);
            if (overlay instanceof Polygon) {
                Polygon polygon = (Polygon) overlay;
                polygon.setVisible(true);
                regionList.add(new Region(encodedRegion.getName(), polygon));
            }
        }
        return regionList;
    }

    private MapAction[] createDefaultActions() {
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
                new AbstractMapAction("R", "Rename selected region") {
                    @Override
                    public void run(RegionMap regionMap) {
                        Region selectedRegion = regionMap.getModel().getRegionSelection().getSelectedRegion();
                        if (selectedRegion == null) {
                            Window.alert("No region selected.");
                            return;
                        }
                        Window.alert("'Rename' not implemented yet.");
                    }
                },
                new AbstractMapAction("D", "Delete selected region") {
                    @Override
                    public void run(RegionMap regionMap) {
                        Region[] selectedRegions = regionMap.getModel().getRegionSelection().getSelectedRegions();
                        if (selectedRegions.length == 0) {
                            Window.alert("No regions selected.");
                            return;
                        }
                        int n = 0;
                        for (Region selectedRegion : selectedRegions) {
                            if (selectedRegion.isUserRegion()) {
                                regionMap.getModel().getRegionSelection().removeSelectedRegions(selectedRegion);
                                regionMap.getModel().getRegionProvider().getList().remove(selectedRegion);
                                regionMap.getMapWidget().removeOverlay(selectedRegion.getPolygon());
                                n++;
                            }
                        }
                        if (n == 0) {
                            Window.alert("The selected regions could not be deleted.");
                        } else if (n < selectedRegions.length) {
                            Window.alert("Some of the selected regions could not be deleted.");
                        }
                    }
                },
                new AbstractMapAction("I", "Display info about the selected region") {
                    @Override
                    public void run(RegionMap regionMap) {
                        Region selectedRegion = regionMap.getModel().getRegionSelection().getSelectedRegion();
                        if (selectedRegion == null) {
                            Window.alert("No region selected.");
                            return;
                        }

                        Polygon polygon = selectedRegion.getPolygon();
                        LatLng northEast = polygon.getBounds().getNorthEast();
                        LatLng southWest = polygon.getBounds().getSouthWest();

                        FlexTable flexTable = new FlexTable();
                        int row = 0;
                        flexTable.setHTML(row, 0, "<b>Region:</b>");
                        flexTable.setHTML(row, 1, selectedRegion.getName());
                        row++;
                        flexTable.setHTML(row, 0, "<b>#Vertices:</b>");
                        flexTable.setHTML(row, 1, polygon.getVertexCount() + "");
                        row++;
                        flexTable.setHTML(row, 0, "<b>Area:</b>");
                        flexTable.setHTML(row, 1, polygon.getArea() + "");
                        flexTable.setHTML(row, 2, "m^2");
                        row++;
                        flexTable.setHTML(row, 0, "<b>North:</b>");
                        flexTable.setHTML(row, 1,northEast.getLatitude() + "");
                        flexTable.setHTML(row, 2, "degree");
                        row++;
                        flexTable.setHTML(row, 0, "<b>South:</b>");
                        flexTable.setHTML(row, 1, southWest.getLatitude() + "");
                        flexTable.setHTML(row, 2, "degree");
                        row++;
                        flexTable.setHTML(row, 0, "<b>West:</b>");
                        flexTable.setHTML(row, 1,southWest.getLongitude() + "");
                        flexTable.setHTML(row, 2, "degree");
                        row++;
                        flexTable.setHTML(row, 0, "<b>East:</b>");
                        flexTable.setHTML(row, 1,  northEast.getLongitude() + "");
                        flexTable.setHTML(row, 2, "degree");

                        regionMap.getMapWidget().getInfoWindow().open(polygon.getBounds().getCenter(),
                                new InfoWindowContent(flexTable));
                    }
                }
        };
    }

    private class GetRegionsCallback implements AsyncCallback<EncodedRegion[]> {

        @Override
        public void onSuccess(EncodedRegion[] encodedRegions) {
            RegionMapModelImpl model = new RegionMapModelImpl(decodeRegions(encodedRegions),
                    createDefaultActions());
            RootLayoutPanel.get().add(new RegionMapWidget(model));
        }

        @Override
        public void onFailure(Throwable throwable) {
            Window.alert("Server error: " + throwable.getMessage());
        }
    }
}
