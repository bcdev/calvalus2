package com.bc.calvalus.client;

import com.bc.calvalus.client.map.AbstractMapAction;
import com.bc.calvalus.client.map.MapAction;
import com.bc.calvalus.client.map.Region;
import com.bc.calvalus.client.map.RegionMap;
import com.bc.calvalus.client.map.RegionMapModelImpl;
import com.bc.calvalus.client.map.RegionMapWidget;
import com.bc.calvalus.client.map.WKTParser;
import com.bc.calvalus.client.map.actions.DeleteRegionsAction;
import com.bc.calvalus.client.map.actions.InsertBoxInteraction;
import com.bc.calvalus.client.map.actions.InsertPolygonInteraction;
import com.bc.calvalus.client.map.actions.LocateRegionsAction;
import com.bc.calvalus.client.map.actions.RenameRegionAction;
import com.bc.calvalus.client.map.actions.SelectInteraction;
import com.bc.calvalus.client.map.actions.ShowRegionInfoAction;
import com.bc.calvalus.shared.EncodedRegion;
import com.google.gwt.core.client.EntryPoint;
import com.google.gwt.core.client.GWT;
import com.google.gwt.maps.client.Maps;
import com.google.gwt.maps.client.overlay.Overlay;
import com.google.gwt.maps.client.overlay.Polygon;
import com.google.gwt.user.client.Window;
import com.google.gwt.user.client.rpc.AsyncCallback;
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


    /*
     * Create array of sample actions.
     */
    private MapAction[] createDefaultActions() {
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
