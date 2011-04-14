package com.bc.calvalus.portal.client;

import com.google.gwt.user.client.ui.Widget;
import org.gwtopenmaps.openlayers.client.LonLat;
import org.gwtopenmaps.openlayers.client.MapOptions;
import org.gwtopenmaps.openlayers.client.MapWidget;
import org.gwtopenmaps.openlayers.client.control.LayerSwitcher;
import org.gwtopenmaps.openlayers.client.control.MousePosition;
import org.gwtopenmaps.openlayers.client.control.NavToolbar;
import org.gwtopenmaps.openlayers.client.control.PanZoomBar;
import org.gwtopenmaps.openlayers.client.layer.Layer;
import org.gwtopenmaps.openlayers.client.layer.TransitionEffect;
import org.gwtopenmaps.openlayers.client.layer.WMS;
import org.gwtopenmaps.openlayers.client.layer.WMSOptions;
import org.gwtopenmaps.openlayers.client.layer.WMSParams;

/**
 * Demo view that shows usage of the OpenLayers GWT widget.
 * It may later on be useful to query for products and let user define
 * product sets based on spatial queries.
 *
 * @author Norman
 */
public class ManageProductSetsView extends PortalView {

    public static final String ID = ManageProductSetsView.class.getName();
    private MapWidget mapWidget;


    public ManageProductSetsView(CalvalusPortal calvalusPortal) {
        super(calvalusPortal);

        // Defining a WMSLayer and adding it to a Map
        WMSParams wmsParams = new WMSParams();
        wmsParams.setFormat("image/png");
        wmsParams.setLayers("basic");
        wmsParams.setStyles("");

        WMSOptions wmsLayerParams = new WMSOptions();
        wmsLayerParams.setUntiled();
        wmsLayerParams.setTransitionEffect(TransitionEffect.RESIZE);

        WMS wmsLayer = new WMS(
                "Basic WMS",
                "http://labs.metacarta.com/wms/vmap0",
                wmsParams,
                wmsLayerParams);

        MapOptions defaultMapOptions = new MapOptions();
        // The map gets PanZoom, Navigation, ArgParser, and Attribution Controls
        // by default. Do removeDefaultControls to remove these.
        // defaultMapOptions.removeDefaultControls();
        defaultMapOptions.setNumZoomLevels(16);
        defaultMapOptions.setProjection("EPSG:4326");

        this.mapWidget = new MapWidget("350px", "350px", defaultMapOptions);
        this.mapWidget.getMap().addLayers(new Layer[]{wmsLayer});

        // Adding controls to the Map
        this.mapWidget.getMap().addControl(new PanZoomBar());
        // use NavToolbar instead of deprecated MouseToolbar
        this.mapWidget.getMap().addControl(new NavToolbar());
        this.mapWidget.getMap().addControl(new MousePosition());
        this.mapWidget.getMap().addControl(new LayerSwitcher());
        // Center and Zoom
        double lon = 4.0;
        double lat = 5.0;
        int zoom = 5;
        this.mapWidget.getMap().setCenter(new LonLat(lon, lat), zoom);
    }

    @Override
    public Widget asWidget() {
        return mapWidget;
    }

    @Override
    public String getViewId() {
        return ID;
    }

    @Override
    public String getTitle() {
        return "Manage Product Sets";
    }
}