package com.bc.calvalus.client.map;

import com.google.gwt.maps.client.MapWidget;
import com.google.gwt.maps.client.control.Control;
import com.google.gwt.maps.client.control.ControlAnchor;
import com.google.gwt.maps.client.control.ControlPosition;
import com.google.gwt.user.client.ui.Widget;

/**
 * The {@link RegionMapToolbar} as a Google maps Control, that can be added directly to the map widget.
 *
 * @author Norman
 */
public class RegionMapToolbarControl extends Control.CustomControl {

    private final RegionMap regionMap;

    public RegionMapToolbarControl(RegionMap regionMap) {
        this(new ControlPosition(ControlAnchor.TOP_LEFT, 100, 10), regionMap);
    }

    public RegionMapToolbarControl(ControlPosition defaultPosition, RegionMap regionMap) {
        super(defaultPosition, false, false);
        this.regionMap = regionMap;
    }

    @Override
    public boolean isSelectable() {
        return false;
    }

    @Override
    protected Widget initialize(MapWidget mapWidget) {
        RegionMapToolbar toolbar = new RegionMapToolbar(regionMap);
        return toolbar.asWidget();
    }
}
