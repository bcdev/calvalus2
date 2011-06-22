package com.bc.calvalus.portal.client;

import com.bc.calvalus.portal.client.map.RegionMapWidget;
import com.google.gwt.user.client.ui.Widget;

/**
 * Demo view that currently shows the input product sets.
 * It may later on be useful to query for products and let user define
 * product sets based on spatial queries.
 *
 * @author Norman
 */
public class ManageRegionsView extends PortalView {

    public static final String ID = ManageRegionsView.class.getName();

    private final RegionMapWidget widget;

    public ManageRegionsView(PortalContext portalContext) {
        super(portalContext);
        widget = RegionMapWidget.createRegionMapWidget(portalContext.getRegions());
    }

    @Override
    public Widget asWidget() {
        return widget.asWidget();
    }

    @Override
    public String getViewId() {
        return ID;
    }

    @Override
    public String getTitle() {
        return "Manage Regions";
    }
}