package com.bc.calvalus.portal.client;

import com.bc.calvalus.portal.client.map.RegionMapWidget;
import com.google.gwt.event.dom.client.ClickEvent;
import com.google.gwt.event.dom.client.ClickHandler;
import com.google.gwt.user.client.ui.Button;
import com.google.gwt.user.client.ui.VerticalPanel;
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

    private final Widget widget;

    public ManageRegionsView(PortalContext portalContext) {
        super(portalContext);
        RegionMapWidget regionMapWidget = RegionMapWidget.create(portalContext.getRegions(), true);
        regionMapWidget.setSize("100%", "600px");

        Button submitButton = new Button();
        submitButton.setText("Save Changes");
        submitButton.addClickHandler(new ClickHandler() {
            @Override
            public void onClick(ClickEvent event) {
                // todo: store new user regions in database.
                Dialog.showMessage("Manage Regions", "Not implemented yet.");
            }
        });

        VerticalPanel verticalPanel = new VerticalPanel();
        verticalPanel.setSpacing(2);
        verticalPanel.setWidth("100%");
        verticalPanel.add(regionMapWidget);
        verticalPanel.add(submitButton);

        widget = verticalPanel;
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