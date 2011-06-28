package com.bc.calvalus.portal.client;

import com.bc.calvalus.portal.client.map.RegionConverter;
import com.bc.calvalus.portal.client.map.RegionMapWidget;
import com.bc.calvalus.portal.shared.DtoRegion;
import com.google.gwt.event.dom.client.ClickEvent;
import com.google.gwt.event.dom.client.ClickHandler;
import com.google.gwt.user.client.rpc.AsyncCallback;
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
    private RegionMapWidget regionMapWidget;

    public ManageRegionsView(final PortalContext portalContext) {
        super(portalContext);
        regionMapWidget = RegionMapWidget.create(portalContext.getRegions(), true);
        regionMapWidget.setSize("800px", "480px");

        Button submitButton = new Button();
        submitButton.setText("Save Changes");
        submitButton.addClickHandler(new ClickHandler() {
            @Override
            public void onClick(ClickEvent event) {
                regionMapWidget.applyVertexChanges();
                DtoRegion[] dtoRegions = RegionConverter.encodeRegions(getPortal().getRegions().getList());
                getPortal().getBackendService().storeRegions(dtoRegions, new AsyncCallback<Void>() {

                    @Override
                    public void onSuccess(Void result) {
                        Dialog.showMessage("Manage Regions", "Regions successfully saved.");
                    }

                    @Override
                    public void onFailure(Throwable caught) {
                        Dialog.showMessage("Manage Regions", "Failed to safe regions:\n" + caught.getMessage());
                    }
                });
            }
        });

        VerticalPanel verticalPanel = new VerticalPanel();
        verticalPanel.setSpacing(2);
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

    @Override
    public void onShowing() {
        // See http://code.google.com/p/gwt-google-apis/issues/detail?id=127
        regionMapWidget.getMapWidget().checkResizeAndCenter();
    }

    @Override
    public void onHidden() {
        regionMapWidget.applyVertexChanges();
    }
}