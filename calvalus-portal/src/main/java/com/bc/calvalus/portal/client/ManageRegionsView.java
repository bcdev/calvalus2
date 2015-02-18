package com.bc.calvalus.portal.client;

import com.bc.calvalus.portal.client.map.RegionConverter;
import com.bc.calvalus.portal.client.map.RegionMapWidget;
import com.bc.calvalus.portal.shared.DtoRegion;
import com.google.gwt.event.dom.client.ClickEvent;
import com.google.gwt.event.dom.client.ClickHandler;
import com.google.gwt.user.client.rpc.AsyncCallback;
import com.google.gwt.user.client.ui.*;

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
        regionMapWidget = new RegionMapWidget(portalContext.getRegionMapModel(), true,
                                              RegionMapWidget.createDefaultEditingActions());
        regionMapWidget.setSize("800px", "480px");

        Button submitButton = new Button();
        submitButton.setText("Save Changes");
        submitButton.addClickHandler(new ClickHandler() {
            @Override
            public void onClick(ClickEvent event) {
                DtoRegion[] dtoRegions = RegionConverter.encodeRegions(getPortal().getRegions().getList());
                getPortal().getBackendService().storeRegions(dtoRegions, new AsyncCallback<Void>() {

                    @Override
                    public void onSuccess(Void result) {
                        Dialog.info("Manage Regions", "Regions successfully saved.");
                    }

                    @Override
                    public void onFailure(Throwable caught) {
                        Dialog.info("Manage Regions", "Failed to safe regions:\n" + caught.getMessage());
                    }
                });
            }
        });

        HorizontalPanel buttonPanel = new HorizontalPanel();
        HorizontalPanel buttonPanel1 = new HorizontalPanel();
        HorizontalPanel buttonPanel2 = new HorizontalPanel();
        buttonPanel.setSpacing(2);
        buttonPanel.setWidth("120%");
        buttonPanel.add(buttonPanel1);
        buttonPanel.add(buttonPanel2);
        buttonPanel1.setCellHorizontalAlignment(submitButton, HasHorizontalAlignment.ALIGN_LEFT);
        buttonPanel1.add(submitButton);
        buttonPanel2.setCellHorizontalAlignment(submitButton, HasHorizontalAlignment.ALIGN_RIGHT);
        buttonPanel2.setCellVerticalAlignment(submitButton, HasVerticalAlignment.ALIGN_BOTTOM);
        Anchor manageRegionsHelp = new Anchor("Show Help");
        buttonPanel2.add(manageRegionsHelp);
        HelpSystem.addClickHandler(manageRegionsHelp, "manageRegions");

        VerticalPanel verticalPanel = new VerticalPanel();
        verticalPanel.setSpacing(2);
        verticalPanel.add(regionMapWidget);
        verticalPanel.add(buttonPanel);

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
        regionMapWidget.getMapWidget().triggerResize();
    }
}