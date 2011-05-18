package com.bc.calvalus.portal.client;

import com.bc.calvalus.portal.shared.GsProductSet;
import com.google.gwt.user.client.ui.FlexTable;
import com.google.gwt.user.client.ui.ListBox;
import com.google.gwt.user.client.ui.Widget;

/**
 * Demo view that currently shows the input product sets.
 * It may later on be useful to query for products and let user define
 * product sets based on spatial queries.
 *
 * @author Norman
 */
public class ManageProductSetsView extends PortalView {

    public static final String ID = ManageProductSetsView.class.getName();

    private final FlexTable widget;

    public ManageProductSetsView(PortalContext portalContext) {
        super(portalContext);

        ListBox inputProductSet = new ListBox();
        inputProductSet.setName("inputProductSet");
        for (GsProductSet productSet : portalContext.getProductSets()) {
            inputProductSet.addItem(productSet.getName(), productSet.getId());
        }
        inputProductSet.setWidth("20em");
        inputProductSet.setVisibleItemCount(6);

        widget = new FlexTable();
        widget.ensureDebugId("widget");
        widget.setCellSpacing(2);
        widget.setCellPadding(2);
        widget.setWidget(0, 0, inputProductSet.asWidget());
    }

    @Override
    public Widget asWidget() {
        return widget;
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