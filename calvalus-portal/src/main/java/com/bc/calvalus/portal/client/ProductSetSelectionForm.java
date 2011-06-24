package com.bc.calvalus.portal.client;

import com.bc.calvalus.portal.shared.GsProductSet;
import com.google.gwt.core.client.GWT;
import com.google.gwt.uibinder.client.UiBinder;
import com.google.gwt.uibinder.client.UiField;
import com.google.gwt.user.client.ui.Composite;
import com.google.gwt.user.client.ui.ListBox;
import com.google.gwt.user.client.ui.Widget;

/**
 * Demo view that lets users submit a new L2 production.
 *
 * @author Norman
 */
public class ProductSetSelectionForm extends Composite {

    interface TheUiBinder extends UiBinder<Widget, ProductSetSelectionForm> {
    }

    private static TheUiBinder uiBinder = GWT.create(TheUiBinder.class);

    @UiField
    ListBox inputProductSet;

    public ProductSetSelectionForm(GsProductSet[] productSets) {
        initWidget(uiBinder.createAndBindUi(this));

        inputProductSet.setName("inputProductSet");
        for (GsProductSet productSet : productSets) {
            inputProductSet.addItem(productSet.getName(), productSet.getPath());
        }
        inputProductSet.setVisibleItemCount(6);
    }

    public String getInputProductSetId() {
        int selectedIndex = inputProductSet.getSelectedIndex();
        if (selectedIndex >= 0) {
            return inputProductSet.getValue(selectedIndex);
        } else {
            return null;
        }
    }
}