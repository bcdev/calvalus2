package com.bc.calvalus.portal.client;

import com.bc.calvalus.portal.shared.DtoProductSet;
import com.google.gwt.core.client.GWT;
import com.google.gwt.event.dom.client.ChangeEvent;
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

    private final DtoProductSet[] productSets;

    interface TheUiBinder extends UiBinder<Widget, ProductSetSelectionForm> {
    }

    private static TheUiBinder uiBinder = GWT.create(TheUiBinder.class);

    @UiField
    ListBox inputProductSet;

    public ProductSetSelectionForm(DtoProductSet[] productSets) {
        this.productSets = productSets;
        initWidget(uiBinder.createAndBindUi(this));

        inputProductSet.setName("inputProductSet");
        for (DtoProductSet productSet : productSets) {
            inputProductSet.addItem(productSet.getPath());
        }
        inputProductSet.setSelectedIndex(0);
    }

    public void addChangeHandler(final ChangeHandler changeHandler) {
        inputProductSet.addChangeHandler(new com.google.gwt.event.dom.client.ChangeHandler() {
            @Override
            public void onChange(ChangeEvent changeEvent) {
                changeHandler.onProductSetChanged(getSelectedProductSet());
            }
        });
    }

    public DtoProductSet getSelectedProductSet() {
        int selectedIndex = inputProductSet.getSelectedIndex();
        if (selectedIndex >= 0) {
            return productSets[selectedIndex];
        } else {
            return null;
        }
    }

    public void validateForm() throws ValidationException {
        DtoProductSet selectedProductSet = getSelectedProductSet();
        boolean inputProductSetIdValid = selectedProductSet != null;
        if (!inputProductSetIdValid) {
            throw new ValidationException(inputProductSet, "An input product set must be selected.");
        }
    }

    public static interface ChangeHandler {
        void onProductSetChanged(DtoProductSet productSet);
    }


}