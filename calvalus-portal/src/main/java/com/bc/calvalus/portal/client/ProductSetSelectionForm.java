package com.bc.calvalus.portal.client;

import com.bc.calvalus.portal.shared.DtoProductSet;
import com.google.gwt.core.client.GWT;
import com.google.gwt.event.dom.client.ChangeEvent;
import com.google.gwt.uibinder.client.UiBinder;
import com.google.gwt.uibinder.client.UiField;
import com.google.gwt.user.client.ui.CheckBox;
import com.google.gwt.user.client.ui.Composite;
import com.google.gwt.user.client.ui.ListBox;
import com.google.gwt.user.client.ui.TextBox;
import com.google.gwt.user.client.ui.Widget;

import java.util.HashMap;
import java.util.Map;

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
    @UiField
    CheckBox useAlternateInputPath;
    @UiField
    TextBox alternateInputPath;

    public ProductSetSelectionForm(DtoProductSet[] productSets) {
        this.productSets = productSets;
        initWidget(uiBinder.createAndBindUi(this));

        inputProductSet.setName("inputProductSet");
        for (DtoProductSet productSet : productSets) {
            inputProductSet.addItem(productSet.getName());
        }
        if (inputProductSet.getItemCount() > 0) {
            inputProductSet.setSelectedIndex(0);
        }

        inputProductSet.addChangeHandler(new com.google.gwt.event.dom.client.ChangeHandler() {
            @Override
            public void onChange(ChangeEvent event) {
                updateAlternateInputPath();
            }
        });

        updateAlternateInputPath();
    }

    private void updateAlternateInputPath() {
        if (!useAlternateInputPath.getValue()) {
            DtoProductSet productSet = getProductSet();
            if (productSet != null) {
                alternateInputPath.setValue(productSet.getPath());
            }
        }
    }

    public void addChangeHandler(final ChangeHandler changeHandler) {
        inputProductSet.addChangeHandler(new com.google.gwt.event.dom.client.ChangeHandler() {
            @Override
            public void onChange(ChangeEvent changeEvent) {
                changeHandler.onProductSetChanged(getProductSet());
            }
        });
    }

    public DtoProductSet getProductSet() {
        int selectedIndex = inputProductSet.getSelectedIndex();
        if (selectedIndex >= 0) {
            return productSets[selectedIndex];
        } else {
            return null;
        }
    }

    public void validateForm() throws ValidationException {
        if (useAlternateInputPath.getValue()) {
            boolean alternateInputPathValid = !alternateInputPath.getValue().trim().isEmpty();
            if (!alternateInputPathValid) {
                throw new ValidationException(alternateInputPath, "Please enter an alternative input path.");
            }
        } else {
            DtoProductSet productSet = getProductSet();
            boolean productSetValid = productSet != null;
            if (!productSetValid) {
                throw new ValidationException(inputProductSet, "An input product set must be selected.");
            }
        }
    }

    public static interface ChangeHandler {
        void onProductSetChanged(DtoProductSet productSet);
    }

    public Map<String, String> getValueMap() {
        Map<String, String> parameters = new HashMap<String, String>();
        if (useAlternateInputPath.getValue()) {
            parameters.put("inputPath", alternateInputPath.getValue().trim());
        } else {
            parameters.put("inputPath", getProductSet().getPath());
        }
        return parameters;
    }

}