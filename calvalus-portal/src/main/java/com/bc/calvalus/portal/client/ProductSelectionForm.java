package com.bc.calvalus.portal.client;

import com.bc.calvalus.portal.shared.DtoInputSelection;
import com.google.gwt.core.client.GWT;
import com.google.gwt.uibinder.client.UiBinder;
import com.google.gwt.uibinder.client.UiField;
import com.google.gwt.user.client.ui.Button;
import com.google.gwt.user.client.ui.Composite;
import com.google.gwt.user.client.ui.ListBox;
import com.google.gwt.user.client.ui.Widget;

import java.util.List;

/**
 * A form that lets users select a product.
 *
 * @author Norman
 */
public class ProductSelectionForm extends Composite {

    interface TheUiBinder extends UiBinder<Widget, ProductSelectionForm> {

    }

    private static TheUiBinder uiBinder = GWT.create(TheUiBinder.class);

    private final PortalContext portal;

    private DtoInputSelection inputSelection;

    @UiField
    ListBox productListBox;

    @UiField
    Button pasteFromCatalogueButton;

    @UiField
    Button testButton;

    public ProductSelectionForm(PortalContext portalContext) {
        this.portal = portalContext;
        this.inputSelection = portal.getInputSelection();

        initWidget(uiBinder.createAndBindUi(this));

        pasteFromCatalogueButton.addClickHandler(event -> {
            portal.retrieveInputSelection();
        });

        testButton.addClickHandler(event -> {
            DtoInputSelection inputSelection = portal.getInputSelection();
            if (inputSelection != null) {
                this.inputSelection = portal.getInputSelection();
                updateListBox();
            }
        });
    }

    public void updateListBox() {
        productListBox.clear();
        if (inputSelection != null) {
            List<String> newProducts = inputSelection.getProductIdentifiers();
            GWT.log(String.valueOf(newProducts.size()));
            int newSelectionIndex = 0;
            for (String product : newProducts) {
                productListBox.addItem(product);
            }
            if (productListBox.getItemCount() > 0) {
                productListBox.setSelectedIndex(newSelectionIndex);
            }
        }
    }
}