package com.bc.calvalus.portal.client;

import com.bc.calvalus.portal.shared.DtoInputSelection;
import com.google.gwt.core.client.GWT;
import com.google.gwt.uibinder.client.UiBinder;
import com.google.gwt.uibinder.client.UiField;
import com.google.gwt.user.client.rpc.AsyncCallback;
import com.google.gwt.user.client.ui.Button;
import com.google.gwt.user.client.ui.Composite;
import com.google.gwt.user.client.ui.ListBox;
import com.google.gwt.user.client.ui.Widget;

import java.util.List;

/**
 * A form that lets users select a product.
 *
 * @author Hans
 */
public class ProductSelectionForm extends Composite {

    interface TheUiBinder extends UiBinder<Widget, ProductSelectionForm> {
    }

    private static TheUiBinder uiBinder = GWT.create(TheUiBinder.class);

    private final PortalContext portal;

    @UiField
    ListBox productListBox;

    @UiField
    Button pasteFromCatalogueButton;

    ProductSelectionForm(PortalContext portalContext) {
        this.portal = portalContext;
        initWidget(uiBinder.createAndBindUi(this));
    }

    private void updateListBox(DtoInputSelection inputSelection) {
        productListBox.clear();
        if (inputSelection != null) {
            List<String> newProducts = inputSelection.getProductIdentifiers();
            int newSelectionIndex = 0;
            for (String product : newProducts) {
                productListBox.addItem(product);
            }
            if (productListBox.getItemCount() > 0) {
                productListBox.setSelectedIndex(newSelectionIndex);
            }
        }
    }

    void addInputSelectionChangeHandler(AsyncCallback<DtoInputSelection> callback) {
        pasteFromCatalogueButton.addClickHandler(event ->
                portal.getContextRetrievalService().getInputSelection(callback));
    }

    AsyncCallback<DtoInputSelection> getInputSelectionCallback() {
        return new UpdateProductListCallback();
    }

    private class UpdateProductListCallback implements AsyncCallback<DtoInputSelection> {

        @Override
        public void onSuccess(DtoInputSelection inputSelection) {
            updateListBox(inputSelection);
        }

        @Override
        public void onFailure(Throwable caught) {
            productListBox.clear();
        }
    }
}